"""
Worker de despliegues — proceso independiente que consume de RabbitMQ.
"""

import asyncio
import json
import logging
import uuid
import re
from datetime import datetime, timezone

import aio_pika
from prisma import Prisma, Json
from prisma.enums import (
    DeploymentStatus,
    DeploymentEventType,
    DeploymentEnvironment,
    DeploymentPolicy,
)

from prometheus_client import Counter, Histogram, start_http_server

from app.core.config import settings
from app.integrations.kubernetes_client import KubernetesClient

# ---------------------------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------------------------
deploy_duration = Histogram(
    'paas_deploy_duration_seconds', 
    'Duración del despliegue en segundos', 
    ['environment', 'policy']
)
deploy_success = Counter(
    'paas_deploy_success_total', 
    'Total de despliegues exitosos', 
    ['environment']
)
deploy_failed = Counter(
    'paas_deploy_failed_total', 
    'Total de despliegues fallidos', 
    ['environment']
)
rollback_events = Counter(
    'paas_rollback_total', 
    'Total de rollbacks ejecutados', 
    ['environment', 'type']
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("worker")

QUEUE_STAGING = "deployments.staging"
QUEUE_PRODUCTION = "deployments.production"
QUEUE_DLQ = "deployments.dlq"

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"
MAX_HEALTH_CHECKS = 10


def _to_k8s_name(value: str, fallback: str = "app") -> str:
    """Normalize a string to a DNS-1123 compatible name for Kubernetes."""
    if not value:
        return fallback
    normalized = value.lower()
    normalized = re.sub(r"[^a-z0-9.-]", "-", normalized)
    normalized = re.sub(r"-+", "-", normalized)
    normalized = re.sub(r"^[^a-z0-9]+", "", normalized)
    normalized = re.sub(r"[^a-z0-9]+$", "", normalized)
    return normalized or fallback


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def _log_event(
    prisma: Prisma,
    deployment_id: str,
    event_type: str,
    event_status: str,
    message: str,
    details: dict = None,
):
    """Registra un evento de trazabilidad en deployment_events."""
    data = {
        "deployment": {"connect": {"id": deployment_id}},
        "event_type": DeploymentEventType(event_type),
        "source": "WORKER",
        "message": message,
    }
    if event_status:
        data["event_status"] = DeploymentStatus(event_status)

    if details:
        data["details"] = Json(details)
    await prisma.deploymentevent.create(data=data)


async def _set_status(prisma: Prisma, deployment_id: str, update_data: dict):
    """Actualiza el deployment en la base de datos asegurando tipos correctos."""
    if "status" in update_data and isinstance(update_data["status"], str):
        update_data["status"] = DeploymentStatus(update_data["status"])
    
    await prisma.deployment.update(where={"id": deployment_id}, data=update_data)


def _parse_env_vars(env_vars) -> dict:
    """Asegura que env_vars sea un diccionario de Python válido (Prisma a veces lo retorna string)"""
    if not env_vars:
        return {}
    if isinstance(env_vars, dict):
        return env_vars
    if isinstance(env_vars, str):
        try:
            return json.loads(env_vars)
        except Exception:
            return {}
    return dict(env_vars)



async def _record_metric(prisma: Prisma, deployment_id: str, name: str, value: float, unit: str = "seconds"):
    """Registra una métrica en la tabla deployment_metrics para el análisis estadístico."""
    try:
        await prisma.deploymentmetric.create(data={
            "deployment_id": deployment_id,
            "metric_name": name,
            "metric_value": value,
            "unit": unit
        })
    except Exception as e:
        logger.error(f"Error al registrar métrica {name}: {e}")


# ---------------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------------

async def process_deployment(prisma: Prisma, k8s: KubernetesClient, payload: dict):
    """
    Procesa un mensaje de despliegue completo:
      RUNNING → K8s apply → health checks → SUCCESS / ROLLED_BACK / FAILED
    """
    deployment_id = payload["deployment_id"]
    service_name = payload["service_name"]
    image = payload["image"]
    environment = payload["environment"]
    policy = payload.get("policy", "replace")
    namespace = payload.get("k8s_namespace") or f"{environment}-ns"
    resource_name = payload.get("k8s_resource_name") or service_name
    health_path = payload.get("health_path", "/health")
    container_port = payload.get("container_port", 8000)
    env_vars = payload.get("env_vars")

    is_rollback = payload.get("is_rollback", False)

    safe_service_name = _to_k8s_name(service_name)
    safe_resource_name = _to_k8s_name(resource_name, fallback=safe_service_name)

    logger.info(
        f"▶ Procesando deployment {deployment_id} ({service_name} → {environment})"
        f"{' [ROLLBACK]' if is_rollback else ''}"
    )

    try:
        # Si es un rollback solicitado manualmente, primero debemos encontrar la imagen a la cual volver
        if is_rollback:
            logger.info(f"🔍 Buscando versión anterior para rollback manual de {service_name}")
            last_success = await prisma.deployment.find_first(
                where={
                    "service_name": service_name,
                    "environment": DeploymentEnvironment(environment),
                    "status": DeploymentStatus.SUCCESS,
                    "id": {"not": deployment_id}
                },
                order={"created_at": "desc"}
            )
            
            if last_success:
                image = last_success.image
                env_vars = _parse_env_vars(last_success.env_vars)
                logger.info(f"↩ Imagen recuperada para rollback manual: {image}")
            else:
                raise RuntimeError("No se encontró una versión anterior exitosa para realizar el rollback manual")

        # 1. Marcar como RUNNING
        now = datetime.now(timezone.utc)
        await _set_status(prisma, deployment_id, {
            "status": DeploymentStatus.RUNNING,
            "started_at": now,
            "worker_id": WORKER_ID,
        })
        await _log_event(
            prisma, deployment_id, "STARTED", "RUNNING",
            f"Worker tomó el deployment. Ejecutando {service_name}:{image} en {environment}...",
        )

        # 2. Aplicar en Kubernetes
        apply_result = await k8s.apply_deployment(
            namespace, safe_resource_name, image, policy, safe_service_name,
            health_path=health_path, port=container_port,
            env_vars=env_vars, environment=environment
        )

        if not apply_result["success"]:
            raise RuntimeError(f"kubectl apply falló: {apply_result['message']}")

        await _log_event(
            prisma, deployment_id, "HEALTHCHECK_OK", "RUNNING",
            f"Despliegue aplicado (revisión {apply_result.get('revision', '?')}). Verificando salud...",
            details=apply_result,
        )

        # 3. Health checks con reintentos
        rollout_ok = False
        for attempt in range(1, MAX_HEALTH_CHECKS + 1):
            logger.info(f"Health check {attempt}/{MAX_HEALTH_CHECKS} para {deployment_id}")
            status_result = await k8s.check_rollout_status(namespace, safe_resource_name)

            if status_result["ready"]:
                rollout_ok = True
                await _log_event(
                    prisma, deployment_id, "HEALTHCHECK_OK", "RUNNING",
                    f"Health check {attempt}/{MAX_HEALTH_CHECKS} OK — "
                    f"{status_result['available_replicas']} réplica(s) disponibles",
                    details=status_result,
                )
                break
            else:
                await _log_event(
                    prisma, deployment_id, "HEALTHCHECK_FAIL", "RUNNING",
                    f"Health check {attempt}/{MAX_HEALTH_CHECKS} falló: {status_result['message']}",
                    details=status_result,
                )
                if attempt < MAX_HEALTH_CHECKS:
                    await asyncio.sleep(3)

        # 4. Resultado final
        finished = datetime.now(timezone.utc)

        if rollout_ok:
            await _set_status(prisma, deployment_id, {
                "status": DeploymentStatus.SUCCESS,
                "success": True,
                "finished_at": finished,
                "rollout_revision": apply_result.get("revision"),
            })
            await _log_event(
                prisma, deployment_id, "FINISHED", "SUCCESS",
                f"✅ Despliegue de {service_name} completado exitosamente en {environment}",
            )
            logger.info(f"✅ Deployment {deployment_id} → SUCCESS")
            
            # Registrar Métricas de Éxito
            duration = (finished - now).total_seconds()
            deploy_duration.labels(environment=environment, policy=policy).observe(duration)
            deploy_success.labels(environment=environment).inc()
            
            await _record_metric(prisma, deployment_id, "deploy_duration", duration)
            await _record_metric(prisma, deployment_id, "success_rate", 1.0, unit="ratio")
            if is_rollback:
                rollback_events.labels(environment=environment, type="manual").inc()
                await _record_metric(prisma, deployment_id, "is_rollback", 1.0, unit="boolean")

        else:
            # Health checks fallaron → intentar rollback buscando la imagen anterior en la DB
            logger.warning(f"⚠ Deployment {deployment_id} falló — buscando versión anterior para rollback")
            
            last_success = await prisma.deployment.find_first(
                where={
                    "service_name": service_name,
                    "environment": DeploymentEnvironment(environment),
                    "status": DeploymentStatus.SUCCESS,
                    "id": {"not": deployment_id}
                },
                order={"created_at": "desc"}
            )

            if last_success:
                logger.info(f"↩ Reventiendo a la imagen anterior: {last_success.image}")
                await _log_event(
                    prisma, deployment_id, "ROLLBACK_STARTED", "RUNNING",
                    f"Iniciando reversión automática a la versión anterior ({last_success.image})...",
                )
                
                rollback_result = await k8s.apply_deployment(
                    namespace, safe_resource_name, last_success.image, "replace", safe_service_name,
                    health_path=health_path, 
                    port=container_port,
                    env_vars=_parse_env_vars(last_success.env_vars),
                    environment=environment
                )
                
                if rollback_result["success"]:
                    # Esperar a que el rollback sea exitoso (Health Check)
                    logger.info(f"Esperando salud del rollback para {service_name}...")
                    rb_ok = False
                    for rb_attempt in range(1, 6):
                        rb_status = await k8s.check_rollout_status(namespace, safe_resource_name)
                        if rb_status["ready"]:
                            rb_ok = True
                            break
                        await asyncio.sleep(3)

                    finished = datetime.now(timezone.utc)
                    await _set_status(prisma, deployment_id, {
                        "status": DeploymentStatus.ROLLED_BACK,
                        "success": False,
                        "rollback_required": True,
                        "rollback_performed": True,
                        "finished_at": finished,
                        "error_message": "Health checks fallaron. Rollback ejecutado automáticamente." if rb_ok else "Health checks fallaron. Rollback ejecutado pero no alcanzó estado saludable.",
                    })
                    
                    status_msg = "↩ Rollback completado y verificado." if rb_ok else "⚠️ Rollback aplicado pero no se pudo verificar salud."
                    await _log_event(
                        prisma, deployment_id, "ROLLBACK_OK" if rb_ok else "ROLLBACK_FAIL", "ROLLED_BACK",
                        f"{status_msg} Versión anterior ({last_success.image}) restaurada.",
                        details=rollback_result,
                    )
                    
                    # Métricas Rollback exitoso
                    duration = (finished - now).total_seconds()
                    deploy_duration.labels(environment=environment, policy=policy).observe(duration)
                    rollback_events.labels(environment=environment, type="auto").inc()
                    
                    await _record_metric(prisma, deployment_id, "deploy_duration", duration)
                    await _record_metric(prisma, deployment_id, "is_rollback", 1.0, unit="boolean")
                    await _record_metric(prisma, deployment_id, "auto_recovery", 1.0, unit="boolean")
                else:
                    await _set_status(prisma, deployment_id, {
                        "status": DeploymentStatus.FAILED,
                        "success": False,
                        "finished_at": finished,
                        "error_message": f"Rollback falló: {rollback_result.get('message')}",
                    })
            else:
                logger.error("❌ No se encontró una versión anterior exitosa para hacer rollback")
                await _set_status(prisma, deployment_id, {
                    "status": DeploymentStatus.FAILED,
                    "success": False,
                    "finished_at": finished,
                    "error_message": "Health checks fallaron y no hay versión previa exitosa.",
                })
                deploy_failed.labels(environment=environment).inc()
                await _record_metric(prisma, deployment_id, "success_rate", 0.0, unit="ratio")

    except Exception as exc:
        logger.exception(f"Error inesperado procesando deployment {deployment_id}: {exc}")
        try:
            finished = datetime.now(timezone.utc)
            await _set_status(prisma, deployment_id, {
                "status": DeploymentStatus.FAILED,
                "success": False,
                "finished_at": finished,
                "error_message": str(exc)[:500],
            })
            await _log_event(
                prisma, deployment_id, "ERROR", "FAILED",
                f"Error inesperado en el worker: {str(exc)[:300]}",
            )
            deploy_failed.labels(environment=environment).inc()
            await _record_metric(prisma, deployment_id, "success_rate", 0.0, unit="ratio")
        except Exception as inner:
            logger.exception(f"No se pudo actualizar estado: {inner}")
        raise


async def main():
    logger.info("🚀 Iniciando Deployment Worker...")
    
    # Iniciar servidor de métricas para el Worker en puerto 8001
    try:
        start_http_server(8001)
        logger.info("📊 Servidor de métricas Prometheus iniciado en puerto 8001")
    except Exception as e:
        logger.warning(f"No se pudo iniciar servidor de métricas: {e}")

    prisma = Prisma()
    await prisma.connect()
    k8s = KubernetesClient(simulate=settings.SIMULATE_K8S)
    
    connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    
    # Canal para Staging (Procesamiento en paralelo)
    channel_staging = await connection.channel()
    await channel_staging.set_qos(prefetch_count=5)
    
    # Canal para Producción (Procesamiento serial estricto)
    channel_prod = await connection.channel()
    await channel_prod.set_qos(prefetch_count=1)

    async def on_message(message: aio_pika.IncomingMessage):
        async with message.process(requeue=False):
            try:
                payload = json.loads(message.body.decode())
                await process_deployment(prisma, k8s, payload)
            except Exception:
                logger.exception("Procesamiento falló")

    # Declarar colas en sus respectivos canales
    await channel_staging.declare_queue(QUEUE_DLQ, durable=True)
    # La DLQ debe declararse en ambos canales para que RabbitMQ la reconozca
    await channel_prod.declare_queue(QUEUE_DLQ, durable=True)
    
    staging_q = await channel_staging.declare_queue(
        QUEUE_STAGING, 
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": QUEUE_DLQ,
        }
    )
    production_q = await channel_prod.declare_queue(
        QUEUE_PRODUCTION, 
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": QUEUE_DLQ,
        }
    )
    await staging_q.consume(on_message)
    await production_q.consume(on_message)

    logger.info("⏳ Worker listo...")
    try:
        await asyncio.Future()
    finally:
        await prisma.disconnect()
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
