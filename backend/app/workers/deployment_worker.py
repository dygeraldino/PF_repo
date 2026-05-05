"""
Worker de despliegues — proceso independiente que consume de RabbitMQ.

Flujo por mensaje:
  QUEUED → RUNNING → (K8s apply + health checks) → SUCCESS | ROLLED_BACK | FAILED

Ejecutar con:
  python -m app.workers.deployment_worker
  o via docker-compose (servicio 'worker')
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import aio_pika
from prisma import Prisma, Json
from prisma.enums import (
    DeploymentStatus,
    DeploymentEventType,
    DeploymentEnvironment,
    DeploymentPolicy,
)

from app.core.config import settings
from app.integrations.kubernetes_client import KubernetesClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("worker")

QUEUE_STAGING = "deployments.staging"
QUEUE_PRODUCTION = "deployments.production"
QUEUE_DLQ = "deployments.dlq"

MAX_HEALTH_CHECKS = 3


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
    # Asegurar que usamos el miembro del Enum de Prisma invocando el constructor
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
    """Actualiza el deployment en la base de datos."""
    await prisma.deployment.update(where={"id": deployment_id}, data=update_data)


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

    logger.info(f"▶ Procesando deployment {deployment_id} ({service_name} → {environment})")

    try:
        # 1. Marcar como RUNNING
        now = datetime.now(timezone.utc)
        await _set_status(prisma, deployment_id, {
            "status": DeploymentStatus.RUNNING,
            "started_at": now,
        })
        await _log_event(
            prisma, deployment_id, "STARTED", "RUNNING",
            f"Worker tomó el deployment. Ejecutando {service_name}:{image} en {environment}...",
        )

        # 2. Aplicar en Kubernetes
        apply_result = await k8s.apply_deployment(namespace, resource_name, image, policy)

        if not apply_result["success"]:
            raise RuntimeError(f"kubectl apply falló: {apply_result['message']}")

        await _log_event(
            prisma, deployment_id, "HEALTHCHECK_OK", "RUNNING",
            f"Despliegue aplicado (revisión {apply_result.get('revision', '?')}). Verificando salud...",
            details=apply_result,
        )

        # 3. Health checks con reintentos
        rollout_ok = False
        last_status_result = {}

        for attempt in range(1, MAX_HEALTH_CHECKS + 1):
            logger.info(f"Health check {attempt}/{MAX_HEALTH_CHECKS} para {deployment_id}")
            status_result = await k8s.check_rollout_status(namespace, resource_name)
            last_status_result = status_result

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

        else:
            # Health checks fallaron → intentar rollback
            logger.warning(f"⚠ Deployment {deployment_id} falló — iniciando rollback")
            await _log_event(
                prisma, deployment_id, "ROLLBACK_STARTED", "RUNNING",
                f"Health checks fallaron después de {MAX_HEALTH_CHECKS} intentos. Iniciando rollback...",
            )

            rollback_result = await k8s.rollback_deployment(namespace, resource_name)
            finished = datetime.now(timezone.utc)

            if rollback_result["success"]:
                await _set_status(prisma, deployment_id, {
                    "status": DeploymentStatus.ROLLED_BACK,
                    "success": False,
                    "rollback_required": True,
                    "rollback_performed": True,
                    "finished_at": finished,
                    "error_message": "Health checks fallaron. Rollback ejecutado automáticamente.",
                })
                await _log_event(
                    prisma, deployment_id, "ROLLBACK_OK", "ROLLED_BACK",
                    f"↩ Rollback de {service_name} completado. Versión anterior restaurada.",
                    details=rollback_result,
                )
                logger.info(f"↩ Deployment {deployment_id} → ROLLED_BACK")
            else:
                await _set_status(prisma, deployment_id, {
                    "status": DeploymentStatus.FAILED,
                    "success": False,
                    "rollback_required": True,
                    "rollback_performed": False,
                    "finished_at": finished,
                    "error_message": f"Health checks y rollback fallaron: {rollback_result['message']}",
                })
                await _log_event(
                    prisma, deployment_id, "ROLLBACK_FAIL", "FAILED",
                    f"❌ Rollback también falló. Intervención manual requerida.",
                    details=rollback_result,
                )
                logger.error(f"❌ Deployment {deployment_id} → FAILED (rollback failed too)")

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
        except Exception as inner:
            logger.exception(f"No se pudo actualizar estado del deployment tras error: {inner}")
        raise  # Re-raise → aio-pika envía el mensaje a DLQ


# ---------------------------------------------------------------------------
# Worker entrypoint
# ---------------------------------------------------------------------------

async def main():
    logger.info("🚀 Iniciando Deployment Worker...")

    # Conectar Prisma
    prisma = Prisma()
    await prisma.connect()
    logger.info("✔ Prisma conectado a la base de datos")

    # Inicializar cliente K8s
    k8s = KubernetesClient(simulate=settings.SIMULATE_K8S)
    logger.info(f"✔ Kubernetes client listo (simulate={settings.SIMULATE_K8S})")

    # Conectar RabbitMQ
    connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    # Declarar colas (idempotente)
    await channel.declare_queue(QUEUE_DLQ, durable=True)
    for qname in [QUEUE_STAGING, QUEUE_PRODUCTION]:
        await channel.declare_queue(
            qname,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": QUEUE_DLQ,
            }
        )

    async def on_message(message: aio_pika.IncomingMessage):
        """Callback invocado por cada mensaje recibido."""
        async with message.process(requeue=False):
            try:
                payload = json.loads(message.body.decode())
                logger.info(f"📨 Mensaje recibido: deployment_id={payload.get('deployment_id')}")
                await process_deployment(prisma, k8s, payload)
            except Exception:
                logger.exception("Procesamiento del mensaje falló — enviando a DLQ")
                # aio-pika hace nack automáticamente al salir del context manager
                # con requeue=False, el DLQ routing-key del mensaje lo redirige a deployments.dlq

    # Suscribirse a ambas colas
    staging_q = await channel.get_queue(QUEUE_STAGING)
    production_q = await channel.get_queue(QUEUE_PRODUCTION)
    await staging_q.consume(on_message)
    await production_q.consume(on_message)

    logger.info(
        f"⏳ Worker listo. Escuchando en: {QUEUE_STAGING}, {QUEUE_PRODUCTION}"
    )

    try:
        await asyncio.Future()  # Correr indefinidamente
    finally:
        await prisma.disconnect()
        await connection.close()
        logger.info("Worker detenido.")


if __name__ == "__main__":
    asyncio.run(main())
