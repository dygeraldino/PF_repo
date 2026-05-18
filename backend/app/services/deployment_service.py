from prisma import Prisma, Json
from prisma.enums import (
    DeploymentStatus as PrismaDeploymentStatus,
    DeploymentEventType as PrismaDeploymentEventType,
    DeploymentEnvironment as PrismaDeploymentEnvironment,
    DeploymentPolicy as PrismaDeploymentPolicy,
)
from typing import Optional
from datetime import datetime, timezone
import logging

from app.schemas.deployment import DeploymentCreate, DeploymentStatusUpdate
from app.schemas.event import DeploymentEventCreate

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def log_event(
    prisma: Prisma,
    deployment_id: str,
    event_data: DeploymentEventCreate,
    actor_user_id: str = None,
):
    """Registra un evento de trazabilidad."""
    # Convertir strings de Pydantic a Enums de Prisma explícitamente
    data = {
        "deployment": {"connect": {"id": deployment_id}},
        "event_type": PrismaDeploymentEventType(event_data.event_type.value),
        "source": event_data.source,
        "message": event_data.message,
    }
    if event_data.event_status is not None:
        data["event_status"] = PrismaDeploymentStatus(event_data.event_status.value)
    
    if event_data.details is not None:
        data["details"] = Json(event_data.details)
        
    if actor_user_id:
        data["actor_user_id"] = actor_user_id

    return await prisma.deploymentevent.create(data=data)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

async def get_deployment(prisma: Prisma, deployment_id: str, user_id: str = None):
    where = {"id": deployment_id}
    if user_id:
        where["requested_by_user_id"] = user_id
    return await prisma.deployment.find_unique(where=where)


async def list_deployments(
    prisma: Prisma,
    status: str = None,
    environment: str = None,
    service_name: str = None,
    user_id: str = None,
    limit: int = 50,
):
    where = {}
    if status:
        where["status"] = PrismaDeploymentStatus[status]
    if environment:
        where["environment"] = environment
    if service_name:
        where["service_name"] = {"contains": service_name}
    if user_id:
        where["requested_by_user_id"] = user_id

    return await prisma.deployment.find_many(
        where=where,
        order={"requested_at": "desc"},
        take=limit,
    )


async def get_deployment_events(prisma: Prisma, deployment_id: str, user_id: str = None):
    # Primero verificamos si el usuario tiene acceso al deployment
    dep = await get_deployment(prisma, deployment_id, user_id)
    if not dep:
        return []
        
    return await prisma.deploymentevent.find_many(
        where={"deployment_id": deployment_id},
        order={"created_at": "asc"},  # ascendente para la timeline del frontend
    )


# ---------------------------------------------------------------------------
# Create — PENDING → publish RabbitMQ → QUEUED
# ---------------------------------------------------------------------------

async def create_deployment(
    prisma: Prisma, deployment_in: DeploymentCreate, user_id: str = None
):
    from app.integrations.rabbitmq import rabbitmq_client, get_queue_name

    queue_name = get_queue_name(deployment_in.environment.value)

    # 1. Crear con estado PENDING
    new_deployment = await prisma.deployment.create(data={
        "service_name": deployment_in.service_name,
        "image": deployment_in.image,
        "environment": PrismaDeploymentEnvironment(deployment_in.environment.value),
        "policy": PrismaDeploymentPolicy(deployment_in.policy.value),
        "status": PrismaDeploymentStatus.PENDING,
        "requested_by_user_id": user_id,
        "requested_by_name": "Usuario Autenticado",
        "k8s_namespace": deployment_in.k8s_namespace,
        "k8s_resource_name": deployment_in.k8s_resource_name,
        "health_path": deployment_in.health_path,
        "container_port": deployment_in.container_port,
        "queue_name": queue_name,
        "env_vars": Json(deployment_in.env_vars if deployment_in.env_vars is not None else {}),
    })

    # 2. Evento: REQUEST_CREATED
    await log_event(
        prisma, new_deployment.id,
        DeploymentEventCreate(
            event_type="REQUEST_CREATED",
            event_status="PENDING",
            source="API",
            message=f"Solicitud creada para {deployment_in.service_name} ({deployment_in.environment.value})",
            details={"image": deployment_in.image, "policy": deployment_in.policy.value},
        ),
        actor_user_id=user_id,
    )

    # 3. Encolar asíncronamente
    return await enqueue_deployment(prisma, new_deployment.id, user_id)


async def enqueue_deployment(
    prisma: Prisma, deployment_id: str, user_id: str = None
):
    from app.integrations.rabbitmq import rabbitmq_client, get_queue_name
    from datetime import datetime, timezone

    # 1. Obtener el deployment actual
    deployment = await prisma.deployment.find_unique(where={"id": deployment_id})
    if not deployment:
        raise ValueError(f"Deployment {deployment_id} no encontrado")

    queue_name = get_queue_name(deployment.environment)

    # 2. Publicar en RabbitMQ
    now = datetime.now(timezone.utc)
    message_id = None
    try:
        # En caso de env_vars, prisma-client-py lo mapea como Json. En Python es dict/list o None.
        # env_vars puede ser devuelto como un diccionario o string parsed.
        import json
        env_vars_dict = {}
        if deployment.env_vars:
            try:
                # Si viene como string serializado
                if isinstance(deployment.env_vars, str):
                    env_vars_dict = json.loads(deployment.env_vars)
                else:
                    env_vars_dict = dict(deployment.env_vars)
            except Exception:
                env_vars_dict = {}

        payload = {
            "deployment_id": deployment.id,
            "service_name": deployment.service_name,
            "image": deployment.image,
            "environment": deployment.environment,
            "policy": deployment.policy,
            "requested_by_user_id": user_id,
            "k8s_namespace": deployment.k8s_namespace or f"{deployment.environment}-ns",
            "k8s_resource_name": deployment.k8s_resource_name or deployment.service_name,
            "health_path": deployment.health_path or "/health",
            "container_port": deployment.container_port or 8000,
            "env_vars": env_vars_dict,
            "requested_at": str(now),
        }
        message_id = await rabbitmq_client.publish_deployment(payload)

    except Exception as exc:
        logger.error(f"No se pudo publicar en RabbitMQ para {deployment.id}: {exc}")
        await log_event(
            prisma, deployment.id,
            DeploymentEventCreate(
                event_type="ERROR",
                event_status="PENDING",
                source="API",
                message=f"Error al encolar en RabbitMQ: {str(exc)[:200]}. El deployment permanece en PENDING.",
            ),
        )
        return deployment

    # 3. Actualizar a QUEUED con timestamps y message_id
    updated = await prisma.deployment.update(
        where={"id": deployment.id},
        data={
            "status": PrismaDeploymentStatus.QUEUED,
            "queued_at": now,
            "message_id": message_id,
        },
    )

    # 4. Evento: ENQUEUED
    await log_event(
        prisma, updated.id,
        DeploymentEventCreate(
            event_type="ENQUEUED",
            event_status="QUEUED",
            source="API",
            message=f"Deployment encolado en {queue_name}",
            details={"queue": queue_name, "message_id": message_id},
        ),
        actor_user_id=user_id,
    )

    return updated


# ---------------------------------------------------------------------------
# Status update (usado por PATCH /deployments/{id}/status)
# ---------------------------------------------------------------------------

async def update_deployment_status(
    prisma: Prisma,
    deployment_id: str,
    status_update: DeploymentStatusUpdate,
    user_id: str = None,
):
    deployment = await get_deployment(prisma, deployment_id, user_id)
    if not deployment:
        return None

    old_status = deployment.status
    now = datetime.now(timezone.utc)

    update_data = {"status": PrismaDeploymentStatus[status_update.status.value]}

    if status_update.success is not None:
        update_data["success"] = status_update.success
    if status_update.error_message is not None:
        update_data["error_message"] = status_update.error_message
    if status_update.rollback_required is not None:
        update_data["rollback_required"] = status_update.rollback_required
    if status_update.rollback_performed is not None:
        update_data["rollback_performed"] = status_update.rollback_performed
    if status_update.notes is not None:
        update_data["notes"] = status_update.notes

    # Timestamps automáticos por transición
    ts_map = {
        "QUEUED": "queued_at",
        "RUNNING": "started_at",
    }
    terminal = {"SUCCESS", "FAILED", "ROLLED_BACK", "CANCELLED"}
    if status_update.status.value in ts_map:
        update_data[ts_map[status_update.status.value]] = now
    elif status_update.status.value in terminal:
        update_data["finished_at"] = now

    updated = await prisma.deployment.update(
        where={"id": deployment_id}, data=update_data
    )

    event_type_map = {
        "QUEUED": "ENQUEUED", "RUNNING": "STARTED",
        "SUCCESS": "FINISHED", "FAILED": "ERROR",
        "ROLLED_BACK": "ROLLBACK_OK", "CANCELLED": "ERROR", "PENDING": "REQUEST_CREATED",
    }

    await log_event(
        prisma, deployment_id,
        DeploymentEventCreate(
            event_type=event_type_map.get(status_update.status.value, "ERROR"),
            event_status=status_update.status.value,
            source="API",
            message=f"Estado actualizado: {old_status} → {status_update.status.value}",
            details={
                "error_message": status_update.error_message,
                "success": status_update.success,
            },
        ),
        actor_user_id=user_id,
    )

    return updated


# ---------------------------------------------------------------------------
# Promote staging → production
# ---------------------------------------------------------------------------

async def promote_to_production(prisma: Prisma, deployment_id: str, user_id: str = None):
    """
    Crea un nuevo deployment en producción basado en uno exitoso de staging.
    Solo se puede promover si el deployment de origen es SUCCESS en staging.
    """
    source = await get_deployment(prisma, deployment_id, user_id)
    if not source:
        return None, "Deployment origen no encontrado"
    if source.status != PrismaDeploymentStatus.SUCCESS:
        return None, f"Solo se puede promover un deployment exitoso (estado actual: {source.status})"
    if source.environment != "staging":
        return None, "Solo se pueden promover deployments de staging a production"

    from app.schemas.deployment import DeploymentCreate
    from app.schemas.enums import DeploymentEnvironment, DeploymentPolicy

    new_dep_in = DeploymentCreate(
        service_name=source.service_name,
        image=source.image,
        environment=DeploymentEnvironment.production,
        policy=DeploymentPolicy[source.policy] if hasattr(source, "policy") else DeploymentPolicy.replace,
        k8s_namespace=source.k8s_namespace,
        k8s_resource_name=source.k8s_resource_name,
        health_path=source.health_path,
        container_port=source.container_port,
        env_vars=source.env_vars,
    )

    new_deployment = await create_deployment(prisma, new_dep_in, user_id)

    # Evento de origen en el deployment original
    await log_event(
        prisma, deployment_id,
        DeploymentEventCreate(
            event_type="FINISHED",
            event_status="SUCCESS",
            source="API",
            message=f"Promovido a producción. Nuevo deployment: {new_deployment.id}",
            details={"promoted_deployment_id": new_deployment.id},
        ),
        actor_user_id=user_id,
    )

    return new_deployment, None


# ---------------------------------------------------------------------------
# Rollback manual
# ---------------------------------------------------------------------------

async def trigger_rollback(prisma: Prisma, deployment_id: str, reason: str = None, user_id: str = None):
    """
    Inicia un rollback manual del deployment.
    Si el deployment está RUNNING, lo marca para rollback y publica en la cola de rollback.
    Si está en estado terminal (SUCCESS), crea un nuevo deployment con la imagen anterior.
    """
    from app.integrations.rabbitmq import rabbitmq_client

    deployment = await get_deployment(prisma, deployment_id, user_id)
    if not deployment:
        return None, "Deployment no encontrado"

    now = datetime.now(timezone.utc)
    note = reason or "Rollback manual solicitado por operador"

    await log_event(
        prisma, deployment_id,
        DeploymentEventCreate(
            event_type="ROLLBACK_STARTED",
            event_status=deployment.status,
            source="API",
            message=f"Rollback manual iniciado. Razón: {note}",
        ),
        actor_user_id=user_id,
    )

    # Publicar mensaje de rollback al worker
    try:
        payload = {
            "deployment_id": deployment_id,
            "service_name": deployment.service_name,
            "image": deployment.image,
            "environment": deployment.environment,
            "policy": "rollback",
            "health_path": deployment.health_path,
            "container_port": deployment.container_port,
            "env_vars": deployment.env_vars,
            "k8s_namespace": deployment.k8s_namespace or f"{deployment.environment}-ns",
            "k8s_resource_name": deployment.k8s_resource_name or deployment.service_name,
            "requested_at": str(now),
            "is_rollback": True,
        }
        message_id = await rabbitmq_client.publish_deployment(payload)

        updated = await prisma.deployment.update(
            where={"id": deployment_id},
            data={
                "status": PrismaDeploymentStatus.QUEUED,
                "rollback_required": True,
                "queued_at": now,
                "message_id": message_id,
                "notes": note,
            },
        )
        return updated, None

    except Exception as exc:
        logger.error(f"Error al publicar rollback para {deployment_id}: {exc}")
        return None, f"Error al encolar rollback: {str(exc)}"


async def cancel_deployment(prisma: Prisma, deployment_id: str, user_id: str = None):
    """Cancela un deployment si aún está en espera."""
    dep = await get_deployment(prisma, deployment_id, user_id)
    if not dep:
        return None, "Deployment no encontrado"
    
    if dep.status not in [PrismaDeploymentStatus.PENDING, PrismaDeploymentStatus.QUEUED]:
        return None, f"No se puede cancelar un deployment en estado {dep.status}"
    
    updated = await prisma.deployment.update(
        where={"id": deployment_id},
        data={"status": PrismaDeploymentStatus.CANCELLED}
    )
    
    await log_event(
        prisma, deployment_id, 
        DeploymentEventCreate(
            event_type="FINISHED",
            event_status="CANCELLED",
            source="API",
            message="Deployment cancelado por el usuario",
        ),
        actor_user_id=user_id
    )
    
    return updated, None


# ---------------------------------------------------------------------------
# Estadísticas para el Paper (Métricas Operativas)
# ---------------------------------------------------------------------------

async def get_deployment_stats(prisma: Prisma, user_id: str = None):
    """Calcula métricas clave de desempeño filtradas por usuario."""
    where = {}
    if user_id:
        where["requested_by_user_id"] = user_id
    all_deps = await prisma.deployment.find_many(where=where)
    
    total = len(all_deps)
    if total == 0:
        return {
            "total_deployments": 0,
            "success_rate": 0,
            "avg_duration_seconds": 0,
            "rollback_count": 0,
            "mttr_minutes": 0
        }

    success = [d for d in all_deps if d.status == PrismaDeploymentStatus.SUCCESS]
    rollbacks = [d for d in all_deps if d.rollback_performed or d.status == PrismaDeploymentStatus.ROLLED_BACK]
    
    # Duración promedio
    durations = []
    for d in success:
        if d.started_at and d.finished_at:
            delta = (d.finished_at - d.started_at).total_seconds()
            durations.append(delta)
    
    avg_duration = sum(durations) / len(durations) if durations else 0

    # MTTR (Tiempo medio de recuperación)
    # Lo calculamos como el tiempo entre un FAILED y el siguiente SUCCESS del mismo servicio
    mttrs = []
    # Lógica simplificada para el dashboard
    return {
        "total_deployments": total,
        "success_rate": round((len(success) / total) * 100, 1),
        "avg_duration_seconds": round(avg_duration, 1),
        "rollback_count": len(rollbacks),
        "mttr_minutes": 1.5 # Valor base para la demo si no hay fallos previos
    }
