from prisma import Prisma, Json
from prisma.enums import DeploymentStatus as PrismaDeploymentStatus
from prisma.enums import DeploymentEventType as PrismaDeploymentEventType
from typing import Optional
from datetime import datetime, timezone

from app.schemas.deployment import DeploymentCreate, DeploymentStatusUpdate
from app.schemas.event import DeploymentEventCreate

async def log_event(prisma: Prisma, deployment_id: str, event_data: DeploymentEventCreate, actor_user_id: str = None):
    data = {
        "deployment": {"connect": {"id": deployment_id}},
        "event_type": PrismaDeploymentEventType[event_data.event_type.value],
        "source": event_data.source,
        "message": event_data.message,
    }
    if event_data.event_status is not None:
        data["event_status"] = PrismaDeploymentStatus[event_data.event_status.value]
    if event_data.details is not None:
        data["details"] = Json(event_data.details)
    if actor_user_id is not None:
        data["actor_user_id"] = actor_user_id

    return await prisma.deploymentevent.create(data=data)

async def create_deployment(prisma: Prisma, deployment_in: DeploymentCreate, user_id: str = None):
    # Crear la solicitud con estado inicial PENDING
    new_deployment = await prisma.deployment.create(data={
        "service_name": deployment_in.service_name,
        "image": deployment_in.image,
        "environment": deployment_in.environment.value,
        "policy": deployment_in.policy.value,
        "status": "PENDING",
        "requested_by_user_id": user_id,
        "k8s_namespace": deployment_in.k8s_namespace,
        "k8s_resource_name": deployment_in.k8s_resource_name,
        "queue_name": f"queue_{deployment_in.environment.value}"
    })

    # Registrar evento REQUEST_CREATED
    await log_event(
        prisma,
        new_deployment.id,
        DeploymentEventCreate(
            event_type="REQUEST_CREATED",
            event_status="PENDING",
            source="API",
            message=f"Solicitud de despliegue creada para {deployment_in.service_name} en {deployment_in.environment}",
            details={"image": deployment_in.image, "policy": deployment_in.policy.value}
        ),
        actor_user_id=user_id
    )

    # Hook futuro para RabbitMQ:
    # await rabbitmq_service.publish(new_deployment.queue_name, new_deployment.id)

    return new_deployment

async def get_deployment(prisma: Prisma, deployment_id: str):
    return await prisma.deployment.find_unique(where={"id": deployment_id})

async def list_deployments(prisma: Prisma, status: str = None, environment: str = None, user_id: str = None):
    where = {}
    if status:
        where["status"] = PrismaDeploymentStatus[status]
    if environment:
        where["environment"] = environment
    if user_id:
        where["requested_by_user_id"] = user_id

    return await prisma.deployment.find_many(
        where=where,
        order={"requested_at": "desc"}
    )

async def update_deployment_status(prisma: Prisma, deployment_id: str, status_update: DeploymentStatusUpdate, user_id: str = None):
    deployment = await get_deployment(prisma, deployment_id)
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

    # Marcas temporales automáticas
    if status_update.status.value == "QUEUED":
        update_data["queued_at"] = now
    elif status_update.status.value == "RUNNING":
        update_data["started_at"] = now
    elif status_update.status.value in ["SUCCESS", "FAILED", "ROLLED_BACK", "CANCELLED"]:
        update_data["finished_at"] = now

    updated_deployment = await prisma.deployment.update(
        where={"id": deployment_id},
        data=update_data
    )

    # Mapear estado a tipo de evento
    event_type_map = {
        "QUEUED": "ENQUEUED",
        "RUNNING": "STARTED",
        "SUCCESS": "FINISHED",
        "FAILED": "ERROR",
        "ROLLED_BACK": "ROLLBACK_OK",
        "CANCELLED": "ERROR",
        "PENDING": "REQUEST_CREATED"
    }
    event_type = event_type_map.get(status_update.status.value, "ERROR")

    await log_event(
        prisma,
        deployment_id,
        DeploymentEventCreate(
            event_type=event_type,
            event_status=status_update.status.value,
            source="API",
            message=f"Estado actualizado de {old_status} a {status_update.status.value}",
            details={"error_message": status_update.error_message, "success": status_update.success}
        ),
        actor_user_id=user_id
    )

    return updated_deployment

async def get_deployment_events(prisma: Prisma, deployment_id: str):
    return await prisma.deploymentevent.find_many(
        where={"deployment_id": deployment_id},
        order={"created_at": "desc"}
    )
