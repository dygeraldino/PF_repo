from fastapi import APIRouter, Depends, HTTPException, Query
from prisma import Prisma
from typing import List, Optional

from app.core.database import get_prisma
from app.schemas.deployment import DeploymentCreate, DeploymentResponse, DeploymentStatusUpdate
from app.schemas.event import DeploymentEventCreate, DeploymentEventResponse
from app.schemas.enums import DeploymentStatus
from app.services import deployment_service
from app.api.dependencies import get_current_user_id

router = APIRouter(prefix="/deployments", tags=["deployments"])

def _to_deployment_response(d) -> dict:
    """Convierte el objeto Prisma a un dict serializable para Pydantic."""
    return {
        "id": str(d.id),
        "service_name": d.service_name,
        "image": d.image,
        "environment": d.environment,
        "policy": d.policy,
        "status": d.status,
        "requested_by_user_id": str(d.requested_by_user_id) if d.requested_by_user_id else None,
        "requested_by_name": d.requested_by_name,
        "requested_at": d.requested_at,
        "queued_at": d.queued_at,
        "started_at": d.started_at,
        "finished_at": d.finished_at,
        "queue_name": d.queue_name,
        "message_id": str(d.message_id) if d.message_id else None,
        "worker_id": d.worker_id,
        "retry_count": d.retry_count,
        "k8s_namespace": d.k8s_namespace,
        "k8s_resource_name": d.k8s_resource_name,
        "rollout_revision": d.rollout_revision,
        "rollback_required": d.rollback_required,
        "rollback_performed": d.rollback_performed,
        "success": d.success,
        "error_message": d.error_message,
        "notes": d.notes,
        "created_at": d.created_at,
        "updated_at": d.updated_at,
    }

def _to_event_response(e) -> dict:
    return {
        "id": e.id,
        "deployment_id": str(e.deployment_id),
        "event_type": e.event_type,
        "event_status": e.event_status,
        "source": e.source,
        "message": e.message,
        "details": e.details,
        "created_at": e.created_at,
        "actor_user_id": str(e.actor_user_id) if e.actor_user_id else None,
    }

@router.post("", response_model=DeploymentResponse, status_code=201)
async def create_deployment(
    deployment_in: DeploymentCreate,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id)
):
    """Crea una solicitud de despliegue."""
    result = await deployment_service.create_deployment(prisma, deployment_in, user_id)
    return DeploymentResponse(**_to_deployment_response(result))

@router.get("", response_model=List[DeploymentResponse])
async def list_deployments(
    status: Optional[DeploymentStatus] = Query(None),
    environment: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    prisma: Prisma = Depends(get_prisma)
):
    """Lista solicitudes con filtros opcionales por estado, entorno y usuario."""
    results = await deployment_service.list_deployments(
        prisma,
        status=status.value if status else None,
        environment=environment,
        user_id=user_id
    )
    return [DeploymentResponse(**_to_deployment_response(d)) for d in results]

@router.get("/{id}", response_model=DeploymentResponse)
async def get_deployment(
    id: str,
    prisma: Prisma = Depends(get_prisma)
):
    """Devuelve detalle de una solicitud."""
    deployment = await deployment_service.get_deployment(prisma, id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment no encontrado")
    return DeploymentResponse(**_to_deployment_response(deployment))

@router.patch("/{id}/status", response_model=DeploymentResponse)
async def update_deployment_status(
    id: str,
    status_update: DeploymentStatusUpdate,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id)
):
    """Actualiza el estado del despliegue."""
    deployment = await deployment_service.update_deployment_status(prisma, id, status_update, user_id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment no encontrado")
    return DeploymentResponse(**_to_deployment_response(deployment))

@router.post("/{id}/events", response_model=DeploymentEventResponse, status_code=201)
async def create_deployment_event(
    id: str,
    event_in: DeploymentEventCreate,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id)
):
    """Registra un evento de trazabilidad."""
    deployment = await deployment_service.get_deployment(prisma, id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment no encontrado")
    event = await deployment_service.log_event(prisma, id, event_in, user_id)
    return DeploymentEventResponse(**_to_event_response(event))

@router.get("/{id}/events", response_model=List[DeploymentEventResponse])
async def get_deployment_events(
    id: str,
    prisma: Prisma = Depends(get_prisma)
):
    """Consulta el historial de eventos."""
    deployment = await deployment_service.get_deployment(prisma, id)
    if not deployment:
        raise HTTPException(status_code=404, detail="Deployment no encontrado")
    events = await deployment_service.get_deployment_events(prisma, id)
    return [DeploymentEventResponse(**_to_event_response(e)) for e in events]
