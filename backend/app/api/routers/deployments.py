from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from prisma import Prisma, Json
from pydantic import BaseModel
from typing import List, Optional

from app.core.database import get_prisma
from app.schemas.deployment import DeploymentCreate, DeploymentResponse, DeploymentStatusUpdate, RepoDeployRequest
from app.schemas.event import DeploymentEventCreate, DeploymentEventResponse
from app.schemas.enums import DeploymentStatus
from app.services import deployment_service
from app.services.repo_service import process_repo_deployment
from app.api.dependencies import get_current_user_id

router = APIRouter(prefix="/deployments", tags=["deployments"])


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _to_deployment_response(d) -> dict:
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
        "previous_deployment_id": str(d.previous_deployment_id) if d.previous_deployment_id else None,
        "rollback_required": d.rollback_required,
        "rollback_performed": d.rollback_performed,
        "success": d.success,
        "error_message": d.error_message,
        "notes": d.notes,
        "is_compose": d.is_compose,
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


# ---------------------------------------------------------------------------
# Endpoints — deployments CRUD
# ---------------------------------------------------------------------------

@router.post("", response_model=DeploymentResponse, status_code=201)
async def create_deployment(
    deployment_in: DeploymentCreate,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """Crea una solicitud de despliegue y la encola en RabbitMQ."""
    result = await deployment_service.create_deployment(prisma, deployment_in, user_id)
    return DeploymentResponse(**_to_deployment_response(result))


@router.post("/from-repo", status_code=202)
async def create_deployment_from_repo(
    request: RepoDeployRequest,
    background_tasks: BackgroundTasks,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """
    Clona un repositorio, construye la imagen y la carga en Kind en background.
    """
    import asyncio
    from prisma.enums import DeploymentEnvironment as PrismaDeploymentEnvironment
    from prisma.enums import DeploymentPolicy as PrismaDeploymentPolicy
    from prisma.enums import DeploymentStatus as PrismaDeploymentStatus
    from app.services.deployment_service import log_event
    from app.schemas.event import DeploymentEventCreate
    
    if not user_id:
        raise HTTPException(status_code=401, detail="No autorizado")

    check_proc = await asyncio.create_subprocess_shell(
        "kind get clusters",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await check_proc.communicate()
    existing_clusters = stdout.decode().strip().split('\n')
    
    msg = "Proceso de clonado y despliegue iniciado en segundo plano"
    if request.cluster_name in existing_clusters:
        msg = f"Aviso: El cluster '{request.cluster_name}' ya existe y será reutilizado. " + msg

    # 1. Crear el registro del deployment inmediatamente
    full_image_name = f"{request.image_name}:{request.image_version}"
    
    parsed_env_vars = {}
    if request.env_file_content:
        for line in request.env_file_content.strip().split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, val = line.split('=', 1)
                parsed_env_vars[key.strip()] = val.strip().strip('"').strip("'")

    new_deployment = await prisma.deployment.create(data={
        "service_name": request.service_name,
        "image": full_image_name,
        "environment": PrismaDeploymentEnvironment.staging,
        "policy": PrismaDeploymentPolicy.replace,
        "status": PrismaDeploymentStatus.PENDING,
        "requested_by_user_id": user_id,
        "requested_by_name": "Usuario Autenticado",
        "k8s_namespace": "staging-ns",
        "k8s_resource_name": request.service_name,
        "health_path": "/",
        "container_port": 80,
        "queue_name": "staging_queue",
        "env_vars": Json(parsed_env_vars),
        "is_compose": request.is_compose,
    })

    # 2. Registrar el primer evento del seguimiento
    await log_event(
        prisma, new_deployment.id,
        DeploymentEventCreate(
            event_type="REQUEST_CREATED",
            event_status="PENDING",
            source="API",
            message=f"Solicitud recibida. {msg}. Iniciando clonado y compilación.",
            details={"repo": request.repo_url, "image": full_image_name, "cluster": request.cluster_name},
        ),
        actor_user_id=user_id,
    )

    # 3. Lanzar tarea en segundo plano
    background_tasks.add_task(
        process_repo_deployment,
        deployment_id=new_deployment.id,
        repo_url=request.repo_url,
        docker_context_path=request.docker_context_path,
        service_name=request.service_name,
        cluster_name=request.cluster_name,
        image_name=request.image_name,
        image_version=request.image_version,
        env_file_content=request.env_file_content,
        is_compose=request.is_compose,

    )
    return {
        "id": new_deployment.id,
        "message": msg,
        "status": "processing"
    }


@router.get("", response_model=List[DeploymentResponse])
async def list_deployments(
    status: Optional[DeploymentStatus] = Query(None),
    environment: Optional[str] = Query(None),
    service_name: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    prisma: Prisma = Depends(get_prisma),
    user_id: str = Depends(get_current_user_id),
):
    """Lista deployments del usuario autenticado."""
    if not user_id:
        raise HTTPException(status_code=401, detail="No autorizado")

    results = await deployment_service.list_deployments(
        prisma,
        status=status.value if status else None,
        environment=environment,
        service_name=service_name,
        user_id=user_id,
        limit=limit,
    )
    return [DeploymentResponse(**_to_deployment_response(d)) for d in results]


# ---------------------------------------------------------------------------
# Endpoints — estadísticas
# ---------------------------------------------------------------------------

@router.get("/stats")
async def get_stats(
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """Obtiene estadísticas globales de despliegues para el análisis del paper."""
    return await deployment_service.get_deployment_stats(prisma, user_id)


@router.get("/{id}", response_model=DeploymentResponse)
async def get_deployment(
    id: str, 
    prisma: Prisma = Depends(get_prisma),
    user_id: str = Depends(get_current_user_id)
):
    """Detalle de un deployment (solo si el usuario es el dueño)."""
    if not user_id:
        raise HTTPException(status_code=401, detail="No autorizado")
        
    dep = await deployment_service.get_deployment(prisma, id, user_id)
    if not dep:
        raise HTTPException(status_code=404, detail="Deployment no encontrado o acceso denegado")
    return DeploymentResponse(**_to_deployment_response(dep))


@router.patch("/{id}/status", response_model=DeploymentResponse)
async def update_deployment_status(
    id: str,
    status_update: DeploymentStatusUpdate,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """Actualiza el estado de un deployment manualmente."""
    dep = await deployment_service.update_deployment_status(prisma, id, status_update, user_id)
    if not dep:
        raise HTTPException(status_code=404, detail="Deployment no encontrado")
    return DeploymentResponse(**_to_deployment_response(dep))


# ---------------------------------------------------------------------------
# Endpoints — events
# ---------------------------------------------------------------------------

@router.post("/{id}/events", response_model=DeploymentEventResponse, status_code=201)
async def create_deployment_event(
    id: str,
    event_in: DeploymentEventCreate,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """Registra un evento de trazabilidad manual."""
    dep = await deployment_service.get_deployment(prisma, id, user_id)
    if not dep:
        raise HTTPException(status_code=404, detail="Deployment no encontrado o acceso denegado")
    event = await deployment_service.log_event(prisma, id, event_in, user_id)
    return DeploymentEventResponse(**_to_event_response(event))


@router.get("/{id}/events", response_model=List[DeploymentEventResponse])
async def get_deployment_events(
    id: str, 
    prisma: Prisma = Depends(get_prisma),
    user_id: str = Depends(get_current_user_id)
):
    """Historial de eventos de un deployment (solo si el usuario es el dueño)."""
    if not user_id:
        raise HTTPException(status_code=401, detail="No autorizado")

    events = await deployment_service.get_deployment_events(prisma, id, user_id)
    # Si get_deployment_events retorna lista vacía por falta de acceso, el frontend lo manejará.
    return [DeploymentEventResponse(**_to_event_response(e)) for e in events]




# ---------------------------------------------------------------------------
# Endpoints — acciones operativas
# ---------------------------------------------------------------------------

class RollbackRequest(BaseModel):
    reason: Optional[str] = None


@router.post("/{id}/promote", response_model=DeploymentResponse, status_code=201)
async def promote_to_production(
    id: str,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """
    Promueve un deployment exitoso de staging a producción.
    Crea un nuevo deployment con el mismo service/image pero en environment=production.
    Solo funciona si el deployment origen está en SUCCESS y es de staging.
    """
    new_dep, error = await deployment_service.promote_to_production(prisma, id, user_id)
    if error:
        raise HTTPException(status_code=422, detail=error)
    return DeploymentResponse(**_to_deployment_response(new_dep))


@router.post("/{id}/rollback", response_model=DeploymentResponse)
async def rollback_deployment(
    id: str,
    body: RollbackRequest = RollbackRequest(),
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """
    Inicia un rollback manual del deployment.
    Publica un mensaje de rollback en RabbitMQ para que el worker lo procese.
    """
    dep, error = await deployment_service.trigger_rollback(
        prisma, id, reason=body.reason, user_id=user_id
    )
    if error:
        raise HTTPException(status_code=422, detail=error)
    return DeploymentResponse(**_to_deployment_response(dep))


@router.post("/{id}/cancel", response_model=DeploymentResponse)
async def cancel_deployment(
    id: str,
    prisma: Prisma = Depends(get_prisma),
    user_id: Optional[str] = Depends(get_current_user_id),
):
    """Cancela un deployment que aún no ha sido procesado."""
    dep, error = await deployment_service.cancel_deployment(prisma, id, user_id)
    if error:
        raise HTTPException(status_code=422, detail=error)
    return DeploymentResponse(**_to_deployment_response(dep))
