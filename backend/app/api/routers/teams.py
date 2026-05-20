from fastapi import APIRouter, Depends, HTTPException
from prisma import Prisma
from typing import List

from app.core.database import get_prisma
from app.api.dependencies import get_current_user_id
from app.services import deployment_service

router = APIRouter(prefix="/teams", tags=["teams"])


@router.get("/services", response_model=List[str])
async def get_team_services(
    prisma: Prisma = Depends(get_prisma),
    user_id: str = Depends(get_current_user_id),
):
    """Obtiene la lista de nombres únicos de servicios desplegados por el equipo del usuario."""
    if not user_id:
        raise HTTPException(status_code=401, detail="No autorizado")
    return await deployment_service.list_team_services(prisma, user_id)


@router.get("/services/{service}/images", response_model=List[str])
async def get_team_service_images(
    service: str,
    prisma: Prisma = Depends(get_prisma),
    user_id: str = Depends(get_current_user_id),
):
    """Obtiene la lista de imágenes únicas para un servicio específico dentro del equipo del usuario."""
    if not user_id:
        raise HTTPException(status_code=401, detail="No autorizado")
    return await deployment_service.list_team_service_images(prisma, user_id, service)
