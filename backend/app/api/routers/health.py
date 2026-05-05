from fastapi import APIRouter, Depends
from pydantic import BaseModel
from prisma import Prisma
from app.core.database import get_prisma

router = APIRouter()

class HealthResponse(BaseModel):
    status: str
    database: str
    message: str

@router.get("/health", response_model=HealthResponse)
async def health_check(prisma: Prisma = Depends(get_prisma)):
    """
    Verifica que la API esté viva y conectada a la base de datos.
    """
    try:
        # Ejecutar query simple para verificar conexión
        await prisma.query_raw("SELECT 1")
        return HealthResponse(
            status="ok",
            database="connected",
            message="API and Database are running"
        )
    except Exception as e:
        return HealthResponse(
            status="error",
            database="disconnected",
            message=f"Database connection failed: {str(e)}"
        )
