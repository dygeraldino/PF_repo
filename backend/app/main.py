from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.database import prisma_client
from app.api.routers import deployments, health

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Conectar el cliente Prisma al iniciar
    await prisma_client.connect()
    yield
    # Desconectar al cerrar
    await prisma_client.disconnect()

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API para la Plataforma de Autoservicio para Despliegues Controlados",
    version="0.1.0",
    lifespan=lifespan
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(health.router)
app.include_router(deployments.router)

@app.get("/")
async def root():
    return {"message": "Bienvenido a la API de Despliegues. Visita /docs para la documentación."}
