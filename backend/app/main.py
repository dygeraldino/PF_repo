from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from prometheus_client import make_asgi_app

from app.core.config import settings
from app.core.database import prisma_client
from app.integrations.rabbitmq import rabbitmq_client
from app.api.routers import deployments, health

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    await prisma_client.connect()
    logger.info("Prisma conectado a la base de datos")

    try:
        await rabbitmq_client.connect()
        logger.info("RabbitMQ conectado")
    except Exception as e:
        # La API arranca aunque RabbitMQ no esté disponible.
        # Los deployments quedarán en PENDING y se registrará el error en eventos.
        logger.warning(f"RabbitMQ no disponible al iniciar: {e}. La API continúa sin cola.")

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    await rabbitmq_client.disconnect()
    await prisma_client.disconnect()
    logger.info("Servicios desconectados")


app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API para la Plataforma de Autoservicio de Despliegues sobre Kubernetes",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(deployments.router)

# Expose metrics for Prometheus
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/", tags=["root"])
async def root():
    return {
        "message": "PaaS Deployments API",
        "docs": "/docs",
        "health": "/health",
    }
