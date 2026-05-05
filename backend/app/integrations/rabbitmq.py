import json
import uuid
import logging
from typing import Callable, Awaitable

import aio_pika
from aio_pika import Message, DeliveryMode
from aio_pika.abc import AbstractRobustConnection, AbstractChannel

from app.core.config import settings

logger = logging.getLogger(__name__)

# Nombres de colas
QUEUE_STAGING = "deployments.staging"
QUEUE_PRODUCTION = "deployments.production"
QUEUE_DLQ = "deployments.dlq"


def get_queue_name(environment: str) -> str:
    """Devuelve el nombre de cola según el entorno."""
    return f"deployments.{environment}"


class RabbitMQClient:
    """
    Cliente RabbitMQ compartido (singleton).
    Se conecta en el startup de la API y se desconecta al cerrar.
    También es usado por el worker para consumir mensajes.
    """

    def __init__(self):
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def connect(self):
        """Conectar a RabbitMQ y declarar todas las colas necesarias."""
        self._connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)
        await self._declare_queues()
        logger.info("RabbitMQ conectado y colas declaradas")

    async def disconnect(self):
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            logger.info("RabbitMQ desconectado")

    async def _declare_queues(self):
        """Declarar colas de forma idempotente (safe si ya existen)."""
        # DLQ sin dead-letter (destino final de mensajes fallidos)
        await self._channel.declare_queue(QUEUE_DLQ, durable=True)

        # Colas principales con DLQ como destino de dead-letters
        for queue_name in [QUEUE_STAGING, QUEUE_PRODUCTION]:
            await self._channel.declare_queue(
                queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": QUEUE_DLQ,
                }
            )

    async def publish_deployment(self, deployment_data: dict) -> str:
        """
        Publicar un mensaje de despliegue en la cola correspondiente.
        Retorna el message_id generado para guardarlo en la DB.
        """
        if not self.is_connected:
            raise RuntimeError("RabbitMQ no está conectado")

        message_id = str(uuid.uuid4())
        queue_name = get_queue_name(deployment_data["environment"])

        message = Message(
            body=json.dumps(deployment_data, default=str).encode(),
            message_id=message_id,
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=queue_name,
        )

        logger.info(
            f"Publicado deployment {deployment_data['deployment_id']} "
            f"→ {queue_name} (msg_id={message_id})"
        )
        return message_id

    async def get_channel(self) -> AbstractChannel:
        """Obtener un channel (para el worker)."""
        if not self.is_connected:
            raise RuntimeError("RabbitMQ no está conectado")
        return self._channel


# Singleton — compartido entre la API y el worker si corren en el mismo proceso.
# El worker crea su propia instancia al iniciar.
rabbitmq_client = RabbitMQClient()
