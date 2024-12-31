
from typing import Optional
from aio_pika import connect_robust, Queue
from config.rabbitmq_config import RabbitMqConfig

import logging

from config.rabbitmq_connection import RabbitMqConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMqConnectionQueue:

    def __init__(self):
        self.channel = None
        self.queue : Optional[Queue] = None

    async def connect(self):
        try:
            rabbitmq_config = RabbitMqConfig()

            self.channel = await RabbitMqConnection().connect()

            self.queue = await self.channel.declare_queue(
                rabbitmq_config.queue,
                durable=True
            )

            logger.info(f"connect rabbitmq {rabbitmq_config.url}")
            return self.queue
        except Exception as e:
            logger.error(f"error connect rabbitmq, details: {str(e)}")
            raise


    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("closing rabbitmq connection")