
from aio_pika import connect_robust, Queue
from config.rabbitmq_config import RabbitMqConfig

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RabbitMqConnection:

    def __init__(self):
        self.channel = None
        self._connection = None
        self._config = RabbitMqConfig()

    async def connect(self):
        try:
            self._connection = await connect_robust(self._config.url)

            self.channel = await self._connection.channel()
        except Exception as e:
            logger.error(f"error connect rabbitmq, details: {str(e)}")
            raise

    async def close(self):
        if self._connection:
            await self._connection.close()
            logger.info("closing rabbitmq connection")

    def isClosed(self):
        return self._connection