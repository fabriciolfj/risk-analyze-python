
from aio_pika import connect_robust, Queue
from config.rabbitmq_config import RabbitMqConfig

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RabbitMqConnection:

    def __init__(self):
        self._connection = None
        self._config = RabbitMqConfig()

    async def connect(self):
        try:
            if not self._connection:
                self._connection = await connect_robust(self._config.url)

            return self._connection
        except Exception as e:
            logger.error(f"error connect rabbitmq, details: {str(e)}")
            raise

    async def close(self):
        if self._connection:
            await self._connection.close()
            logger.info("closing rabbitmq connection")

    def is_closed(self):
        return self._connection is None or self._connection.is_closed