
from aio_pika import connect_robust, Queue
from config.rabbitmq_config import RabbitMqConfig

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RabbitMqConnection:

    def __init__(self):
        self.connection = None

    async def connect(self):
        try:
            rabbitmq_config = RabbitMqConfig()

            self.connection = await connect_robust(rabbitmq_config.url)
            return self.connection
        except Exception as e:
            logger.error(f"error connect rabbitmq, details: {str(e)}")
            raise


    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("closing rabbitmq connection")