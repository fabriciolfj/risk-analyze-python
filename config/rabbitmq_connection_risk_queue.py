import configparser
from typing import Optional

from aio_pika import connect_robust, Queue
import logging

from config.rabbitmq_config import RabbitMqConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMqConnectionRiskQueue:

    def __init__(self):
        self.channel = None
        self.connection = None
        self.queue : Optional[Queue] = None

    async def connect(self):
        try:
            rabbitmq_config = RabbitMqConfig()
            config = configparser.ConfigParser()
            config.read('config.ini')

            self.connection = await connect_robust(rabbitmq_config.url)
            self.channel = await self.connection.channel()

            self.queue = await self.channel.declare_queue(
                config['rabbitmq']['queue_name'],
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