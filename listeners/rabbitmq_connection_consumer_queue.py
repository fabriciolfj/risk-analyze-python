import logging

from typing import Optional
from aio_pika import Queue
from config.config_properties import ConfigProperties
from config.rabbitmq_connection import RabbitMqConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMqConnectionConsumer:

    def __init__(self):
        self.channel = None
        self.queue : Optional[Queue] = None
        self.properties = ConfigProperties()
        self.config_connection = RabbitMqConnection()

    async def connect(self):
        try:
            await self.config_connection.connect()

            self.channel = self.config_connection.channel

            self.queue = await self.channel.declare_queue(
                self.properties.config['rabbitmq']['queue_name'],
                durable=True
            )

            logger.info(f"connect rabbitmq")
            return self.queue
        except Exception as e:
            logger.error(f"error connect rabbitmq, details: {str(e)}")
            raise


    async def close(self):
        if self.channel:
            await self.channel.close()
            logger.info("closing rabbitmq connection")