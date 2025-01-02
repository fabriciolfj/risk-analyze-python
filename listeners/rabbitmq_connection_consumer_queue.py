import logging

from typing import Optional
from aio_pika import Queue, Channel
from config.config_properties import ConfigProperties
from config.rabbitmq_connection import RabbitMqConnection
from exceptions.rabbit_exception import RabbitMqError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMqConnectionConsumer:

    def __init__(self, connection: RabbitMqConnection):
        self._channel : Optional[Channel] = None
        self._queue : Optional[Queue] = None
        self.properties = ConfigProperties()
        self.config_connection = connection

    async def connect(self):
        try:
            connection = await self.config_connection.connect()

            self._channel = await connection.channel()

            queue_name = self.properties.config['rabbitmq']['queue_name']
            self._queue = await self._channel.declare_queue(
                queue_name,
                durable=True
            )

            logger.info(f"connect rabbitmq")
            return self._queue
        except Exception as e:
            logger.error(f"error connect rabbitmq, details: {str(e)}")
            raise


    async def close(self) -> None:
        """Close channel if open"""
        if self._channel:
            try:
                await self._channel.close()
                self._channel = None
                self._queue = None
                logger.info("RabbitMQ channel closed successfully")
            except Exception as e:
                logger.error(f"Error closing channel: {str(e)}")
                raise RabbitMqError(f"Failed to close channel: {str(e)}")