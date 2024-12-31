import configparser
import json
import logging

from typing import Dict, Any
from aio_pika import Message, DeliveryMode

from config.rabbitmq_connection import RabbitMqConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RabbitMqProducerResultRisk:

    def __init__(self, rabbitmq):
        self.channel = None
        self.connection = None
        config = configparser.ConfigParser()
        config.read("config.ini")

        self.queue = config.get('rabbitmq', 'queue_result_risk')

    async def connect(self) -> None:
        try:
            self.connection = RabbitMqConnection().connection
            self.channel = await self.connection.connect()
            self.queue = await (self.channel.
                                declare_queue(queue_name=self.queue))

            logger.info("producer result risk connected")
        except Exception as e:
            logger.error(f"producer result risk error: {e}")
            raise


    async def send_message(self, data: Dict[str, Any]) -> None:
        try:
            if not self.connection or self.connection.is_closed:
                await self.connect()

            message = Message(
                body=json.dumps(data).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                content_type='application/json'
            )

            await self.channel.default_exchange.publish(
                message,
                routing_key=self.queue
            )

            logger.info(f"message send: {data}")

        except Exception as e:
            logger.error(f"fail send message: {str(e)}")
            raise


    async def close(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("conection closed")