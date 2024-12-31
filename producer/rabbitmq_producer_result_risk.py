import configparser
import json
import logging

from aio_pika import Message, DeliveryMode
from pip._internal.configuration import Configuration

from config.config_properties import ConfigProperties
from config.rabbitmq_connection import RabbitMqConnection
from exceptions.rabbit_exception import RabbitMqError
from model.customer_risk import CustomerRisk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RabbitMqProducerResultRisk:

    def __init__(self):
        properties = ConfigProperties()
        self.queue = properties.config['rabbitmq']['queue_result_risk']
        self.config_connection = RabbitMqConnection()
        self.connection = None

    async def send_message(self, customer: CustomerRisk) -> None:
        try:
            if self.config_connection.is_closed():
                self.connection = await self.config_connection.connect()

            message = Message(
                body=json.dumps(customer.to_dict()).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                content_type='application/json'
            )

            channel = await self.connection.channel()
            channel.default_exchange.publish(
                message,
                routing_key=self.queue
            )

            logger.info(f"message send: {customer}")

        except Exception as e:
            logger.error(f"fail send message: {str(e)}")
            raise


    async def close(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("conection closed")