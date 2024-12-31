import configparser
import json
import logging

from aio_pika import Message, DeliveryMode

from config.rabbitmq_connection import RabbitMqConnection
from exceptions.rabbit_exception import RabbitMqError
from model.customer_risk import CustomerRisk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RabbitMqProducerResultRisk:

    def __init__(self):
        self.queue = None
        self.config_connection = RabbitMqConnection()
        self._load_config()

    async def send_message(self, customer: CustomerRisk) -> None:
        try:
            if not self.config_connection.isClosed():
                await self._connect()

            if not self.config_connection.channel:
                await self._connect()

            message = Message(
                body=json.dumps(customer.to_dict()).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                content_type='application/json'
            )

            channel = self.config_connection.channel
            await channel.default_exchange.publish(
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

    def _load_config(self) -> None:
        try:
            config = configparser.ConfigParser()
            config.read("config.ini")
            self.queue = config.get('rabbitmq', 'queue_result_risk')
        except (configparser.Error, KeyError) as e:
            logger.error(f"Configuration error: {str(e)}")
            raise RabbitMqError(f"failed to load configuration: {str(e)}")

    async def _connect(self) -> None:
        try:
            await self.config_connection.connect()
            logger.info("producer result risk connected")
        except Exception as e:
            logger.error(f"producer result risk error: {e}")
            raise