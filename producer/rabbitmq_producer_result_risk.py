import json
import logging

import aio_pika
from aio_pika import Message, DeliveryMode
from config.config_properties import ConfigProperties
from config.rabbitmq_connection import RabbitMqConnection
from model.customer_risk import CustomerRisk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RabbitMqProducerResultRisk:

    def __init__(self, config_connection: RabbitMqConnection):
        properties = ConfigProperties()
        self.queue = properties.config['rabbitmq']['queue_result_risk']
        self.__config_connection = config_connection
        self.__connection = None

    async def send_message(self, customer: CustomerRisk) -> None:
        try:
            if self.__config_connection.is_closed() or self.__connection is None:
                self.__connection = await self.__config_connection.connect()

            message = Message(
                body=json.dumps(customer.to_dict()).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                content_type='application/json'
            )

            channel = await self.__connection.channel()
            exchange = await channel.declare_exchange(
                self.queue,
                aio_pika.ExchangeType.DIRECT,
                durable=True
            )

            await exchange.publish(
                routing_key=self.queue,
                message=message,
            )

            logger.info(f"message send: {customer}, queue {self.queue}")

        except Exception as e:
            logger.error(f"fail send message: {str(e)}")
            raise


    async def close(self) -> None:
        if self.__connection and not self.__connection.is_closed:
            await self.__connection.close()
            logger.info("conection closed")