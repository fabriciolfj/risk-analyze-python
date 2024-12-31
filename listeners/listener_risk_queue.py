import asyncio
import json
import logging

from typing import Optional
from aio_pika import IncomingMessage, Queue
from listeners.rabbitmq_connection_consumer_queue import RabbitMqConnectionConsumer
from model.payment import Payment
from service.customer_risk_service import CustomerRiskService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RiskListener:
    def __init__(self, connection: RabbitMqConnectionConsumer, customer_risk_service: CustomerRiskService):
        self.queue: Optional[Queue] = None
        self.connection = connection
        self.customer_risk_service = customer_risk_service

    async def start(self):
        try:
            self.queue = await self.connection.connect()
            await self.queue.consume(self.process_message)

            logger.info("consumidor iniciado, aguardando mensagens...")
            await asyncio.Future()
        except Exception as e:
            logger.error(f"Erro no consumidor: {str(e)}")
            raise
        finally:
            await self.connection.close()


    async def process_message(self, message: IncomingMessage):
        async with message.process():
            try:
                data = json.loads(message.body)

                logger.info(f"received message: {data}")

                await self.customer_risk_service.analyze(Payment(**data))
            except json.JSONDecodeError:
                logger.error(f"failed to decode message: {data}")
            except Exception:
                logger.error(f"failed receive message: {data}")
                await message.reject(requeue=True)


    async def stop(self):
        try:
            logger.info("init stop consumer")

            if self.queue and self.queue.channel:
                await self.queue.channel.close()

            if self.connection:
                await self.connection.close()

            self.queue = None
            logger.info("consumer stoped successfully")

        except Exception as e:
            logger.error(f"error stop consumer: {str(e)}")
            raise
