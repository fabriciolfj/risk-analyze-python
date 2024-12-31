import asyncio
import json
import logging
from typing import Optional

from aio_pika import IncomingMessage, Queue

from config.rabbitmq_connection_risk_queue import RabbitMqConnectionRiskQueue
from model.Payment import Payment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RiskListener:
    def __init__(self, connection: RabbitMqConnectionRiskQueue):
        self.queue: Optional[Queue] = None
        self.connection = connection


    async def process_message(self, message: IncomingMessage):
        async with message.process():
            try:
                data = json.loads(message.body)
                logger.info(f"received message: {data}")
                return Payment(**data)
            except json.JSONDecodeError:
                logger.error(f"failed to decode message: {data}")
            except Exception as e:
                logger.error(f"failed receive message: {data}")
                await message.reject(requeue=True)

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
