import asyncio

from config.rabbitmq_connection_risk_queue import RabbitMqConnectionRiskQueue
from listeners.listener_risk_queue import RiskListener
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
async def main():
    connection = RabbitMqConnectionRiskQueue()
    consumer = RiskListener(connection)

    try:
        await consumer.start()
    except Exception as e:
        logger.info(f"encerrando consumidor risk")
        await connection.close()

if __name__ == '__main__':
    asyncio.run(main())