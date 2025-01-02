import asyncio
import logging
import platform
import signal
import sys

from typing import Optional
from contextlib import AsyncExitStack
from datetime import datetime
from clients.bureau_customer import BureauCustomer
from config.rabbitmq_connection import RabbitMqConnection
from listeners.rabbitmq_connection_consumer_queue import RabbitMqConnectionConsumer
from listeners.listener_risk_queue import RiskListener
from producer.rabbitmq_producer_result_risk import RabbitMqProducerResultRisk
from service.customer_risk_service import CustomerRiskService


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'risk_service_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )

    logging.getLogger('aio_pika').setLevel(logging.WARNING)
    logging.getLogger('pika').setLevel(logging.WARNING)

    return logging.getLogger(__name__)


logger = setup_logging()
class RiskApp:
    def __init__(self):
        self.__connection_consumer: Optional[RabbitMqConnectionConsumer] = None
        self.__listener:            Optional[RiskListener] = None
        self.__rabbit_connection:   Optional[RabbitMqConnection] = None
        self.__producer_connection: Optional[RabbitMqProducerResultRisk] = None
        self.__exit_stack:          Optional[AsyncExitStack] = None
        self.__bureau_customer:     Optional[BureauCustomer] = None
        self.__service:             Optional[CustomerRiskService] = None
        self.__shutdown_event = asyncio.Event()

    async def startup(self) -> None:
        try:
            self.__load_configs()

            await self.__listener.start()

            logger.info("services started successfully")

        except Exception as e:
            logger.error("error started:", exc_info=e)
            await self.shutdown()
            raise

    async def shutdown(self) -> None:
        logger.info("init off gracefully...")

        if self.__listener:
            await self.__listener.stop()

        if self.__connection_consumer:
            await self.__connection_consumer.close()

        await self.__exit_stack.aclose()
        logger.info("app finished successfully")

    def handle_signals(self) -> None:
        if platform.system() == 'Windows':
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            except Exception as e:
                logger.warning(f"not possibility WindowsSelectorEventLoopPolicy, details: ${e}", exc_info=e)
        else:
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    asyncio.get_event_loop().add_signal_handler(
                        sig,
                        lambda s=sig: asyncio.create_task(self.handle_shutdown(s))
                    )
                except NotImplementedError:
                    logger.warning(f"add_signal_handler not supported {sig.name}")

    async def handle_shutdown(self, sig: signal.Signals) -> None:
        logger.info(f"receive signal de shutdown: {sig.name}")
        self.__shutdown_event.set()

    async def run(self) -> None:
        try:
            self.handle_signals()
            await self.startup()

            logger.info("started consumer risk")
            await self.__listener.start()

            await self.__shutdown_event.wait()

        except Exception as e:
            logger.error("error execution", exc_info=e)
            raise
        finally:
            await self.shutdown()

    def __load_configs(self):
        self.__rabbit_connection   = RabbitMqConnection()
        self.__connection_consumer = RabbitMqConnectionConsumer(self.__rabbit_connection)
        self.__producer_connection = RabbitMqProducerResultRisk(self.__rabbit_connection)
        self.__bureau_customer     = BureauCustomer()
        self.__service             = CustomerRiskService(self.__bureau_customer, self.__producer_connection)
        self.__listener            = RiskListener(self.__connection_consumer, self.__service)
        self.__exit_stack          = Optional[AsyncExitStack]

async def main() -> None:
    app = RiskApp()

    try:
        await app.run()
    except Exception as e:
        logger.error("error fatal:", exc_info=e)
        sys.exit(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("app exited")
        sys.exit(0)