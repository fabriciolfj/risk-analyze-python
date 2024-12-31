import asyncio
import logging
import platform
import signal
import sys

from typing import Optional
from contextlib import AsyncExitStack
from datetime import datetime
from clients.bureau_customer import BureauCustomer
from config.rabbitmq_connection_queue import RabbitMqConnectionQueue
from listeners.listener_risk_queue import RiskListener
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
        self.connection: Optional[RabbitMqConnectionQueue] = None
        self.consumer: Optional[RiskListener] = None
        self.exit_stack = AsyncExitStack()
        self._shutdown_event = asyncio.Event()

    async def startup(self) -> None:
        try:
            self.connection = RabbitMqConnectionQueue()
            bureau = BureauCustomer()
            customer_risk_service = CustomerRiskService(bureau)
            self.consumer = RiskListener(self.connection, customer_risk_service)

            await self.consumer.start()
            logger.info("services started successfully")

        except Exception as e:
            logger.error("error started:", exc_info=e)
            await self.shutdown()
            raise

    async def shutdown(self) -> None:
        logger.info("init off gracefully...")

        if self.consumer:
            await self.consumer.stop()

        if self.connection:
            await self.connection.close()

        await self.exit_stack.aclose()
        logger.info("app finished successfully")

    def handle_signals(self) -> None:
        if platform.system() == 'Windows':
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            except:
                logger.warning("not possibility WindowsSelectorEventLoopPolicy")
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
        self._shutdown_event.set()

    async def run(self) -> None:
        try:
            self.handle_signals()
            await self.startup()

            logger.info("started consumer risk")
            await self.consumer.start()

            await self._shutdown_event.wait()

        except Exception as e:
            logger.error("error execution", exc_info=e)
            raise
        finally:
            await self.shutdown()

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