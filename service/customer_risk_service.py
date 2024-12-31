import logging


from clients.bureau_customer import BureauCustomer
from model.payment import Payment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class CustomerRiskService:
    def __init__(self, bureau: BureauCustomer):
        self.bureau = bureau

    def analyze(self, payment: Payment):
        result = self.bureau.request(payment)

        logger.info(f"result analyze risk ${result}")

