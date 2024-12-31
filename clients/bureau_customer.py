import configparser
import logging
import requests

from config.config_properties import ConfigProperties
from model.customer_risk import CustomerRisk
from model.payment import Payment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class BureauCustomer:


    def request(self, payment: Payment):
        properties = ConfigProperties()

        url = properties.config['customer']['url']
        try:
            result = requests.get(f"{url}/{payment.customer}/card/{payment.identifier}")
            return CustomerRisk(**result.json())
        except Exception as e:
            logger.error(f"fail request customer {payment.customer}, details {e}")
            raise e