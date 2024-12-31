import configparser
import logging

from pyspark.resource import requests
from model import Payment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class BureauCustomer:


    def request(self, payment: Payment):
        config = configparser.ConfigParser()
        config.read('config.ini')

        url = config['customer']['url']
        try:
            r = requests.get(f"url/{payment.customer}/identifier/{payment.identifier}")
        except Exception as e:
            logger.error(f"fail request customer {payment.customer}, details {e}")