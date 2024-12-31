import configparser

from config.rabbitmq_properties import RabbitMqProperties


class RabbitMqConfig:

    def __init__(self):
        properties = RabbitMqProperties()
        self.url = properties.url

        config = configparser.ConfigParser()
        config.read('config.ini')

        self.queue = config['rabbitmq']['queue_name']