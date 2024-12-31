import configparser

from config.rabbitmq_properties import RabbitMqProperties


class RabbitMqConfig:

    def __init__(self):
        properties = RabbitMqProperties()
        self.url = properties.url