from config.config_properties import ConfigProperties


class RabbitMqProperties:

    def __init__(self):
        self.properties = ConfigProperties()

        self.host = self.properties.config['rabbitmq']['host']
        self.port = self.properties.config['rabbitmq']['port']
        self.user = self.properties.config['rabbitmq']['user']
        self.password = self.properties.config['rabbitmq']['password']
        self.url = f"amqp://{self.user}:{self.password}@{self.host}:{self.port}"



