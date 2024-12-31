import configparser


class RabbitMqProperties:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        self.host = config['rabbitmq']['host']
        self.port = config['rabbitmq']['port']
        self.user = config['rabbitmq']['user']
        self.password = config['rabbitmq']['password']
        self.url = f"amqp://{self.user}:{self.password}@{self.host}:{self.port}"



