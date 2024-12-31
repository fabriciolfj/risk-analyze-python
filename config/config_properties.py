import configparser


class ConfigProperties:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.config = config