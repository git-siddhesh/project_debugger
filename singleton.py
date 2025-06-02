from logger_setup import setup_logger

class LoggerSingleton:
    _instance = None
    _handler = None

    @classmethod
    def get_logger(cls, config):
        if cls._instance is None:
            cls._instance, cls._handler = setup_logger(config)
        return cls._instance

    @classmethod
    def reopen(cls):
        if cls._handler:
            cls._handler.reopen()
