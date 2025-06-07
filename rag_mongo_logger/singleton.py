# singleton.py
# from logger_setup import setup_logger # Make sure this import is correct

# Assuming setup_logger is in logger_setup.py as implemented above
from .logger_setup import setup_logger 

class LoggerSingleton:
    _instance = None
    _handler = None # To store the MongoHandler instance for reopening

    @classmethod
    def get_logger(cls, config, log_console=True, logger_name="app_logger"):
        if cls._instance is None:
            # Set log_console to True/False as needed, or make it part of config
            cls._instance, mongo_handler_instance = setup_logger(config, log_console=log_console, logger_name=logger_name)
            # Store the handler if it's the MongoHandler and has a 'reopen' method
            if hasattr(mongo_handler_instance, 'reopen'):
                 cls._handler = mongo_handler_instance
        return cls._instance

    @classmethod
    def reopen(cls):
        if cls._handler and hasattr(cls._handler, 'reopen'):
            # print("LoggerSingleton attempting to reopen handler.")
            cls._handler.reopen()
        # else:
            # print("No handler available or handler does not support reopen in LoggerSingleton.")
    
    @classmethod
    def close_logger(cls): # Good practice to add a close method
        if cls._handler and hasattr(cls._handler, 'close'):
            # print("LoggerSingleton attempting to close handler.")
            cls._handler.close()
            cls._handler = None
        if cls._instance:
            # Remove handlers from logger to prevent further use after close
            for handler in list(cls._instance.handlers): # Iterate over a copy
                cls._instance.removeHandler(handler)
            cls._instance = None
        # print("LoggerSingleton resources released.")