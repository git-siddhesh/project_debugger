
# logger_setup.py
import logging
import json
from datetime import datetime, timezone
from handlers import MongoHandler # Assuming mongo_handler.py contains your MongoHandler class
from handlers import MongoHandler # Assuming mongo_handler.py contains your MongoHandler class

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
        }

        # Add fields from LoggerAdapter's 'extra'
        if hasattr(record, 'conversation_id'):
            log_record['conversation_id'] = record.conversation_id
        if hasattr(record, 'bot_id'):
            log_record['bot_id'] = record.bot_id
        if hasattr(record, 'user_id'):
            log_record['user_id'] = record.user_id
        if hasattr(record, 'training_id'):
            log_record['training_id'] = record.training_id
        if hasattr(record, 'extra'):
            # If there are any extra fields added by the LoggerAdapter
            for key, value in record.extra.items():
                if key not in log_record:
                    log_record[key] = value
        
        # Include any other 'extra' fields passed to the logger
        # These are fields that might have been added directly to the log call's extra dictionary
        # and are not one of the standard fields or the specific ones above.
        standard_attrs = {'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
                          'funcName', 'levelname', 'levelno', 'lineno', 'module', 'msecs',
                          'message', 'msg', 'name', 'pathname', 'process', 'processName',
                          'relativeCreated', 'stack_info', 'thread', 'threadName',
                          'conversation_id', 'bot_id', 'user_id'} # Already handled
        
        # Add other custom fields from record if they exist and are not standard
        for key, value in record.__dict__.items():
            if key not in standard_attrs and key not in log_record:
                log_record[key] = value

        if record.exc_info:
            log_record['exc_info'] = self.formatException(record.exc_info)
            # For sensitive environments, you might want to be careful about logging full tracebacks.
            # Consider summarizing or redacting if necessary.
            # log_record['traceback'] = traceback.format_exc() # Or using formatException

        return json.dumps(log_record, default=str)

def setup_logger(config, log_console=True, logger_name="app_logger"):
    """
    Configures and returns a logger instance.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG) # Set to desired level, e.g., logging.INFO for production
    logger.handlers = [] # Clear existing handlers

    # Configure MongoHandler
    # Batch size and fallback file can also be part of the config
    mongo_handler = MongoHandler(
        config=config,
        batch_size=config.get("log_batch_size", 10),
        fallback_file=config.get("log_fallback_file", "log_fallback.txt")
    )
    json_formatter = JsonFormatter()
    mongo_handler.setFormatter(json_formatter)
    logger.addHandler(mongo_handler)

    if log_console:
        console_handler = logging.StreamHandler()
        # You might want a simpler formatter for the console
        console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(module)s:%(lineno)d)')
        if hasattr(json_formatter, 'formatTime'): # For consistency if JsonFormatter customizes time
            console_formatter.formatTime = json_formatter.formatTime
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
    logger.propagate = False # Prevent log duplication if root logger also has handlers

    return logger, mongo_handler