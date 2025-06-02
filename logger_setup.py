import logging
import json
from handlers import MongoHandler

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "conversation_id": getattr(record, "conversation_id", None),
            "extra": getattr(record, "extra", {}),
        }, default=str)

def setup_logger(config):
    logger = logging.getLogger("async_logger")
    logger.setLevel(logging.DEBUG)  # Global min level

    handler = MongoHandler(config=config, batch_size=10)
    handler.setFormatter(JSONFormatter())

    logger.addHandler(handler)
    return logger, handler  # Return both for `reopen()`
