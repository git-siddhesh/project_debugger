# context_logging.py
import logging
import traceback
from contextlib import contextmanager

class ConversationLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Ensure 'extra' dict exists
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        
        # Add conversation-specific context from adapter's 'extra' to log record's 'extra'
        # These will be picked up by JsonFormatter if they are attributes of the record.
        # JsonFormatter needs to be aware of these field names.
        # The LogRecord object will have these as attributes after process()
        kwargs["extra"]["conversation_id"] = self.extra["conversation_id"]
        kwargs["extra"]["bot_id"] = self.extra["bot_id"]
        kwargs["extra"]["user_id"] = self.extra["user_id"]
        
        return msg, kwargs

@contextmanager
def conversation_logger(logger, conversation_id:str, bot_id:str, user_id:str='admin'):
    adapter = ConversationLoggerAdapter(logger=logger, extra={
        "conversation_id": conversation_id,
        "bot_id": bot_id,
        "user_id": user_id
    })
    try:
        yield adapter
    except Exception:
        # Log the exception using the adapter itself so it gets conversation context
        adapter.error(f"Unhandled exception in conversation context: {traceback.format_exc()}")
        raise
    # No explicit flush here, rely on MongoHandler's batching or close()
    # The original code flushed all handlers of the main logger.
    # This might be too aggressive if there are other handlers.
    # MongoHandler's batching or eventual close should handle flushing.
    # If immediate flush is needed for a specific context, it could be done on the handler.
    # For example:
    # finally:
    #     for handler in logger.handlers:
    #         if isinstance(handler, MongoHandler): # Target specific handler
    #             handler.flush()


class RagTrainingLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Ensure 'extra' dict exists
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        
        kwargs["extra"]["training_id"] = self.extra["training_id"]
        kwargs["extra"]["bot_id"] = self.extra["bot_id"]
        
        return msg, kwargs

@contextmanager
def training_logger(logger, training_id, bot_id):
    adapter = RagTrainingLoggerAdapter(
        logger=logger, 
        extra={
                "training_id": training_id,
                "bot_id": bot_id,
            })
    try:
        yield adapter
    except Exception:
        # Log the exception using the adapter itself so it gets conversation context
        adapter.error(f"Unhandled exception in conversation context: {traceback.format_exc()}")
        raise
    # No explicit flush here, rely on MongoHandler's batching or close()
    # The original code flushed all handlers of the main logger.
    # This might be too aggressive if there are other handlers.
    # MongoHandler's batching or eventual close should handle flushing.
    # If immediate flush is needed for a specific context, it could be done on the handler.
    # For example:
    # finally:
    #     for handler in logger.handlers:
    #         if isinstance(handler, MongoHandler): # Target specific handler
    #             handler.flush()
