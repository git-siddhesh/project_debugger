from contextlib import contextmanager
import logging
import traceback

@contextmanager
def conversation_logger(logger, conversation_id):
    adapter = logging.LoggerAdapter(logger, {"conversation_id": conversation_id})

    try:
        yield adapter
    except Exception:
        adapter.error(traceback.format_exc())
        raise
    finally:
        for handler in logger.handlers:
            if hasattr(handler, "flush"):
                handler.flush()
