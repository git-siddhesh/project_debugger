import atexit
import logging
import queue
import threading
import traceback
from datetime import datetime
import sys
import json
from pymongo import UpdateOne
from collections import defaultdict
from pymongo import MongoClient
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, JSON
from sqlalchemy.exc import SQLAlchemyError

from logging.handlers import RotatingFileHandler


class AsyncBufferedLogger:
    def __init__(self, db_type='mongo', config=None, fallback_file='log_fallback.txt',
                 min_log_level=logging.INFO, batch_size=10):
        self.db_type = db_type
        self.config = config or {}
        self.fallback_file = fallback_file
        self.min_log_level = min_log_level
        self.batch_size = batch_size

        self.queue = queue.Queue()
        self.buffer = []
        self.lock = threading.Lock()
        self.running = True

        self.db_ready = False
        self._connect_to_db()

        self._init_fallback_logger()

        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

        # Global exception and shutdown handling
        sys.excepthook = self._handle_exception
        atexit.register(self.flush)

    def _connect_to_db(self):
        try:
            if self.db_type == 'mongo':
                self.client = MongoClient(self.config.get("uri"))
                self.db = self.client[self.config.get("db", "logs")]
                self.collection = self.db[self.config.get("collection", "conversations")]
                self.db_ready = True

            elif self.db_type == 'postgres':
                self.engine = create_engine(self.config.get("uri"))
                self.metadata = MetaData()
                self.logs_table = Table(
                    self.config.get("table", "logs"), self.metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('timestamp', String),
                    Column('level', String),
                    Column('message', String),
                    Column('extra', JSON),
                    Column('conversation_id', String),
                )
                self.metadata.create_all(self.engine)
                self.db_ready = True

        except Exception as e:
            self._fallback(f"[DB Connection Error] {e}")
            self.db_ready = False

    def reopen(self):
        """Attempt to reconnect to the database."""
        try:
            self._connect_to_db()
            if self.db_ready:
                self._fallback("[Reopen] Successfully reconnected to the database.")
            else:
                self._fallback("[Reopen] Failed to reconnect.")
        except Exception as e:
            self._fallback(f"[Reopen Exception] {str(e)}")


    def log(self, message, level=logging.INFO, extra=None, conversation_id=None):
        if level < self.min_log_level:
            return

        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": logging.getLevelName(level),
            "message": message,
            "extra": extra or {},
            "conversation_id": conversation_id,
        }
        self.queue.put(entry)

    def debug(self, msg, **kwargs):
        self.log(msg, level=logging.DEBUG, **kwargs)

    def info(self, msg, **kwargs):
        self.log(msg, level=logging.INFO, **kwargs)

    def warning(self, msg, **kwargs):
        self.log(msg, level=logging.WARNING, **kwargs)

    def error(self, msg, **kwargs):
        self.log(msg, level=logging.ERROR, **kwargs)

    def critical(self, msg, **kwargs):
        self.log(msg, level=logging.CRITICAL, **kwargs)


    def _worker(self):
        while self.running or not self.queue.empty():
            try:
                entry = self.queue.get(timeout=1)
            except queue.Empty:
                continue
            with self.lock:
                self.buffer.append(entry)
                if len(self.buffer) >= self.batch_size:
                    self._flush_to_db()


    def _flush_to_db(self):
        if not self.buffer:
            return
        try:
            if not self.db_ready:
                self._connect_to_db()
            
            if self.db_type == 'mongo' and self.db_ready:
                grouped = defaultdict(list)
                for entry in self.buffer:
                    cid = entry.pop("conversation_id", "unknown")
                    grouped[cid].append(entry)

                ops = [
                    UpdateOne(
                        {"conversation_id": cid},
                        {"$push": {"logs": {"$each": logs}}, "$setOnInsert": {"created_at": datetime.utcnow()}},
                        upsert=True,
                    )
                    for cid, logs in grouped.items()
                ]
                self.collection.bulk_write(ops)

            elif self.db_type == 'postgres' and self.db_ready:
                with self.engine.connect() as conn:
                    conn.execute(self.logs_table.insert(), self.buffer)
            else:
                raise Exception("DB not ready")

        except Exception as e:
            self._fallback(f"[Flush Error] {str(e)}")
            self._fallback(self.buffer)

        self.buffer.clear()


    def flush(self):
        self.running = False
        if self.thread.is_alive():
            self.thread.join(timeout=2)  # Wait for worker to finish
        with self.lock:
            self._flush_to_db()


    def _init_fallback_logger(self):
        self.fallback_logger = logging.getLogger("fallback_logger")
        handler = RotatingFileHandler(self.fallback_file, maxBytes=5_000_000, backupCount=3)
        self.fallback_logger.addHandler(handler)
        self.fallback_logger.setLevel(logging.INFO)
        self.fallback_logger.propagate = False

        
    def _fallback(self, content):
        if hasattr(self, 'fallback_logger'):
            if isinstance(content, list):
                for entry in content:
                    self.fallback_logger.info(json.dumps(entry, default=str))
            else:
                self.fallback_logger.error(str(content))
        else:
            with open(self.fallback_file, "a") as f:
                if isinstance(content, list):
                    for entry in content:
                        f.write(json.dumps(entry, default=str) + "\n")
                else:
                    f.write(str(content) + "\n")

    def _handle_exception(self, exc_type, exc_value, exc_traceback):
        self.log(f"Uncaught Exception: {''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))}",
                 level=logging.ERROR)
        self.flush()
        sys.__excepthook__(exc_type, exc_value, exc_traceback)


config = {
    # "uri": "postgresql://user:password@localhost:5432/logsdb",
    # "table": "logs",
    # for Mongo: {
    "uri": "mongodb://localhost:27017/", 
    "db": "logs", 
    "collection": "conversations",
}
# logger = AsyncBufferedLogger(db_type='postgres', config=config)

class LoggerSingleton:
    _instance = None

    @classmethod
    def get_logger(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = AsyncBufferedLogger(*args, **kwargs)
        return cls._instance


from contextlib import contextmanager
import traceback
import types

@contextmanager
def conversation_logger(logger: AsyncBufferedLogger, conversation_id=None):
    # Create a temporary wrapper object
    wrapper = types.SimpleNamespace()

    def make_wrapper(method):
        return lambda msg, **kwargs: method(msg, conversation_id=conversation_id, **kwargs)

    # Wrap methods to automatically include conversation_id
    wrapper.debug = make_wrapper(logger.debug)
    wrapper.info = make_wrapper(logger.info)
    wrapper.warning = make_wrapper(logger.warning)
    wrapper.error = make_wrapper(logger.error)
    wrapper.critical = make_wrapper(logger.critical)
    wrapper.log = lambda msg, level=logging.INFO, **kwargs: logger.log(msg, level=level, conversation_id=conversation_id, **kwargs)

    try:
        yield wrapper
    except Exception:
        logger.error(traceback.format_exc(), conversation_id=conversation_id)
        raise
    finally:
        logger.flush()



logger: AsyncBufferedLogger = LoggerSingleton.get_logger(db_type='mongo', config=config)

def run_conversation():
    with conversation_logger(logger, conversation_id="conv-42") as log:
        log.info("Conversation started")
        log.debug("Retrieving relevant documents")
        log.info("Generating answer")
        # raise RuntimeError("Boom!") → would be logged
        log.info("Conversation finished")



@app.post("/logger/reopen")
def trigger_reconnect():
    logger.reopen()
    return {"status": "Reconnection attempted"}
