import logging
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import json
import traceback

class MongoHandler(logging.Handler):
    def __init__(self, config, batch_size=10, fallback_file="log_fallback.txt"):
        super().__init__()
        self.config = config
        self.batch_size = batch_size
        self.fallback_file = fallback_file
        self.buffer = []
        self.db_ready = False
        self._connect()

    def _connect(self):
        try:
            self.client = MongoClient(self.config["uri"])
            self.db = self.client[self.config.get("db", "logs")]
            self.collection = self.db[self.config.get("collection", "conversations")]
            self.db_ready = True
        except Exception as e:
            self.db_ready = False
            self._fallback(f"[MongoHandler Connect Error] {str(e)}")

    def emit(self, record):
        try:
            log_entry = self.format(record)
            data = json.loads(log_entry)
            self.buffer.append(data)
            if len(self.buffer) >= self.batch_size:
                self.flush()
        except Exception as e:
            self._fallback(f"[MongoHandler Emit Error] {str(e)}")
            self._fallback(self.buffer)

    def flush(self):
        if not self.db_ready:
            self._connect()
        if not self.db_ready or not self.buffer:
            return

        try:
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
        except Exception as e:
            self._fallback(f"[MongoHandler Flush Error] {str(e)}")
            self._fallback(self.buffer)
        finally:
            self.buffer.clear()

    def reopen(self):
        self._connect()

    def _fallback(self, content):
        try:
            with open(self.fallback_file, "a") as f:
                if isinstance(content, list):
                    for entry in content:
                        f.write(json.dumps(entry, default=str) + "\n")
                else:
                    f.write(str(content) + "\n")
        except Exception:
            pass
