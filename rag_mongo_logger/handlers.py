# mongo_handler.py
import logging
from collections import defaultdict
from pymongo import MongoClient, UpdateOne, InsertOne
import datetime
import json
import traceback
from typing import Dict, Any

class MongoHandler(logging.Handler):
    def __init__(self, config: Dict[str, Any], batch_size=10, fallback_file="log_fallback.txt"):
        super().__init__()
        self.config = config
        self.mode = self.config.get("logger_mode", "chat")  # Get mode from config ["chat", "training"]
        self.env = self.config.get("env", "dev")  # Default to 'dev' if not specified
        self.debug = self.config.get("debug", True)  # Debug mode for more verbose logging
        self.batch_size = batch_size
        self.fallback_file = fallback_file
        self.buffer = []
        self.db_ready = False
        self.client = None
        self.db = None
        self._connect()

    def _get_collection_name(self, bot_id):
        env = self.config.get("env", "dev")
        # Collection name format: logs_{env}_{bot_id}
        # create different collection for training and chat
        return f"{bot_id}_{env}_{self.mode}"  # Use mode to differentiate collections

    def _connect(self):
        try:
            if self.client:
                try:
                    self.client.close()
                except Exception:
                    pass
            self.client = MongoClient(self.config["uri"])
            self.client.admin.command('ping')
            self.db = self.client[self.config.get("db", "logs")]
            self.db_ready = True
        except Exception as e:
            self.db_ready = False
            self._fallback(f"[MongoHandler Connect Error] {str(e)} - URI: {self.config.get('uri')}")

    def emit(self, record):
        try:
            # self.format(record) uses the handler's formatter (JsonFormatter)
            log_json_string = self.format(record)
            log_entry_dict = json.loads(log_json_string)

            self.buffer.append(log_entry_dict)
            if len(self.buffer) >= self.batch_size:
                self.flush()
        except json.JSONDecodeError as e:
            self._fallback(f"[MongoHandler Emit Error] JSONDecodeError: {str(e)} - Record data: {str(record.__dict__)}")
        except Exception as e:
            record_info = str(vars(record)) if 'record' in locals() and record else 'N/A'
            self._fallback(f"[MongoHandler Emit Error] {str(e)} - Traceback: {traceback.format_exc()} - Record: {record_info}")
            if self.buffer:
                 self._fallback(["[UNPROCESSED BUFFER DUE TO EMIT ERROR]"] + self.buffer)

    def flush(self):
        if not self.db_ready:
            self._connect()

        if not self.db_ready or not self.buffer:
            if not self.buffer:
                pass
            elif not self.db_ready:
                self._fallback(["[UNFLUSHED BUFFER DUE TO DB NOT READY]"] + self.buffer)
                self.buffer.clear()
            return

        current_buffer = list(self.buffer)
        self.buffer.clear()

        try:
            if self.mode == "chat":
                self._flush_chat_logs(current_buffer)
            elif self.mode == "training":
                self._flush_training_logs(current_buffer)
            else:
                self._fallback(f"[MongoHandler Flush Warning] Unknown logger mode '{self.mode}'. Falling back training log data: {current_buffer}")
                # Default to simple insert for unknown modes or handle as error
                self._flush_generic_logs_as_separate_docs(current_buffer, "unknown_mode_logs")


        except Exception as e:
            self._fallback(f"[MongoHandler Flush Error ({self.mode} mode)] {str(e)} - Traceback: {traceback.format_exc()}")
            self._fallback([f"[UNFLUSHED BUFFER DUE TO FLUSH ERROR ({self.mode} mode)]"] + current_buffer)

    def _flush_chat_logs(self, buffer_to_flush):
        # Group logs: each entry in current_buffer is a dict from JsonFormatter
        # It should contain 'bot_id', 'conversation_id', 'user_id' at its top level.
            
        # We will create documents where top-level fields are conv_id, bot_id, user_id
        # and 'logs' is an array of individual log messages (the rest of the entry).
        # Group logs by (bot_id, conversation_id) for chat mode
        grouped_for_update = defaultdict(lambda: {"logs_to_push": [], "user_id": None, "bot_id": None, "conv_id": None})

        for entry_dict in buffer_to_flush:
            conversation_id = entry_dict.get("conversation_id", "unknown_cid")
            bot_id = entry_dict.get("bot_id", "unknown_bid")
            user_id = entry_dict.get("user_id", "unknown_uid") # This will be for $setOnInsert
            
            key = (bot_id, conversation_id) # Group by these for UpdateOne logic
            
            grouped_for_update[key]["logs_to_push"].append(entry_dict)
            if grouped_for_update[key]["user_id"] is None:
                grouped_for_update[key]["user_id"] = user_id
            if grouped_for_update[key]["bot_id"] is None:
                grouped_for_update[key]["bot_id"] = bot_id
            if grouped_for_update[key]["conv_id"] is None:
                grouped_for_update[key]["conv_id"] = conversation_id
        
        operations_by_collection = defaultdict(list)
        for (op_bot_id, op_conv_id), data in grouped_for_update.items():
            if op_bot_id == "unknown_bid" or op_conv_id == "unknown_cid":
                self._fallback(f"[MongoHandler Chat Flush Warning] Missing bot_id or conversation_id: {data['logs_to_push']}")
                continue

            collection_name = self._get_collection_name(op_bot_id)
            
            # Create the MongoDB update operation
            op = UpdateOne(
                {"conversation_id": op_conv_id, "bot_id": op_bot_id}, # Filter to find the document
                {
                    "$push": {"logs": {"$each": data["logs_to_push"]}},
                    "$setOnInsert": {
                        "conversation_id": op_conv_id, # Explicitly set on insert
                        "bot_id": op_bot_id,           # Explicitly set on insert
                        "user_id": data["user_id"],
                        "created_at": datetime.datetime.now(datetime.UTC)

                    },
                    "$set": {"updated_at": datetime.datetime.now(datetime.UTC)}
                },
                upsert=True  # Create the document if it doesn't exist
            )
            operations_by_collection[collection_name].append(op)

        for collection_name, ops in operations_by_collection.items():
            if ops:
                collection = self.db[collection_name]
                collection.bulk_write(ops, ordered=False) # ordered=False can improve performance

    def _flush_training_logs(self, buffer_to_flush):
        # for training mode, we will handle logs differently
        # we will group by training_id, and bot_id will be on top view only
            
        grouped_for_update = defaultdict(lambda: {"logs_to_push": [], "training_id": None, "bot_id": None})

        for entry_dict in buffer_to_flush:
            bot_id = entry_dict.get("bot_id", "unknown_bid")
            training_id = entry_dict.get("training_id", "unknown_uid") # This will be for $setOnInsert
            
            key = (bot_id, training_id) # Group by these for UpdateOne logic
            
            grouped_for_update[key]["logs_to_push"].append(entry_dict)
            if grouped_for_update[key]["training_id"] is None:
                grouped_for_update[key]["training_id"] = training_id
            if grouped_for_update[key]["bot_id"] is None:
                grouped_for_update[key]["bot_id"] = bot_id
        
        operations_by_collection = defaultdict(list)
        for (op_bot_id, op_train_id), data in grouped_for_update.items():
            if op_bot_id == "unknown_bid" or op_train_id == "unknown_cid":
                self._fallback(f"[MongoHandler Chat Flush Warning] Missing bot_id or training_id: {data['logs_to_push']}")
                continue

            collection_name = self._get_collection_name(op_bot_id)
            
            # Create the MongoDB update operation
            op = UpdateOne(
                {"training_id": op_train_id, "bot_id": op_bot_id}, # Filter to find the document
                {
                    "$push": {"logs": {"$each": data["logs_to_push"]}},
                    "$setOnInsert": {
                        "training_id": op_train_id, # Explicitly set on insert
                        "bot_id": op_bot_id,           # Explicitly set on insert
                        "created_at": datetime.datetime.now(datetime.UTC)

                    },
                    "$set": {"updated_at": datetime.datetime.now(datetime.UTC)}
                },
                upsert=True  # Create the document if it doesn't exist
            )
            operations_by_collection[collection_name].append(op)

        for collection_name, ops in operations_by_collection.items():
            if ops:
                collection = self.db[collection_name]
                collection.bulk_write(ops, ordered=False) # ordered=False can improve performance

    def _flush_generic_logs_as_separate_docs(self, buffer_to_flush, collection_prefix="generic"):
        # Fallback flushing mechanism: store each log as a separate document
        if not buffer_to_flush:
            return
        env = self.config.get("env", "dev")
        collection_name = f"{collection_prefix}_{env}_{self.handler_instance_id}"
        operations = [InsertOne(entry_dict) for entry_dict in buffer_to_flush]
        if operations:
            collection = self.db[collection_name]
            collection.bulk_write(operations, ordered=False)

    def _flush_generic_logs_by_bot_id(self, buffer_to_flush, bot_id: str):
        self._flush_generic_logs_as_separate_docs(buffer_to_flush, collection_prefix=f"generic_{bot_id}")

    def reopen(self):
        self._connect()

    def _fallback(self, content):
        try:
            timestamp = datetime.datetime.now(datetime.UTC).isoformat() + "Z"
            with open(self.fallback_file, "a", encoding="utf-8") as f:
                if isinstance(content, list):
                    for entry in content:
                        if isinstance(entry, dict):
                             f.write(f"{timestamp} - {json.dumps(entry, default=str)}\n")
                        else: # If entry is a string (e.g. error message)
                             f.write(f"{timestamp} - {str(entry)}\n")
                else: # If content is a single message/string
                    f.write(f"{timestamp} - {str(content)}\n")
        except Exception as e_fallback:
            # Critical: fallback logging itself failed. Print to stderr.
            print(f"CRITICAL FALLBACK ERROR: {e_fallback} - Original content: {str(content)[:500]}...", flush=True) # Print limited content
            traceback.print_exc()

    def close(self):
        self.flush()
        if self.db_ready and self.client:
            try:
                self.client.close()
            except Exception as e:
                self._fallback(f"[MongoHandler Close Error] {str(e)}")
        super().close()
        