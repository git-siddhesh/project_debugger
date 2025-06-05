
# rag-mongo-logger

🚀 A robust, async-buffered Python logging library supporting MongoDB, PostgreSQL, and local file fallback. Designed for **chatbots**, **training pipelines**, or any system needing conversation-oriented logging.

---

## 📦 Installation

Install from PyPI:

```bash
pip install rag-mongo-logger
````

---

## ⚙️ Configuration

```python
config = {
    "uri": "mongodb://localhost:27017/",  # or PostgreSQL URI
    "db": "conversational_logs",          # MongoDB database name
    "env": "prod",                        # Optional: "dev", "staging", "prod"
    "logger_mode": "chat",                # "chat" or "training"
    "log_batch_size": 5,                  # Buffer size before flushing
    "log_fallback_file": "fallback.txt"   # Local file fallback on DB failure
}
```

---

## 🧠 Modes of Operation

### 1. Chat Mode (`logger_mode='chat'`)

Logs conversations with optional metadata like `conversation_id`, `bot_id`, `user_id`.

```python
from rag_mongo_logger.singleton import LoggerSingleton
from rag_mongo_logger.context_logging import conversation_logger

logger = LoggerSingleton.get_logger(config)

with conversation_logger(logger, conversation_id="conv-123", bot_id="bot-xyz", user_id="admin_001") as log:
    log.info("User started the conversation.")
    log.debug("Fetching documents...")
    log.warning("Timeout fetching from external API.")
```

### 2. Training Mode (`logger_mode='training'`)

Logs training operations with context like `bot_id` and `training_id`.

```python
from rag_mongo_logger.context_logging import training_logger

with training_logger(logger, bot_id="bot-abc", training_id="train-001") as log:
    log.info("Training session started.")
    log.debug("Processing document 3 of 10.")
    log.warning("Skipped document due to format issue.")
```

---

## 🔁 Manual DB Reconnect

If your DB (Mongo/PostgreSQL) crashes and comes back up mid-execution:

```python
logger.reopen()
```

Or expose a FastAPI endpoint:

```python
@app.post("/logger/reopen")
def trigger_reconnect():
    logger.reopen()
    return {"status": "Reconnection attempted"}
```

---

## 🧪 Example Usage

```python
from rag_mongo_logger.singleton import LoggerSingleton
from rag_mongo_logger.context_logging import conversation_logger, training_logger

config = {
    "uri": "mongodb://localhost:27017/",
    "db": "conversational_logs",
    "env": "prod",
    "logger_mode": "training",
    "log_batch_size": 5,
    "log_fallback_file": "app_fallback_logs.txt"
}

logger = LoggerSingleton.get_logger(config)

# Training mode
with training_logger(logger, bot_id="bot_v2", training_id="train_123") as log:
    log.info("Training started.")
    log.debug("Document 1 processed.")
    log.warning("Skipped doc due to malformed content.")

# Switch to chat mode
LoggerSingleton.close_logger()
config["logger_mode"] = "chat"
LoggerSingleton.reopen()
logger = LoggerSingleton.get_logger(config)

# Chat mode
with conversation_logger(logger, conversation_id="conv_42", bot_id="bot_v2", user_id="admin") as log:
    log.info("Conversation started.")
    log.debug("User asked about refund policy.")
```

---

## ✅ Best Practices (Do's)

* ✅ Use `LoggerSingleton.get_logger(config)` once per mode/config.
* ✅ Always use context loggers (`conversation_logger` or `training_logger`) for automatic flushing and tagging.
* ✅ Call `LoggerSingleton.close_logger()` before switching modes.
* ✅ Use `.reopen()` when recovering from DB crashes mid-run.
* ✅ Provide consistent `conversation_id`, `bot_id`, `user_id` for grouping logs.

---

## ❌ Pitfalls to Avoid (Don'ts)

* ❌ Don't call `get_logger()` repeatedly without closing — it’s a singleton.
* ❌ Don't log outside the context manager unless necessary — flushing won't be guaranteed.
* ❌ Don't forget to close the logger at the end of scripts (`LoggerSingleton.close_logger()`).
* ❌ Don't rely solely on the DB connection — always configure a fallback file.

---

## 📁 Fallback Logs

If the DB is down, logs are written to `log_fallback_file` (e.g., `fallback.txt`), using a rotating file handler (5MB x 3 backups).

---

## 📜 License

MIT License © Siddhesh Dosi

---

## 🙋 Need Help?

Open an issue or contribute at [GitHub Repository](https://github.com/yourusername/rag-mongo-logger)



