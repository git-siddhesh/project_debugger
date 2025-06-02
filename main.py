from singleton import LoggerSingleton
from context_logging import conversation_logger

config = {
    "uri": "mongodb://localhost:27017/",
    "db": "logs",
    "collection": "conversations",
}

logger = LoggerSingleton.get_logger(config)

def run_conversation():
    with conversation_logger(logger, conversation_id="conv-42") as log:
        log.info("Conversation started")
        log.debug("Fetching data")
        log.info("Generating answer")
        log.info("Conversation finished")

# Manual recovery endpoint
@app.post("/logger/reopen")
def trigger_reconnect():
    LoggerSingleton.reopen()
    return {"status": "Reconnect attempted"}
