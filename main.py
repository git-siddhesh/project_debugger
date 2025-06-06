# run_conversation()
# # Manual recovery endpoint
# @app.post("/logger/reopen")
# def trigger_reconnect():
#     LoggerSingleton.reopen()
#     return {"status": "Reconnect attempted"}


# main.py
# import time
from rag_mongo_logger.singleton import LoggerSingleton
from rag_mongo_logger.context_logging import conversation_logger, training_logger

def run_conversation(conv_id: str, bot_id: str, messages_count: int):
    user_id = 'admin_abc' 
    with conversation_logger(logger, conversation_id=conv_id, bot_id=bot_id, user_id=user_id) as log:

        log.info(f"Conversation {conv_id} started for user {user_id} with bot {bot_id}.")
        for i in range(messages_count):
            log.debug(f"Processing message {i+1}/{messages_count}.")
            # time.sleep(0.1) 
            if i % 3 == 0:
                log.warning(f"A sample warning at message {i+1}.")
        log.info("Conversation finished.")

def run_training_conversation(bot_id: str, training_id: str, messages_count: int):
    with training_logger(logger, bot_id=bot_id, training_id=training_id) as log:
        log.info(f"Training not: {bot_id} started with training instance: {training_id}.")
        for i in range(messages_count):
            log.debug(f"Processing document {i+1}/{messages_count}.")
            # time.sleep(0.1) 
            if i % 3 == 0:
                log.warning(f"A sample warning while processing document number {i+1}.")
        log.info("Training finished.")


if __name__ == "__main__":
    print("Starting example conversations...")

    # Configuration for the logger
    config = {
        "uri": "mongodb://localhost:27017/",  # Your MongoDB URI
        "db": "conversational_logs_5",          # Database name
        "env": "prod",                        # Environment (e.g., dev, prod, staging)
        # "logger_mode": "chat", # To be used if setup_logger handles modes
        "debug": False,                        # Enable debug mode for more verbose logging        
        "logger_mode": "training", 
        "log_batch_size": 5,                  # How many log entries to buffer before flushing
        "log_fallback_file": "app_fallback_logs.txt" # Fallback file
    }

    print("\nStarting training conversation simulation...")
    config["logger_mode"] = "training"  # Change mode for training logs
    logger = LoggerSingleton.get_logger(config)

    run_training_conversation(bot_id="bot_v2", training_id="1", messages_count=7)
    run_training_conversation(bot_id="bot_v2", training_id="1", messages_count=3)
    run_training_conversation(bot_id="bot_v2", training_id="2", messages_count=7)
    
    LoggerSingleton.close_logger() # Call the new close method

    print("\nStarting chat conversation simulations...")
    config["logger_mode"] = "chat"  # Change mode for chat logs
    LoggerSingleton.reopen()  # Reopen the logger to reset the mode and connection
    print("Reopen attempted. Logging one more message.")
    logger = LoggerSingleton.get_logger(config)


    # Example 1
    run_conversation(conv_id="my_convo-1", bot_id="bot_v2", messages_count=7)
    
    # Example 2: Different conversation, same bot
    run_conversation(conv_id="my_convo-2", bot_id="bot_v2", messages_count=3)

    # Example 3: Different bot
    run_conversation(conv_id="my_convo-3", bot_id="bot_v2", messages_count=6)

    # Important: Ensure logs are flushed on application exit
    print("All conversations simulated. Closing logger to flush remaining logs...")
    LoggerSingleton.close_logger() # Call the new close method
    print("Logger closed. Check MongoDB and fallback file if any.")

