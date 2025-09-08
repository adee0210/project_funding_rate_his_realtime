import os
from dotenv import load_dotenv

load_dotenv()
MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST", "localhost"),
    "port": os.getenv("MONGO_PORT", "27017"),
    "user": os.getenv("MONGO_USER"),
    "pass": os.getenv("MONGO_PASS"),
    "auth": os.getenv("MONGO_AUTH", "admin"),
}

TELE_CONFIG = {
    "tele_bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
    "tele_chat_id": os.getenv("TELEGRAM_CHAT_ID"),
    "tele_check_interval_second": 30,
    "tele_method": "sendMessage",
    "tele_message_parse": "HTML",
}
