"""Cấu hình biến môi trường cho ứng dụng"""

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

# Cấu hình realtime
REALTIME_CONFIG = {
    "websocket_url": os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/ws/"),
    "reconnect_interval": int(os.getenv("WS_RECONNECT_INTERVAL", "5")),
    "max_reconnect_attempts": int(os.getenv("WS_MAX_RECONNECT_ATTEMPTS", "10")),
    "batch_size": int(os.getenv("REALTIME_BATCH_SIZE", "20")),
    "transform_interval": int(os.getenv("REALTIME_TRANSFORM_INTERVAL", "5")),
    "collection_name": os.getenv("REALTIME_COLLECTION", "realtime"),
    "ping_interval": int(os.getenv("WS_PING_INTERVAL", "30")),
    "connection_timeout": int(os.getenv("WS_CONNECTION_TIMEOUT", "10")),
}

# Cấu hình hệ thống
SYSTEM_CONFIG = {
    "top_symbols_count": int(os.getenv("TOP_SYMBOLS_COUNT", "100")),
    "history_update_interval": int(os.getenv("HISTORY_UPDATE_INTERVAL", "3600")),
    "monitoring_interval": int(os.getenv("MONITORING_INTERVAL", "3600")),  
    "max_symbols_per_websocket": int(os.getenv("MAX_SYMBOLS_PER_WS", "200")),
}
