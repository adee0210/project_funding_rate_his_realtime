from pymongo import MongoClient
from src.config.config_variable import MONGO_CONFIG


class ConfigMongo:
    """Singleton class để cấu hình kết nối MongoDB"""

    _instance = None

    def _init_config(self):
        self._config = {
            "host": MONGO_CONFIG["host"],
            "port": int(MONGO_CONFIG["port"]),  # Convert to int
            "user": MONGO_CONFIG["user"],
            "pass": MONGO_CONFIG["pass"],
            "auth": MONGO_CONFIG["auth"],
        }

    @property
    def get_config(self):
        return self._config

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigMongo, cls).__new__(cls)
            cls._instance._init_config()
            cls._instance._client = None
        return cls._instance

    def get_client(self):
        """Lấy client MongoDB đã được cấu hình

        Returns:
            MongoClient object
        """
        if self._client is None:
            self._client = MongoClient(
                host=self.get_config["host"],
                port=self.get_config["port"],
                username=self.get_config["user"],
                password=self.get_config["pass"],
                authSource=self.get_config["auth"],
            )
        return self._client
