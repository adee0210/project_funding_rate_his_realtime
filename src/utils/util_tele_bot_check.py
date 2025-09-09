from datetime import datetime
import requests
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname((__file__)), "..")))
from config.config_logging import ConfigLogging
from config.config_variable import TELE_CONFIG


class UtilTeleBotCheck:
    def __init__(self):
        self.logger = ConfigLogging.config_logging("Util tele bot check data")
        self.tele_chat_id = TELE_CONFIG["tele_chat_id"]
        self.tele_bot_token = TELE_CONFIG["tele_bot_token"]
        self.tele_method = TELE_CONFIG["tele_method"]

        self.tele_check_interval_second = TELE_CONFIG["tele_check_interval_second"]

        self.tele_message_parse = TELE_CONFIG["tele_message_parse"]

        self.tele_url = (
            f"https://api.telegram.org/bot{self.tele_bot_token}/{self.tele_method}"
        )

    def send_message(self, title: str, message: str, warning: str = "Warning"):
        try:
            message = f"""
        <b>{title.upper()}</b>
            Message: {message.capitalize()}
            Datetime: {datetime.now()}
            Level log: {warning}
        Wait the 30s to check next time!
            """
            response = requests.post(
                url=self.tele_url,
                data={
                    "text": message,
                    "chat_id": self.tele_chat_id,
                    "parse_mode": self.tele_message_parse,
                },
                timeout=10,
            )
            response.raise_for_status()
            self.logger.info("Telegram message sent sucessfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {str(e)}")
            return False


if __name__ == "__main__":
    test = UtilTeleBotCheck()
    test.send_message("test", "xin chao")
