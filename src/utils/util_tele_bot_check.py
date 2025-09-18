import requests
import time
from typing import Optional
from src.config.config_variable import TELE_CONFIG
from src.config.config_logging import ConfigLogging


class UtilTeleBotCheck:
    """Lớp tiện ích cho thông báo Telegram bot"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("UtilTeleBotCheck")
        self.bot_token = TELE_CONFIG.get("tele_bot_token")
        self.chat_id = TELE_CONFIG.get("tele_chat_id")
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self.parse_mode = TELE_CONFIG.get("tele_message_parse", "HTML")
        self.last_sent_time = 0
        self.min_interval = TELE_CONFIG.get("tele_check_interval_second", 30)

    def send_message(self, message: str, force: bool = False) -> bool:
        """Gửi tin nhắn tới chat Telegram

        Args:
            message: Văn bản tin nhắn để gửi
            force: Buộc gửi mà không giới hạn tốc độ

        Returns:
            True nếu gửi thành công, False nếu không
        """
        try:
            # Kiểm tra xem bot có được cấu hình không
            if not self.bot_token or not self.chat_id:
                self.logger.warning("Telegram bot not configured")
                return False

            # Giới hạn tốc độ (trừ khi buộc)
            current_time = time.time()
            if not force and (current_time - self.last_sent_time) < self.min_interval:
                self.logger.debug("Rate limiting: message not sent")
                return False

            # Chuẩn bị request
            url = f"{self.base_url}/sendMessage"
            params = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": self.parse_mode,
            }

            # Gửi request
            response = requests.post(url, data=params, timeout=10)
            response.raise_for_status()

            self.last_sent_time = current_time
            self.logger.debug("Telegram message sent successfully")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error sending Telegram message: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending Telegram message: {e}")
            return False

    def send_alert(self, title: str, message: str, level: str = "INFO") -> bool:
        """Gửi tin nhắn cảnh báo đã định dạng

        Args:
            title: Tiêu đề cảnh báo
            message: Tin nhắn cảnh báo
            level: Mức độ cảnh báo (INFO, WARNING, ERROR)

        Returns:
            True nếu gửi thành công, False nếu không
        """
        try:
            # Định dạng tin nhắn với emoji dựa trên mức độ
            emoji_map = {
                "INFO": "ℹ️",
                "WARNING": "⚠️",
                "ERROR": "❌",
                "SUCCESS": "✅",
            }

            emoji = emoji_map.get(level.upper(), "📢")

            formatted_message = f"{emoji} <b>{title}</b>\n\n{message}"

            if level.upper() in ["WARNING", "ERROR"]:
                formatted_message += f"\n\nTime: {time.strftime('%Y-%m-%d %H:%M:%S')}"

            return self.send_message(formatted_message, force=True)

        except Exception as e:
            self.logger.error(f"Error sending alert: {e}")
            return False

    def send_status_update(self, status_data: dict) -> bool:
        """Gửi cập nhật trạng thái hệ thống

        Args:
            status_data: Từ điển chứa thông tin trạng thái

        Returns:
            True nếu gửi thành công, False nếu không
        """
        try:
            message = "<b>System Status Update</b>\n\n"

            for key, value in status_data.items():
                if isinstance(value, bool):
                    value = "Yes" if value else "No"
                elif isinstance(value, (int, float)):
                    value = f"{value:,}"

                # Định dạng key để hiển thị
                display_key = key.replace("_", " ").title()
                message += f"• <b>{display_key}:</b> {value}\n"

            return self.send_message(message)

        except Exception as e:
            self.logger.error(f"Error sending status update: {e}")
            return False

    def test_connection(self) -> bool:
        """Kiểm tra kết nối Telegram bot

        Returns:
            True nếu kết nối thành công, False nếu không
        """
        try:
            if not self.bot_token:
                self.logger.error("Bot token not configured")
                return False

            url = f"{self.base_url}/getMe"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            if data.get("ok"):
                bot_info = data.get("result", {})
                self.logger.info(
                    f"Bot connection successful: {bot_info.get('first_name', 'Unknown')}"
                )
                return True
            else:
                self.logger.error("Bot connection failed")
                return False

        except Exception as e:
            self.logger.error(f"Error testing bot connection: {e}")
            return False

    def send_funding_cycle_start(self, cycle_type: str, symbols_count: int, next_funding_time: str = None) -> bool:
        """Send funding cycle start notification
        
        Args:
            cycle_type: "4h" or "8h"
            symbols_count: Number of symbols in this cycle
            next_funding_time: Optional next funding time
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            emoji = "🔄" if cycle_type == "8h" else "⚡"
            title = f"Funding Cycle {cycle_type.upper()} Started"
            
            message = f"{emoji} <b>{title}</b>\n\n"
            message += f"• <b>Cycle Type:</b> {cycle_type} intervals\n"
            message += f"• <b>Symbols Count:</b> {symbols_count:,}\n"
            
            if next_funding_time:
                message += f"• <b>Next Funding:</b> {next_funding_time}\n"
                
            message += f"• <b>Started At:</b> {time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
            
            return self.send_message(message)
            
        except Exception as e:
            self.logger.error(f"Error sending funding cycle start notification: {e}")
            return False

    def send_funding_update_result(self, cycle_type: str, success_count: int, total_count: int, 
                                 failed_symbols: list = None, execution_time: float = None) -> bool:
        """Send funding update result notification
        
        Args:
            cycle_type: "4h" or "8h" 
            success_count: Number of successfully updated symbols
            total_count: Total number of symbols processed
            failed_symbols: List of symbols that failed to update
            execution_time: Time taken for the update
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Determine status
            if success_count == total_count:
                emoji = "✅"
                level = "SUCCESS"
                title = f"Funding {cycle_type.upper()} Update Completed"
            elif success_count > 0:
                emoji = "⚠️"
                level = "WARNING" 
                title = f"Funding {cycle_type.upper()} Update Partial"
            else:
                emoji = "❌"
                level = "ERROR"
                title = f"Funding {cycle_type.upper()} Update Failed"
            
            message = f"{emoji} <b>{title}</b>\n\n"
            message += f"• <b>Cycle:</b> {cycle_type} intervals\n"
            message += f"• <b>Success:</b> {success_count}/{total_count} symbols\n"
            
            if execution_time:
                message += f"• <b>Duration:</b> {execution_time:.2f}s\n"
            
            if failed_symbols:
                message += f"• <b>Failed Symbols:</b> {', '.join(failed_symbols[:5])}"
                if len(failed_symbols) > 5:
                    message += f" (+{len(failed_symbols) - 5} more)"
                message += "\n"
            
            message += f"• <b>Time:</b> {time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
            
            # Only send if there are issues or force sending for success
            if level in ["WARNING", "ERROR"] or (level == "SUCCESS" and total_count > 50):
                return self.send_message(message, force=True)
            else:
                self.logger.debug(f"Funding update completed successfully, no notification needed")
                return True
                
        except Exception as e:
            self.logger.error(f"Error sending funding update result: {e}")
            return False

    def send_data_verification_alert(self, cycle_type: str, missing_symbols: list, 
                                   expected_count: int, actual_count: int) -> bool:
        """Send alert when data verification fails
        
        Args:
            cycle_type: "4h" or "8h"
            missing_symbols: List of symbols with missing/stale data
            expected_count: Expected number of updated symbols
            actual_count: Actual number of updated symbols
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            title = f"Data Verification Failed - {cycle_type.upper()} Cycle"
            
            message = f"❌ <b>{title}</b>\n\n"
            message += f"• <b>Cycle:</b> {cycle_type} intervals\n"
            message += f"• <b>Expected Updates:</b> {expected_count}\n"
            message += f"• <b>Actual Updates:</b> {actual_count}\n"
            message += f"• <b>Missing Count:</b> {len(missing_symbols)}\n"
            
            if missing_symbols:
                message += f"• <b>Missing Symbols:</b> {', '.join(missing_symbols[:5])}"
                if len(missing_symbols) > 5:
                    message += f" (+{len(missing_symbols) - 5} more)"
                message += "\n"
            
            message += f"• <b>Alert Time:</b> {time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
            message += f"\n\n⚠️ <i>Please check the funding rate extraction system</i>"
            
            return self.send_message(message, force=True)
            
        except Exception as e:
            self.logger.error(f"Error sending data verification alert: {e}")
            return False
