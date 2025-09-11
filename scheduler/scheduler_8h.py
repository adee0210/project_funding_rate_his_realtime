#!/usr/bin/env python3
"""
Scheduler script to run funding rate data extraction every 8 hours (0h, 8h, 16h)
"""

import sys
import os
import time
import schedule
from datetime import datetime

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from src.extract.extract_history import ExtractFundingRateHistory
from src.config.config_logging import ConfigLogging
from src.utils.util_tele_bot_check import UtilTeleBotCheck


class FundingRateScheduler:
    """Scheduler cho việc trích xuất dữ liệu tỷ lệ funding mỗi 8 giờ"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("FundingRateScheduler")
        self.extract = ExtractFundingRateHistory()
        self.tele_bot = UtilTeleBotCheck()
        self.is_running = False

    def run_scheduled_extraction(self):
        """Chạy trích xuất theo lịch và lưu vào history collection

        Chạy vào 0h, 8h, 16h mỗi ngày để:
        - Trích xuất dữ liệu funding rate trong 8h qua
        - Lưu vào history collection thay vì realtime
        - Đảm bảo dữ liệu được lưu trữ lâu dài
        """
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.logger.info(f"Starting scheduled extraction at: {current_time}")

            # Get top 100 symbols để tránh quá tải
            symbols = self.extract.get_top_symbols(limit=100)

            if not symbols:
                self.logger.error("No symbols to process")
                self.tele_bot.send_message(
                    "Scheduled extraction failed - no symbols found"
                )
                return

            self.logger.info(f"Processing {len(symbols)} symbols for history storage")

            # Gửi thông báo bắt đầu
            self.tele_bot.send_message(
                f"📊 Scheduled history extraction started at {current_time}\n"
                f"⏰ Time: {datetime.now().strftime('%H:%M')} (8-hour interval)\n"
                f"🎯 Processing {len(symbols)} symbols to history collection"
            )

            # Trích xuất dữ liệu 8h gần nhất để lưu vào history
            # Logic auto-detect sẽ xử lý việc lấy data từ lần cuối cùng
            success = self.extract.extract_all_history(symbols, days_back=1)

            if success:
                self.logger.info("Scheduled extraction completed successfully")
                self.tele_bot.send_message(
                    f"✅ History extraction completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"📈 {len(symbols)} symbols processed to history collections"
                )
            else:
                self.logger.error("Scheduled extraction failed")
                self.tele_bot.send_message("❌ Scheduled history extraction failed")

        except Exception as e:
            self.logger.error(f"Error in scheduled extraction: {e}")
            self.tele_bot.send_message(f"❌ Scheduled extraction error: {str(e)}")

    def start_scheduler(self):
        """Khởi động scheduler để chạy trích xuất mỗi 8 giờ"""
        try:
            self.logger.info("Starting funding rate scheduler")

            # Schedule extraction at 00:00, 08:00, and 16:00 daily
            schedule.every().day.at("00:00").do(self.run_scheduled_extraction)
            schedule.every().day.at("08:00").do(self.run_scheduled_extraction)
            schedule.every().day.at("16:00").do(self.run_scheduled_extraction)

            # Gửi thông báo khởi động
            self.tele_bot.send_message(
                "🚀 Funding Rate History Scheduler Started\n"
                "⏰ Schedule: Every 8 hours (00:00, 08:00, 16:00)\n"
                "🎯 Processing top 100 symbols\n"
                "💾 Data saved to history collections (permanent storage)"
            )

            self.logger.info(
                "Scheduler configured for 00:00, 08:00, and 16:00 daily - saving to history"
            )
            self.is_running = True

            # Chạy ngay lập tức một lần để test
            self.logger.info("Running initial history extraction...")
            self.run_scheduled_extraction()

            # Keep scheduler running
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute

        except KeyboardInterrupt:
            self.logger.info("Scheduler stopped by user")
            self.tele_bot.send_message("Funding Rate Scheduler stopped")
        except Exception as e:
            self.logger.error(f"Error in scheduler: {e}")
            self.tele_bot.send_message(f"Scheduler error: {str(e)}")
        finally:
            self.is_running = False


def main():
    """Main entry point for scheduler"""
    scheduler = FundingRateScheduler()

    try:
        scheduler.start_scheduler()
    except KeyboardInterrupt:
        print("\nScheduler stopped by user")
        scheduler.stop_scheduler()
    except Exception as e:
        print(f"Error: {e}")
        scheduler.stop_scheduler()
        sys.exit(1)


if __name__ == "__main__":
    main()
