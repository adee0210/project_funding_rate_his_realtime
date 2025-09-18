import threading
import time
import signal
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.config_logging import ConfigLogging
from src.config.config_variable import SYSTEM_CONFIG
from src.extract.extract_history import ExtractFundingRateHistory
from scheduler.advanced_funding_scheduler import AdvancedFundingRateScheduler
from src.load.load_mongo import LoadMongo
from src.utils.util_tele_bot_check import UtilTeleBotCheck


class FundingRateManager:
    """Quản lý chính cho việc trích xuất và giám sát dữ liệu tỷ lệ funding"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("FundingRateManager")

        # Initialize components
        self.extract_history = ExtractFundingRateHistory()
        self.load_mongo = LoadMongo()
        self.tele_bot = UtilTeleBotCheck()

        # State management
        self.is_running = False
        self.symbols = []
        self.history_thread = None
        self.advanced_scheduler = None  # New advanced scheduler

        # Configuration from config_variable
        self.top_symbols_count = SYSTEM_CONFIG["top_symbols_count"]
        self.history_update_interval = SYSTEM_CONFIG["history_update_interval"]
        self.monitoring_interval = SYSTEM_CONFIG["monitoring_interval"]  # Now 1 hour

        # Giới hạn symbols cho realtime để tránh tràn RAM
        self.max_realtime_symbols = min(100, self.top_symbols_count)

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Xử lý tín hiệu tắt"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)

    def initialize(self) -> bool:
        """Khởi tạo hệ thống

        Returns:
            True nếu khởi tạo thành công, False nếu không
        """
        try:
            self.logger.info("Initializing Funding Rate Manager")

            # Get top symbols
            self.symbols = self.extract_history.get_top_symbols(self.top_symbols_count)
            if not self.symbols:
                self.logger.error("Failed to get top symbols")
                return False

            self.logger.info(
                f"Loaded {len(self.symbols)} symbols: {', '.join(self.symbols[:10])}..."
            )

            # Gửi thông báo khởi tạo
            message = f"Funding Rate Manager Initialized\n"
            message += f"Monitoring {len(self.symbols)} symbols\n"
            message += f"History update interval: {self.history_update_interval}s\n"
            message += f"Monitoring interval: {self.monitoring_interval}s (Advanced scheduler handles notifications)"

            self.tele_bot.send_message(message)

            return True

        except Exception as e:
            self.logger.error(f"Error during initialization: {e}")
            return False

    def _extract_initial_history(self):
        """Trích xuất lịch sử tỷ lệ funding ban đầu"""
        try:
            self.logger.info("Starting initial history extraction")
            # Extract recent history for all symbols (last 7 days)
            success = self.extract_history.extract_all_history(self.symbols, days_back=7)
            if success:
                self.logger.info("Initial history extraction completed")
            else:
                self.logger.warning("Initial history extraction completed with some issues")
        except Exception as e:
            self.logger.error(f"Error in initial history extraction: {e}")

    def _periodic_history_update(self):
        """Cập nhật lịch sử tỷ lệ funding theo chu kỳ"""
        while self.is_running:
            try:
                self.logger.info("Starting periodic history update")
                # Extract recent history (last 2 days for incremental update)
                success = self.extract_history.extract_all_history(self.symbols, days_back=2)
                if success:
                    self.logger.info("Periodic history update completed")
                else:
                    self.logger.warning("Periodic history update completed with issues")

                # Wait for next update cycle
                for _ in range(self.history_update_interval):
                    if not self.is_running:
                        break
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in periodic incremental update: {e}")
                time.sleep(60)  # Wait 1 minute before retrying

    def start(self) -> bool:
        """Khởi động hệ thống tỷ lệ funding

        Returns:
            True nếu khởi động thành công, False nếu không
        """
        try:
            if self.is_running:
                self.logger.warning("System is already running")
                return True

            self.logger.info("Starting Funding Rate Manager")
            self.is_running = True

            # Chọn symbols cho realtime (giới hạn để tránh quá tải)
            symbols_for_realtime = self.symbols[:self.max_realtime_symbols]
            self.logger.info(f"Selected {len(symbols_for_realtime)} symbols for realtime extraction")

            # Initialize and start advanced scheduler
            self.advanced_scheduler = AdvancedFundingRateScheduler(symbols_for_realtime)
            if not self.advanced_scheduler.start_scheduler():
                self.logger.error("Failed to start advanced scheduler")
                self.is_running = False
                return False

            # Khởi động trích xuất lịch sử ban đầu trong background
            threading.Thread(target=self._extract_initial_history, daemon=True).start()

            # Khởi động cập nhật lịch sử định kỳ
            self.history_thread = threading.Thread(
                target=self._periodic_history_update, daemon=True
            )
            self.history_thread.start()

            # Advanced scheduler handles all monitoring and notifications
            # No need for separate system monitoring thread

            self.logger.info("Funding Rate Manager with Advanced Scheduler started successfully")
            self.tele_bot.send_message(
                "🚀 Funding Rate Manager with Advanced Scheduler started!\n"
                f"📊 Multi-interval monitoring: {len(symbols_for_realtime)} symbols\n"
                f"⚡ 1h monitoring: Real-time data updates\n"
                f"🔄 4h/8h cycles: Funding rate extraction\n"
                f"🔍 Intelligent verification and alerts (built-in)\n"
                f"💾 History handled by separate scheduler (0h, 8h, 16h)\n"
                f"📱 Status updates sent only when needed"
            )

            return True

        except Exception as e:
            self.logger.error(f"Error starting system: {e}")
            self.is_running = False
            return False

    def stop(self) -> bool:
        """Dừng hệ thống tỷ lệ funding

        Returns:
            True nếu dừng thành công, False nếu không
        """
        try:
            if not self.is_running:
                self.logger.warning("System is not running")
                return True

            self.logger.info("Stopping Funding Rate Manager")
            self.is_running = False

            # Stop advanced scheduler
            if self.advanced_scheduler:
                self.advanced_scheduler.stop_scheduler()

            # Chờ threads kết thúc
            if self.history_thread and self.history_thread.is_alive():
                self.history_thread.join(timeout=10)

            self.logger.info("Funding Rate Manager stopped successfully")
            self.tele_bot.send_message("Funding Rate Manager stopped")
            return True

        except Exception as e:
            self.logger.error(f"Error stopping system: {e}")
            return False

    def restart(self) -> bool:
        """Khởi động lại hệ thống tỷ lệ funding

        Returns:
            True nếu khởi động lại thành công, False nếu không
        """
        self.logger.info("Restarting Funding Rate Manager")

        if not self.stop():
            return False

        time.sleep(5)  # Wait a bit before restarting

        return self.start()

    def get_status(self) -> Dict[str, Any]:
        """Lấy trạng thái hệ thống

        Returns:
            Từ điển trạng thái
        """
        try:
            stats = self.load_mongo.get_funding_rate_stats()
            
            # Get advanced scheduler status
            scheduler_status = {}
            if self.advanced_scheduler:
                scheduler_status = self.advanced_scheduler.get_status()

            return {
                "is_running": self.is_running,
                "symbols_count": len(self.symbols),
                "symbols": self.symbols[:10],  # First 10 symbols
                "advanced_scheduler_status": scheduler_status,
                "database_stats": stats,
                "threads_alive": {
                    "history_thread": (
                        self.history_thread.is_alive() if self.history_thread else False
                    ),
                },
            }

        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {"error": str(e)}

    def run_forever(self):
        """Chạy hệ thống mãi mãi"""
        if not self.initialize():
            self.logger.error("Failed to initialize system")
            return

        if not self.start():
            self.logger.error("Failed to start system")
            return

        try:
            # Advanced scheduler handles everything, just keep main thread alive
            while self.is_running:
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self.stop()


if __name__ == "__main__":
    manager = FundingRateManager()
    manager.run_forever()