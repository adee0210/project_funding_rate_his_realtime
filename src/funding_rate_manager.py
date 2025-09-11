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
from src.extract.extract_realtime import ExtractFundingRateRealtime
from src.load.load_mongo import LoadMongo
from src.utils.util_tele_bot_check import UtilTeleBotCheck


class FundingRateManager:
    """Quản lý chính cho việc trích xuất và giám sát dữ liệu tỷ lệ funding"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("FundingRateManager")

        # Initialize components
        self.extract_history = ExtractFundingRateHistory()
        self.extract_realtime = ExtractFundingRateRealtime()
        self.load_mongo = LoadMongo()
        self.tele_bot = UtilTeleBotCheck()

        # State management
        self.is_running = False
        self.symbols = []
        self.history_thread = None
        self.monitoring_thread = None

        # Configuration from config_variable
        self.top_symbols_count = SYSTEM_CONFIG["top_symbols_count"]
        self.history_update_interval = SYSTEM_CONFIG["history_update_interval"]
        self.monitoring_interval = SYSTEM_CONFIG["monitoring_interval"]

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
            message += f"Monitoring interval: {self.monitoring_interval}s"

            self.tele_bot.send_message(message)

            return True

        except Exception as e:
            self.logger.error(f"Error during initialization: {e}")
            return False

    def _extract_initial_history(self):
        """Trích xuất lịch sử tỷ lệ funding ban đầu"""
        try:
            self.logger.info("Starting initial history extraction")

            # Extract 30 days of history for all symbols
            success = self.extract_history.extract_all_history(
                self.symbols, days_back=30
            )

            if success:
                self.logger.info("Initial history extraction completed successfully")
                self.tele_bot.send_message(
                    "Initial funding rate history extraction completed"
                )
            else:
                self.logger.error("Initial history extraction failed")
                self.tele_bot.send_message(
                    "Initial funding rate history extraction failed"
                )

        except Exception as e:
            self.logger.error(f"Error in initial history extraction: {e}")
            self.tele_bot.send_message(f"❌ Error in history extraction: {str(e)}")

    def _periodic_history_update(self):
        """Cập nhật lịch sử tỷ lệ funding định kỳ - chỉ cho realtime system

        Lưu ý: History chính được xử lý bởi scheduler riêng (0h, 8h, 16h)
        Function này chỉ xử lý backup incremental updates
        """
        while self.is_running:
            try:
                self.logger.info("Starting periodic incremental update")

                # Chỉ extract incremental data cho backup
                # Scheduler chính sẽ handle history extraction
                success = self.extract_history.extract_recent_history(
                    self.symbols[
                        : self.max_realtime_symbols
                    ]  # Chỉ process symbols đang monitor realtime
                )

                if success:
                    self.logger.info("Periodic incremental update completed")
                else:
                    self.logger.warning("Periodic incremental update had issues")

                # Wait for next update (longer interval since main history handled by scheduler)
                for _ in range(self.history_update_interval * 2):  # Double interval
                    if not self.is_running:
                        break
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in periodic incremental update: {e}")
                time.sleep(60)  # Wait 1 minute before retrying

    def _system_monitoring(self):
        """Giám sát trạng thái hệ thống và gửi cập nhật"""
        while self.is_running:
            try:
                # Get system statistics
                stats = self.load_mongo.get_funding_rate_stats()
                realtime_status = self.extract_realtime.get_status()

                # Tạo tin nhắn trạng thái
                message = f"Funding Rate System Status\n\n"
                message += f"Realtime Status: {'Connected' if realtime_status['is_connected'] else 'Disconnected'}\n"
                message += f"Symbols Monitored: {realtime_status['symbols_count']}\n"
                message += f"Total Collections: {stats.get('total_symbols', 0)}\n"
                message += f"Realtime Records: {stats.get('realtime_count', 0)}\n"
                message += (
                    f"Reconnect Attempts: {realtime_status['reconnect_attempts']}\n"
                )
                message += (
                    f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )

                # Thêm chi tiết collection (top 5)
                if stats.get("collections"):
                    message += f"\n\nTop Collections:\n"
                    sorted_collections = sorted(
                        stats["collections"].items(), key=lambda x: x[1], reverse=True
                    )[:5]

                    for collection, count in sorted_collections:
                        symbol = collection.replace("funding_rate_history_", "").upper()
                        message += f"• {symbol}: {count:,} records\n"

                self.logger.info("System status monitoring update")
                self.tele_bot.send_message(message)

                # Wait for next monitoring cycle
                for _ in range(self.monitoring_interval):
                    if not self.is_running:
                        break
                    time.sleep(1)

            except Exception as e:
                self.logger.error(f"Error in system monitoring: {e}")
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

            # Initialize system
            if not self.initialize():
                return False

            self.is_running = True

            # Khởi động trích xuất realtime với số lượng symbols giới hạn
            symbols_for_realtime = self.symbols[: self.max_realtime_symbols]
            self.logger.info(
                f"Starting realtime extraction for {len(symbols_for_realtime)} symbols (limited from {len(self.symbols)} total)"
            )

            if not self.extract_realtime.start_realtime_extraction(
                symbols_for_realtime
            ):
                self.logger.error("Failed to start realtime extraction")
                self.is_running = False
                return False

            # Khởi động trích xuất lịch sử ban đầu trong background
            threading.Thread(target=self._extract_initial_history, daemon=True).start()

            # Khởi động cập nhật lịch sử định kỳ
            self.history_thread = threading.Thread(
                target=self._periodic_history_update, daemon=True
            )
            self.history_thread.start()

            # Khởi động giám sát hệ thống
            self.monitoring_thread = threading.Thread(
                target=self._system_monitoring, daemon=True
            )
            self.monitoring_thread.start()

            self.logger.info("Funding Rate Realtime Manager started successfully")
            self.tele_bot.send_message(
                "🚀 Funding Rate Realtime Manager started!\n"
                f"📊 Monitoring {len(symbols_for_realtime)} symbols in realtime\n"
                f"⏰ History handled by separate scheduler (0h, 8h, 16h)\n"
                f"💾 Realtime data: WebSocket stream active"
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

            # Dừng trích xuất realtime
            self.extract_realtime.stop_realtime_extraction()

            # Chờ threads kết thúc
            if self.history_thread and self.history_thread.is_alive():
                self.history_thread.join(timeout=10)

            if self.monitoring_thread and self.monitoring_thread.is_alive():
                self.monitoring_thread.join(timeout=10)

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
            realtime_status = self.extract_realtime.get_status()

            return {
                "is_running": self.is_running,
                "symbols_count": len(self.symbols),
                "symbols": self.symbols[:10],  # First 10 symbols
                "realtime_status": realtime_status,
                "database_stats": stats,
                "threads_alive": {
                    "history_thread": (
                        self.history_thread.is_alive() if self.history_thread else False
                    ),
                    "monitoring_thread": (
                        self.monitoring_thread.is_alive()
                        if self.monitoring_thread
                        else False
                    ),
                },
            }

        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {"error": str(e)}

    def run_forever(self):
        """Chạy hệ thống mãi mãi (blocking)"""
        try:
            if not self.start():
                return False

            self.logger.info("Funding Rate Manager is running. Press Ctrl+C to stop.")

            # Keep the main thread alive
            while self.is_running:
                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            self.stop()


if __name__ == "__main__":
    manager = FundingRateManager()
    manager.run_forever()
