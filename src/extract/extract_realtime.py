import requests
import time
import schedule
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import traceback

from src.config.config_logging import ConfigLogging
from src.config.config_variable import REALTIME_CONFIG, SYSTEM_CONFIG
from src.load.load_mongo import LoadMongo
from src.transform.transform_funding import TransformFundingData
from src.utils.util_tele_bot_check import UtilTeleBotCheck
from src.utils.funding_interval_detector import FundingIntervalDetector


class ExtractFundingRateRealtime:
    """Trích xuất dữ liệu tỷ lệ funding từ Binance REST API với các cập nhật theo lịch"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("ExtractFundingRateRealtime")
        self.load_mongo = LoadMongo()
        self.transform_funding = TransformFundingData()
        self.tele_bot = UtilTeleBotCheck()
        
        # Khởi tạo detector phát hiện interval funding
        self.interval_detector = FundingIntervalDetector("funding_intervals_cache.json")

        # Cấu hình
        self.config = REALTIME_CONFIG
        self.base_url = "https://fapi.binance.com"
        
        # Trạng thái và quản lý
        self.is_running = False
        self.symbols = []
        self.scheduler_thread = None
        self.last_update_time = None
        
        # Các symbol theo chu kỳ funding khác nhau
        self.symbols_8h = []  # Chu kỳ chuẩn 8 giờ
        self.symbols_4h = []  # Chu kỳ 4 giờ

    def start_realtime_extraction(self, symbols: List[str]) -> bool:
        """Bắt đầu trích xuất tỷ lệ funding theo lịch

        Args:
            symbols: Danh sách các symbol cần giám sát

        Returns:
            True nếu khởi động thành công, False nếu không
        """
        try:
            if self.is_running:
                self.logger.warning("Realtime extraction already running")
                return True

            if not symbols:
                self.logger.error("No symbols provided for realtime extraction")
                return False

            self.symbols = symbols[:100]  # Top 100 symbols
            self.is_running = True
            
            # Phân loại các symbol theo tần suất funding
            self._categorize_symbols_by_funding_frequency()
            
            self.logger.info(
                f"Starting scheduled extraction for {len(self.symbols)} symbols"
            )
            self.logger.info(f"8-hour funding symbols: {len(self.symbols_8h)}")
            self.logger.info(f"4-hour funding symbols: {len(self.symbols_4h)}")

            # Thiết lập các lịch
            self._setup_schedules()
            
            # Bắt đầu luồng scheduler
            self.scheduler_thread = threading.Thread(
                target=self._run_scheduler, daemon=True
            )
            self.scheduler_thread.start()

            # Kích hoạt cập nhật ban đầu cho chu kỳ funding gần nhất
            threading.Thread(target=self._initial_update, daemon=True).start()

            # Gửi thông báo ban đầu
            self.tele_bot.send_message(
                f"Funding Rate Realtime Extraction Started\n"
                f"Monitoring {len(self.symbols)} symbols\n"
                f"8-hour symbols: {len(self.symbols_8h)}\n"
                f"4-hour symbols: {len(self.symbols_4h)}\n"
                f"Initial update triggered for nearest funding cycle"
            )

            self.logger.info("Realtime extraction started successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error starting realtime extraction: {e}")
            self.is_running = False
            return False

    def stop_realtime_extraction(self) -> bool:
        """Dừng trích xuất realtime

        Returns:
            True nếu dừng thành công, False nếu không
        """
        try:
            if not self.is_running:
                self.logger.warning("Realtime extraction not running")
                return True

            self.logger.info("Stopping realtime extraction")
            self.is_running = False
            
            # Xóa các lịch
            schedule.clear()
            
            # Chờ luồng scheduler kết thúc
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=10)

            self.logger.info("Realtime extraction stopped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error stopping realtime extraction: {e}")
            return False

    def _categorize_symbols_by_funding_frequency(self):
        """Phân loại các symbol theo chu kỳ funding (4h hoặc 8h) bằng cơ chế phát hiện thông minh"""
        try:
            self.logger.info("Starting intelligent funding interval detection...")
            
            # Sử dụng detector để phát hiện interval
            intervals = self.interval_detector.detect_funding_intervals(self.symbols)
            
            # Reset lại danh sách symbol
            self.symbols_8h = []
            self.symbols_4h = []

            # Phân loại symbol dựa trên kết quả phát hiện
            for symbol in self.symbols:
                interval = intervals.get(symbol, "8h")  # Mặc định là 8h nếu không phát hiện được

                if interval == "4h":
                    self.symbols_4h.append(symbol)
                else:
                    self.symbols_8h.append(symbol)
            
            # Ghi log kết quả
            self.logger.info(f"Funding interval detection completed:")
            self.logger.info(f"  - 8h symbols: {len(self.symbols_8h)}")
            self.logger.info(f"  - 4h symbols: {len(self.symbols_4h)}")
            
            # Ghi log thống kê cache
            cache_stats = self.interval_detector.get_cache_stats()
            self.logger.info(f"Cache stats: {cache_stats['total_symbols']} total symbols cached")
            
            # Hiển thị một vài ví dụ
            if self.symbols_4h:
                self.logger.info(f"4h symbols examples: {self.symbols_4h[:5]}")
            if self.symbols_8h:
                self.logger.info(f"8h symbols examples: {self.symbols_8h[:5]}")
                
        except Exception as e:
            self.logger.error(f"Error in intelligent funding interval detection: {e}")
            # Fallback về cách làm thận trọng
            self.symbols_8h = self.symbols.copy()
            self.symbols_4h = []
            self.logger.warning("Fallback: All symbols set to 8h funding")

    def _setup_schedules(self):
        """Thiết lập các job theo lịch để cập nhật tỷ lệ funding"""
        try:
            # Lên lịch cập nhật 8 giờ vào 00:00, 08:00, 16:00 UTC
            schedule.every().day.at("00:00").do(self._update_8h_symbols)
            schedule.every().day.at("08:00").do(self._update_8h_symbols)
            schedule.every().day.at("16:00").do(self._update_8h_symbols)
            
            # Lên lịch cập nhật 4 giờ vào 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC
            schedule.every().day.at("00:00").do(self._update_4h_symbols)
            schedule.every().day.at("04:00").do(self._update_4h_symbols)
            schedule.every().day.at("08:00").do(self._update_4h_symbols)
            schedule.every().day.at("12:00").do(self._update_4h_symbols)
            schedule.every().day.at("16:00").do(self._update_4h_symbols)
            schedule.every().day.at("20:00").do(self._update_4h_symbols)
            
            self.logger.info("Schedules setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up schedules: {e}")

    def _run_scheduler(self):
        """Vòng lặp chạy scheduler"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Kiểm tra mỗi phút
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)

    def _update_8h_symbols(self):
        """Cập nhật tỷ lệ funding cho các symbol chu kỳ 8 giờ"""
        if not self.is_running or not self.symbols_8h:
            return
            
        try:
            self.logger.info(f"Updating {len(self.symbols_8h)} symbols with 8-hour funding")
            self._fetch_and_update_funding_rates(self.symbols_8h, "8h")
        except Exception as e:
            self.logger.error(f"Error updating 8h symbols: {e}")

    def _update_4h_symbols(self):
        """Cập nhật tỷ lệ funding cho các symbol chu kỳ 4 giờ"""
        if not self.is_running or not self.symbols_4h:
            return
            
        try:
            self.logger.info(f"Updating {len(self.symbols_4h)} symbols with 4-hour funding")
            self._fetch_and_update_funding_rates(self.symbols_4h, "4h")
        except Exception as e:
            self.logger.error(f"Error updating 4h symbols: {e}")

    def _fetch_and_update_funding_rates(self, symbols: List[str], interval: str):
        """Lấy và cập nhật tỷ lệ funding cho các symbol được chỉ định

        Args:
            symbols: Danh sách symbol cần cập nhật
            interval: Chu kỳ funding (4h hoặc 8h)
        """
        try:
            # Lấy dữ liệu funding hiện tại từ API
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"API request failed with status {response.status_code}")
                return
                
            data = response.json()
            
            # Lọc dữ liệu cho các symbol của chúng ta
            filtered_data = []
            for item in data:
                if item['symbol'] in symbols:
                    # Chuyển đổi response API về định dạng của chúng ta
                    funding_data = {
                        'symbol': item['symbol'],
                        'interval': interval,
                        'time_to_next_funding': item.get('nextFundingTime', 0),
                        'funding_rate': float(item.get('lastFundingRate', 0)),
                        'interest_rate': float(item.get('interestRate', 0)),
                        'mark_price': float(item.get('markPrice', 0)),
                        'index_price': float(item.get('indexPrice', 0)),
                        'estimated_settle_price': float(item.get('estimatedSettlePrice', 0)),
                        'funding_cap': 0.005,  # Ngưỡng funding tối đa chuẩn Binance
                        'funding_floor': -0.005,  # Ngưỡng funding tối thiểu chuẩn Binance
                        'last_update_time': int(time.time() * 1000)
                    }
                    filtered_data.append(funding_data)
            
            if not filtered_data:
                self.logger.warning(f"No data received for {interval} symbols")
                return
                
            # Biến đổi dữ liệu
            transformed_data = self.transform_funding.transform_realtime_funding_data(filtered_data)
            
            if transformed_data:
                # Update data in MongoDB (upsert)
                success = self.load_mongo.update_realtime_funding_data(
                    self.config["collection_name"], 
                    transformed_data
                )
                
                if success:
                    self.last_update_time = datetime.now(timezone.utc)
                    self.logger.info(f"Updated {len(transformed_data)} {interval} funding records")
                    
                    # Gửi thông báo cho các cập nhật đáng kể
                    if len(transformed_data) > 50:
                        self.tele_bot.send_message(
                            f"Funding Rate Update Complete\n"
                            f"Interval: {interval}\n"
                            f"Updated: {len(transformed_data)} symbols\n"
                            f"Time: {self.last_update_time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                        )
                else:
                    self.logger.error(f"Failed to update {interval} funding data")
            else:
                self.logger.warning(f"No transformed data for {interval} symbols")
                
        except Exception as e:
            self.logger.error(f"Error fetching and updating {interval} funding rates: {e}")
            traceback.print_exc()

    def get_status(self) -> Dict[str, Any]:
        """Lấy trạng thái trích xuất realtime

        Returns:
            Từ điển trạng thái
        """
        try:
            return {
                "is_running": self.is_running,
                "is_connected": True,  # REST API doesn't have persistent connection
                "symbols_count": len(self.symbols),
                "symbols_8h_count": len(self.symbols_8h),
                "symbols_4h_count": len(self.symbols_4h),
                "symbols": self.symbols[:10] if self.symbols else [],
                "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
                "scheduler_thread_alive": (
                    self.scheduler_thread.is_alive()
                    if self.scheduler_thread
                    else False
                ),
                "next_scheduled_jobs": len(schedule.jobs),
            }

        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {"error": str(e)}

    def _initial_update(self):
        """Kích hoạt cập nhật ban đầu cho chu kỳ funding gần nhất"""
        try:
            import time
            from datetime import datetime, timezone
            
            # Wait a moment for system to fully initialize
            time.sleep(3)
            
            current_time = datetime.now(timezone.utc)
            current_hour = current_time.hour
            
            self.logger.info(f"Triggering initial update at {current_hour}:00 UTC")
            
            # Determine which cycles to update based on current time
            # For 8h cycles: 0, 8, 16 - find the most recent one
            recent_8h_hours = [0, 8, 16]
            nearest_8h = max([h for h in recent_8h_hours if h <= current_hour], default=16)
            
            # For 4h cycles: 0, 4, 8, 12, 16, 20 - find the most recent one
            recent_4h_hours = [0, 4, 8, 12, 16, 20]
            nearest_4h = max([h for h in recent_4h_hours if h <= current_hour], default=20)
            
            self.logger.info(f"Current time: {current_hour}:00 UTC")
            self.logger.info(f"Nearest 8h cycle: {nearest_8h}:00 UTC")
            self.logger.info(f"Nearest 4h cycle: {nearest_4h}:00 UTC")
            
            # Update both 8h and 4h symbols for their respective nearest cycles
            if self.symbols_8h:
                self.logger.info(f"Updating {len(self.symbols_8h)} symbols for {nearest_8h}:00 cycle")
                self._update_8h_symbols()
                
            if self.symbols_4h:
                self.logger.info(f"Updating {len(self.symbols_4h)} symbols for {nearest_4h}:00 cycle")
                self._update_4h_symbols()
                
            self.logger.info("Initial update completed")
            
        except Exception as e:
            self.logger.error(f"Error in initial update: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get realtime extraction status
        
        Returns:
            Status dictionary
        """
        try:
            return {
                "is_running": self.is_running,
                "is_connected": True,  # REST API doesn't have persistent connection
                "symbols_count": len(self.symbols),
                "symbols_8h_count": len(self.symbols_8h),
                "symbols_4h_count": len(self.symbols_4h),
                "symbols": self.symbols[:10] if self.symbols else [],
                "last_update_time": self.last_update_time.isoformat() if self.last_update_time else None,
                "scheduler_thread_alive": (
                    self.scheduler_thread.is_alive()
                    if self.scheduler_thread
                    else False
                ),
                "next_scheduled_jobs": len(schedule.jobs),
            }

        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {"error": str(e)}
