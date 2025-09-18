

import time
import schedule
import threading
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
import traceback

from src.config.config_logging import ConfigLogging
from src.utils.util_tele_bot_check import UtilTeleBotCheck
from src.utils.funding_interval_detector import FundingIntervalDetector
from src.extract.extract_realtime import ExtractFundingRateRealtime
from src.load.load_mongo import LoadMongo


class AdvancedFundingRateScheduler:
    """Bộ lập lịch nâng cao cho việc trích xuất tỷ lệ funding với hỗ trợ nhiều chu kỳ"""

    def __init__(self, symbols: List[str]):
        self.logger = ConfigLogging.config_logging("AdvancedFundingRateScheduler")
        self.tele_bot = UtilTeleBotCheck()
        self.interval_detector = FundingIntervalDetector()
        self.extractor = ExtractFundingRateRealtime()
        self.load_mongo = LoadMongo()
        
    # Cấu hình
        self.symbols = symbols
        self.is_running = False
        self.scheduler_thread = None
        
        self.symbols_1h = []   # 1-hour monitoring (high frequency)
        self.symbols_4h = []   # 4-hour funding
        self.symbols_8h = []   # Standard 8-hour funding
        
    # Cấu hình kiểm tra dữ liệu
        self.verification_delay = 300  # 5 minutes after funding time
        self.max_data_age_1h = 3900    # 65 minutes for 1h cycle
        self.max_data_age_4h = 14700   # 4h 5min for 4h cycle  
        self.max_data_age_8h = 29100   # 8h 5min for 8h cycle
        
        # Last execution tracking
        self.last_1h_execution = None
        self.last_4h_execution = None
        self.last_8h_execution = None
        
    def start_scheduler(self) -> bool:
        """Khởi động bộ lập lịch tỷ lệ funding"""
        try:
            if self.is_running:
                self.logger.warning("Scheduler already running")
                return True
            
            # Phân loại symbols trước
            self._categorize_symbols()
            
            # Thiết lập tất cả lịch
            self._setup_funding_schedules()
            self._setup_verification_schedules()
            
            # Start scheduler thread
            self.is_running = True
            self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self.scheduler_thread.start()
            
            # Send startup notification
            self.tele_bot.send_alert(
                "Advanced Funding Rate Scheduler Started",
                f"Multi-interval scheduler initialized successfully\n"
                f"1h monitoring: {len(self.symbols_1h)} symbols\n"
                f"4h funding: {len(self.symbols_4h)} symbols\n"
                f"8h funding: {len(self.symbols_8h)} symbols\n"
                f"Total symbols: {len(self.symbols)}",
                "SUCCESS"
            )
            
            self.logger.info("Advanced funding rate scheduler started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting scheduler: {e}")
            return False
    
    def stop_scheduler(self) -> bool:
        """Dừng bộ lập lịch tỷ lệ funding"""
        try:
            if not self.is_running:
                self.logger.warning("Scheduler not running")
                return True
                
            self.logger.info("Stopping advanced funding rate scheduler")
            self.is_running = False
            
            # Clear all schedules
            schedule.clear()
            
            # Wait for scheduler thread
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=10)
            
            self.logger.info("Advanced funding rate scheduler stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping scheduler: {e}")
            return False
    
    def _categorize_symbols(self):
        """Phân loại symbol theo chu kỳ funding (bao gồm giám sát 1h nếu có)"""
        try:
            self.logger.info("Categorizing symbols by funding interval...")
            
            # Get intervals from detector
            intervals = self.interval_detector.detect_funding_intervals(self.symbols)
            
            # Reset lists
            self.symbols_1h = []
            self.symbols_4h = []
            self.symbols_8h = []
            
            # Categorize symbols
            for symbol in self.symbols:
                interval = intervals.get(symbol, "8h")
                
                # Assign symbols based on their actual funding cycle
                if interval == "1h":
                    self.symbols_1h.append(symbol)
                elif interval == "4h":
                    self.symbols_4h.append(symbol)
                else:  # Default to 8h
                    self.symbols_8h.append(symbol)
            
            self.logger.info(f"Symbol categorization completed:")
            self.logger.info(f"  1h monitoring: {len(self.symbols_1h)} symbols")
            self.logger.info(f"  4h funding: {len(self.symbols_4h)} symbols")
            self.logger.info(f"  8h funding: {len(self.symbols_8h)} symbols")
            
        except Exception as e:
            self.logger.error(f"Error categorizing symbols: {e}")
            # Fallback: all to 8h with 1h monitoring
            self.symbols_1h = self.symbols.copy()
            self.symbols_8h = self.symbols.copy()
            self.symbols_4h = []
    
    def _setup_funding_schedules(self):
        """Thiết lập tất cả lịch trích xuất tỷ lệ funding"""
        try:
            # Lịch giám sát 1 giờ (mỗi giờ cho dữ liệu realtime)
            if self.symbols_1h:
                for hour in range(24):
                    schedule.every().day.at(f"{hour:02d}:00").do(self._execute_1h_monitoring)
                self.logger.info("1h monitoring schedules setup completed (24 schedules)")
            
            # Lịch trích xuất 4 giờ (00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC)
            if self.symbols_4h:
                schedule.every().day.at("00:00").do(self._execute_4h_funding)
                schedule.every().day.at("04:00").do(self._execute_4h_funding)
                schedule.every().day.at("08:00").do(self._execute_4h_funding)
                schedule.every().day.at("12:00").do(self._execute_4h_funding)
                schedule.every().day.at("16:00").do(self._execute_4h_funding)
                schedule.every().day.at("20:00").do(self._execute_4h_funding)
                self.logger.info("4h funding schedules setup completed")
            
            # Lịch trích xuất 8 giờ (00:00, 08:00, 16:00 UTC)
            if self.symbols_8h:
                schedule.every().day.at("00:00").do(self._execute_8h_funding)
                schedule.every().day.at("08:00").do(self._execute_8h_funding)
                schedule.every().day.at("16:00").do(self._execute_8h_funding)
                self.logger.info("8h funding schedules setup completed")
                
        except Exception as e:
            self.logger.error(f"Error setting up funding schedules: {e}")
    
    def _setup_verification_schedules(self):
        """Thiết lập lịch kiểm tra xác thực dữ liệu"""
        try:
            # Kiểm tra 1h (5 phút sau mỗi giờ)
            if self.symbols_1h:
                for hour in range(24):
                    schedule.every().day.at(f"{hour:02d}:05").do(self._verify_1h_data)
            
            # Lịch kiểm tra 4h
            if self.symbols_4h:
                schedule.every().day.at("00:05").do(self._verify_4h_data)
                schedule.every().day.at("04:05").do(self._verify_4h_data)
                schedule.every().day.at("08:05").do(self._verify_4h_data)
                schedule.every().day.at("12:05").do(self._verify_4h_data)
                schedule.every().day.at("16:05").do(self._verify_4h_data)
                schedule.every().day.at("20:05").do(self._verify_4h_data)
            
            # Lịch kiểm tra 8h
            if self.symbols_8h:
                schedule.every().day.at("00:05").do(self._verify_8h_data)
                schedule.every().day.at("08:05").do(self._verify_8h_data)
                schedule.every().day.at("16:05").do(self._verify_8h_data)
            
            self.logger.info("Data verification schedules setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up verification schedules: {e}")
    
    def _run_scheduler(self):
        """Vòng lặp chính của bộ lập lịch"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)
    
    def _execute_1h_monitoring(self):
        """Thực hiện giám sát 1 giờ cho dữ liệu realtime"""
        if not self.symbols_1h:
            return
            
        try:
            current_hour = datetime.now(timezone.utc).hour
            self.logger.info(f"Starting 1h monitoring for {len(self.symbols_1h)} symbols at {current_hour:02d}:00")
            
            # Gửi thông báo bắt đầu (chỉ vào các giờ chính để tránh spam)
            if current_hour % 4 == 0:  # Only at 0, 4, 8, 12, 16, 20
                self.tele_bot.send_funding_cycle_start(
                    "1h", 
                    len(self.symbols_1h),
                    f"{(current_hour + 1) % 24:02d}:00 UTC"
                )
            
            # Thực hiện trích xuất dữ liệu
            start_time = time.time()
            result = self._extract_funding_data(self.symbols_1h, "1h")
            execution_time = time.time() - start_time
            
            # Gửi thông báo kết quả (chỉ khi có vấn đề nghiêm trọng)
            if result["success_count"] < len(self.symbols_1h) * 0.9:  # < 90% success
                self.tele_bot.send_funding_update_result(
                    "1h",
                    result["success_count"],
                    result["total_count"], 
                    result["failed_symbols"],
                    execution_time
                )
            
            self.last_1h_execution = datetime.now(timezone.utc)
            
        except Exception as e:
            self.logger.error(f"Error in 1h monitoring execution: {e}")
            if datetime.now(timezone.utc).hour % 6 == 0:  # Alert every 6 hours
                self.tele_bot.send_alert(
                    "1H Monitoring Error",
                    f"Failed to execute 1h monitoring\n\nError: {str(e)}",
                    "ERROR"
                )
    
    def _execute_4h_funding(self):
        """Thực hiện trích xuất tỷ lệ funding chu kỳ 4 giờ"""
        if not self.symbols_4h:
            return
            
        try:
            self.logger.info(f"Starting 4h funding extraction for {len(self.symbols_4h)} symbols")
            
            # Send start notification
            current_time = datetime.now(timezone.utc)
            next_funding = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=4)
            
            self.tele_bot.send_funding_cycle_start(
                "4h", 
                len(self.symbols_4h),
                next_funding.strftime("%Y-%m-%d %H:%M UTC")
            )
            
            # Thực hiện trích xuất dữ liệu
            start_time = time.time()
            result = self._extract_funding_data(self.symbols_4h, "4h")
            execution_time = time.time() - start_time
            
            # Gửi thông báo kết quả
            self.tele_bot.send_funding_update_result(
                "4h",
                result["success_count"],
                result["total_count"],
                result["failed_symbols"], 
                execution_time
            )
            
            self.last_4h_execution = datetime.now(timezone.utc)
            
        except Exception as e:
            self.logger.error(f"Error in 4h funding execution: {e}")
            self.tele_bot.send_alert(
                "4H Funding Extraction Error", 
                f"Failed to execute 4h funding extraction\n\nError: {str(e)}",
                "ERROR"
            )
    
    def _execute_8h_funding(self):
        """Thực hiện trích xuất tỷ lệ funding chu kỳ 8 giờ"""
        if not self.symbols_8h:
            return
            
        try:
            self.logger.info(f"Starting 8h funding extraction for {len(self.symbols_8h)} symbols")
            
            # Send start notification
            current_time = datetime.now(timezone.utc)
            next_funding = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=8)
            
            self.tele_bot.send_funding_cycle_start(
                "8h", 
                len(self.symbols_8h),
                next_funding.strftime("%Y-%m-%d %H:%M UTC")
            )
            
            # Thực hiện trích xuất dữ liệu
            start_time = time.time()
            result = self._extract_funding_data(self.symbols_8h, "8h")
            execution_time = time.time() - start_time
            
            # Gửi thông báo kết quả
            self.tele_bot.send_funding_update_result(
                "8h",
                result["success_count"],
                result["total_count"], 
                result["failed_symbols"],
                execution_time
            )
            
            self.last_8h_execution = datetime.now(timezone.utc)
            
        except Exception as e:
            self.logger.error(f"Error in 8h funding execution: {e}")
            self.tele_bot.send_alert(
                "8H Funding Extraction Error",
                f"Failed to execute 8h funding extraction\n\nError: {str(e)}",
                "ERROR"
            )
    
    def _extract_funding_data(self, symbols: List[str], interval: str) -> Dict[str, Any]:
        """Thực hiện trích xuất dữ liệu funding cho các symbol đã cho"""
        try:
            # Track which symbols succeeded and failed
            # Theo dõi symbol thành công và thất bại
            successful_symbols = []
            failed_symbols = []
            
            # Trích xuất theo batch để xử lý lỗi tốt hơn
            batch_size = 50 if interval == "1h" else 20  # Batch lớn hơn cho 1h
            total_batches = (len(symbols) + batch_size - 1) // batch_size
            
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                if interval != "1h" or batch_num == 1:  # Giảm logging cho 1h
                    self.logger.info(f"Processing {interval} batch {batch_num}/{total_batches} ({len(batch_symbols)} symbols)")
                
                try:
                    # Gọi phương thức extractor cho batch này
                    self.extractor._fetch_and_update_funding_rates(batch_symbols, interval)
                    
                    # If no exception, consider all batch symbols successful
                    successful_symbols.extend(batch_symbols)
                    
                except Exception as batch_error:
                    self.logger.error(f"{interval} batch {batch_num} failed: {batch_error}")
                    failed_symbols.extend(batch_symbols)
                    
                    # Đệm nhỏ giữa các batch
                    time.sleep(0.5 if interval == "1h" else 1)
            
            success_count = len(successful_symbols)
            total_count = len(symbols)
            
            if interval != "1h" or success_count < total_count * 0.9:  # Chỉ log vấn đề cho 1h
                self.logger.info(f"{interval} extraction completed: {success_count}/{total_count} symbols successful")
            
            return {
                "success_count": success_count,
                "total_count": total_count,
                "failed_symbols": failed_symbols,
                "successful_symbols": successful_symbols
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting {interval} funding data: {e}")
            return {
                "success_count": 0,
                "total_count": len(symbols),
                "failed_symbols": symbols,
                "successful_symbols": []
            }
    
    def _verify_1h_data(self):
        """Xác thực dữ liệu giám sát 1 giờ"""
        if not self.symbols_1h:
            return
        self._verify_funding_data(self.symbols_1h, "1h", self.max_data_age_1h)
    
    def _verify_4h_data(self):
        """Xác thực dữ liệu funding 4 giờ"""
        if not self.symbols_4h:
            return
        self._verify_funding_data(self.symbols_4h, "4h", self.max_data_age_4h)
    
    def _verify_8h_data(self):
        """Xác thực dữ liệu funding 8 giờ"""
        if not self.symbols_8h:
            return
        self._verify_funding_data(self.symbols_8h, "8h", self.max_data_age_8h)
    
    def _verify_funding_data(self, symbols: List[str], interval: str, max_age: int):
        """Xác thực dữ liệu funding cho các symbol và khoảng thời gian cho trước"""
        try:
            if interval != "1h":  # Giảm logging cho kiểm tra 1h
                self.logger.info(f"Xác thực dữ liệu {interval} cho {len(symbols)} symbols")
            
            # Dùng LoadMongo để xác minh dữ liệu gần đây
            verification_result = self.load_mongo.verify_recent_funding_data(
                "realtime",
                symbols,
                max_age
            )
            
            missing_symbols = verification_result.get("missing_symbols", [])
            stale_symbols = verification_result.get("stale_symbols", [])
            success_rate = verification_result.get("success_rate", 0)
            
            # Kết hợp các symbol thiếu và cũ lại thành danh sách vấn đề
            problematic_symbols = missing_symbols + stale_symbols
            
            # Ngưỡng khác nhau cho từng khoảng thời gian
            alert_threshold = 0.85 if interval == "1h" else 0.95  # Ngưỡng thấp hơn cho 1h
            alert_count_threshold = 20 if interval == "1h" else 5
            
            if interval != "1h":
                self.logger.info(f"{interval} data verification results:")
                self.logger.info(f"  Success rate: {success_rate:.1%}")
                self.logger.info(f"  Verified symbols: {verification_result.get('verified_symbols', 0)}/{len(symbols)}")
            
            if problematic_symbols:
                if success_rate < alert_threshold or len(problematic_symbols) > alert_count_threshold:
                    self.tele_bot.send_data_verification_alert(
                        interval,
                        problematic_symbols,
                        len(symbols),
                        verification_result.get("verified_symbols", 0)
                    )
                elif interval != "1h":
                    self.logger.info(f"Minor {interval} verification issues, no alert sent")
            elif interval != "1h":
                self.logger.info(f"{interval} data verification passed completely")
                
        except Exception as e:
            self.logger.error(f"Error verifying {interval} funding data: {e}")
            # Only send error alerts for important intervals
            if interval != "1h" or datetime.now(timezone.utc).hour % 6 == 0:
                self.tele_bot.send_alert(
                    f"{interval.upper()} Data Verification Error",
                    f"Failed to verify {interval} funding data\n\nError: {str(e)}",
                    "ERROR"
                )
    
    def get_status(self) -> Dict[str, Any]:
        """Lấy trạng thái bộ lập lịch"""
        return {
            "is_running": self.is_running,
            "total_symbols": len(self.symbols),
            "symbols_1h": len(self.symbols_1h),
            "symbols_4h": len(self.symbols_4h), 
            "symbols_8h": len(self.symbols_8h),
            "last_1h_execution": self.last_1h_execution.isoformat() if self.last_1h_execution else None,
            "last_4h_execution": self.last_4h_execution.isoformat() if self.last_4h_execution else None,
            "last_8h_execution": self.last_8h_execution.isoformat() if self.last_8h_execution else None,
            "scheduled_jobs": len(schedule.jobs)
        }