"""
Funding Rate Scheduler with Data Verification and Intelligent Notifications
Scheduler thông minh cho funding rate với kiểm tra dữ liệu và thông báo có điều kiện
"""

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


class FundingRateScheduler:
    """
    Advanced scheduler for funding rate extraction with data verification
    Lập lịch thông minh với kiểm tra dữ liệu và thông báo có điều kiện
    """
    
    def __init__(self, symbols: List[str]):
        self.logger = ConfigLogging.config_logging("FundingRateScheduler")
        self.tele_bot = UtilTeleBotCheck()
        self.interval_detector = FundingIntervalDetector()
        self.extractor = ExtractFundingRateRealtime()
        self.load_mongo = LoadMongo()
        
        # Configuration
        self.symbols = symbols
        self.is_running = False
        self.scheduler_thread = None
        
        # Symbol categorization
        self.symbols_8h = []
        self.symbols_4h = []
        
        # Data verification settings
        self.verification_delay = 300  # 5 minutes after funding time
        self.max_data_age = 3600  # 1 hour maximum data age
        
        # Last execution tracking
        self.last_8h_execution = None
        self.last_4h_execution = None
        
    def start_scheduler(self) -> bool:
        """Start the funding rate scheduler"""
        try:
            if self.is_running:
                self.logger.warning("Scheduler already running")
                return True
            
            # Categorize symbols first
            self._categorize_symbols()
            
            # Setup schedules
            self._setup_funding_schedules()
            self._setup_verification_schedules()
            
            # Start scheduler thread
            self.is_running = True
            self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self.scheduler_thread.start()
            
            # Send startup notification
            self.tele_bot.send_alert(
                "Funding Rate Scheduler Started",
                f"Scheduler initialized successfully\\n"
                f"8h symbols: {len(self.symbols_8h)}\\n"
                f"4h symbols: {len(self.symbols_4h)}\\n"
                f"Total symbols: {len(self.symbols)}",
                "SUCCESS"
            )
            
            self.logger.info("Funding rate scheduler started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting scheduler: {e}")
            return False
    
    def stop_scheduler(self) -> bool:
        """Stop the funding rate scheduler"""
        try:
            if not self.is_running:
                self.logger.warning("Scheduler not running")
                return True
                
            self.logger.info("Stopping funding rate scheduler")
            self.is_running = False
            
            # Clear all schedules
            schedule.clear()
            
            # Wait for scheduler thread
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=10)
            
            self.logger.info("Funding rate scheduler stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping scheduler: {e}")
            return False
    
    def _categorize_symbols(self):
        """Categorize symbols by funding interval"""
        try:
            self.logger.info("Categorizing symbols by funding interval...")
            
            # Get intervals from detector
            intervals = self.interval_detector.detect_funding_intervals(self.symbols)
            
            # Reset lists
            self.symbols_8h = []
            self.symbols_4h = []
            
            # Categorize
            for symbol in self.symbols:
                interval = intervals.get(symbol, "8h")
                if interval == "4h":
                    self.symbols_4h.append(symbol)
                else:
                    self.symbols_8h.append(symbol)
            
            self.logger.info(f"Symbol categorization completed:")
            self.logger.info(f"  8h symbols: {len(self.symbols_8h)}")
            self.logger.info(f"  4h symbols: {len(self.symbols_4h)}")
            
        except Exception as e:
            self.logger.error(f"Error categorizing symbols: {e}")
            # Fallback: all to 8h
            self.symbols_8h = self.symbols.copy()
            self.symbols_4h = []
    
    def _setup_funding_schedules(self):
        """Setup funding rate extraction schedules"""
        try:
            # 8-hour funding schedules (00:00, 08:00, 16:00 UTC)
            if self.symbols_8h:
                schedule.every().day.at("00:00").do(self._execute_8h_funding)
                schedule.every().day.at("08:00").do(self._execute_8h_funding)
                schedule.every().day.at("16:00").do(self._execute_8h_funding)
                self.logger.info("8h funding schedules setup completed")
            
            # 4-hour funding schedules (00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC)
            if self.symbols_4h:
                schedule.every().day.at("00:00").do(self._execute_4h_funding)
                schedule.every().day.at("04:00").do(self._execute_4h_funding)
                schedule.every().day.at("08:00").do(self._execute_4h_funding)
                schedule.every().day.at("12:00").do(self._execute_4h_funding)
                schedule.every().day.at("16:00").do(self._execute_4h_funding)
                schedule.every().day.at("20:00").do(self._execute_4h_funding)
                self.logger.info("4h funding schedules setup completed")
                
        except Exception as e:
            self.logger.error(f"Error setting up funding schedules: {e}")
    
    def _setup_verification_schedules(self):
        """Setup data verification schedules (5 minutes after funding times)"""
        try:
            # 8h verification schedules
            if self.symbols_8h:
                schedule.every().day.at("00:05").do(self._verify_8h_data)
                schedule.every().day.at("08:05").do(self._verify_8h_data)
                schedule.every().day.at("16:05").do(self._verify_8h_data)
            
            # 4h verification schedules  
            if self.symbols_4h:
                schedule.every().day.at("00:05").do(self._verify_4h_data)
                schedule.every().day.at("04:05").do(self._verify_4h_data)
                schedule.every().day.at("08:05").do(self._verify_4h_data)
                schedule.every().day.at("12:05").do(self._verify_4h_data)
                schedule.every().day.at("16:05").do(self._verify_4h_data)
                schedule.every().day.at("20:05").do(self._verify_4h_data)
            
            self.logger.info("Data verification schedules setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up verification schedules: {e}")
    
    def _run_scheduler(self):
        """Main scheduler loop"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)
    
    def _execute_8h_funding(self):
        """Execute 8h funding rate extraction"""
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
            
            # Execute extraction
            start_time = time.time()
            result = self._extract_funding_data(self.symbols_8h, "8h")
            execution_time = time.time() - start_time
            
            # Send result notification
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
                f"Failed to execute 8h funding extraction\\n\\nError: {str(e)}",
                "ERROR"
            )
    
    def _execute_4h_funding(self):
        """Execute 4h funding rate extraction"""
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
            
            # Execute extraction
            start_time = time.time()
            result = self._extract_funding_data(self.symbols_4h, "4h")
            execution_time = time.time() - start_time
            
            # Send result notification
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
                f"Failed to execute 4h funding extraction\\n\\nError: {str(e)}",
                "ERROR"
            )
    
    def _extract_funding_data(self, symbols: List[str], interval: str) -> Dict[str, Any]:
        """Execute funding data extraction for given symbols
        
        Returns:
            Dict with extraction results
        """
        try:
            # Track which symbols succeeded and failed
            successful_symbols = []
            failed_symbols = []
            
            # Extract data in batches for better error handling
            batch_size = 20
            total_batches = (len(symbols) + batch_size - 1) // batch_size
            
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_symbols)} symbols)")
                
                try:
                    # Use the extractor's method for this batch
                    self.extractor._fetch_and_update_funding_rates(batch_symbols, interval)
                    
                    # If no exception, consider all batch symbols successful
                    successful_symbols.extend(batch_symbols)
                    
                except Exception as batch_error:
                    self.logger.error(f"Batch {batch_num} failed: {batch_error}")
                    failed_symbols.extend(batch_symbols)
                    
                    # Small delay between batches
                    time.sleep(1)
            
            success_count = len(successful_symbols)
            total_count = len(symbols)
            
            self.logger.info(f"Extraction completed: {success_count}/{total_count} symbols successful")
            
            if failed_symbols:
                self.logger.warning(f"Failed symbols: {failed_symbols[:5]}")
            
            return {
                "success_count": success_count,
                "total_count": total_count,
                "failed_symbols": failed_symbols,
                "successful_symbols": successful_symbols
            }
            
        except Exception as e:
            self.logger.error(f"Error extracting funding data: {e}")
            return {
                "success_count": 0,
                "total_count": len(symbols),
                "failed_symbols": symbols,
                "successful_symbols": []
            }
    
    def _verify_8h_data(self):
        """Verify 8h funding data was properly inserted/updated"""
        if not self.symbols_8h:
            return
            
        self._verify_funding_data(self.symbols_8h, "8h")
    
    def _verify_4h_data(self):
        """Verify 4h funding data was properly inserted/updated"""
        if not self.symbols_4h:
            return
            
        self._verify_funding_data(self.symbols_4h, "4h")
    
    def _verify_funding_data(self, symbols: List[str], interval: str):
        """Verify funding data for given symbols and interval"""
        try:
            self.logger.info(f"Verifying {interval} funding data for {len(symbols)} symbols")
            
            # Use LoadMongo to verify recent data
            verification_result = self.load_mongo.verify_recent_funding_data(
                "realtime",  # collection name
                symbols,
                self.max_data_age
            )
            
            missing_symbols = verification_result.get("missing_symbols", [])
            stale_symbols = verification_result.get("stale_symbols", [])
            success_rate = verification_result.get("success_rate", 0)
            
            # Combine missing and stale symbols as problematic
            problematic_symbols = missing_symbols + stale_symbols
            
            # Log results
            self.logger.info(f"{interval} data verification results:")
            self.logger.info(f"  Success rate: {success_rate:.1%}")
            self.logger.info(f"  Verified symbols: {verification_result.get('verified_symbols', 0)}/{len(symbols)}")
            
            if problematic_symbols:
                self.logger.warning(f"  Missing symbols: {len(missing_symbols)}")
                self.logger.warning(f"  Stale symbols: {len(stale_symbols)}")
                self.logger.warning(f"  Problematic symbols: {problematic_symbols[:5]}")
                
                # Send verification alert only if there are significant issues
                # (more than 5% failure rate or more than 5 symbols)
                if success_rate < 0.95 or len(problematic_symbols) > 5:
                    self.tele_bot.send_data_verification_alert(
                        interval,
                        problematic_symbols,
                        len(symbols),
                        verification_result.get("verified_symbols", 0)
                    )
                else:
                    self.logger.info(f"Minor verification issues (< 5%), no alert sent")
            else:
                self.logger.info(f"{interval} data verification passed completely")
                
        except Exception as e:
            self.logger.error(f"Error verifying {interval} funding data: {e}")
            # Send error alert
            self.tele_bot.send_alert(
                f"{interval.upper()} Data Verification Error",
                f"Failed to verify {interval} funding data\\n\\nError: {str(e)}",
                "ERROR"
            )
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status"""
        return {
            "is_running": self.is_running,
            "total_symbols": len(self.symbols),
            "symbols_8h": len(self.symbols_8h),
            "symbols_4h": len(self.symbols_4h),
            "last_8h_execution": self.last_8h_execution.isoformat() if self.last_8h_execution else None,
            "last_4h_execution": self.last_4h_execution.isoformat() if self.last_4h_execution else None,
            "scheduled_jobs": len(schedule.jobs)
        }