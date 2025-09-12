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


class ExtractFundingRateRealtime:
    """Extract funding rate data from Binance REST API with scheduled updates"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("ExtractFundingRateRealtime")
        self.load_mongo = LoadMongo()
        self.transform_funding = TransformFundingData()
        self.tele_bot = UtilTeleBotCheck()

        # Configuration
        self.config = REALTIME_CONFIG
        self.base_url = "https://fapi.binance.com"
        
        # State management
        self.is_running = False
        self.symbols = []
        self.scheduler_thread = None
        self.last_update_time = None
        
        # Symbols with different funding intervals
        self.symbols_8h = []  # Standard 8-hour funding
        self.symbols_4h = []  # 4-hour funding

    def start_realtime_extraction(self, symbols: List[str]) -> bool:
        """Start scheduled funding rate extraction
        
        Args:
            symbols: List of symbols to monitor
            
        Returns:
            True if started successfully, False otherwise
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
            
            # Categorize symbols by funding frequency
            self._categorize_symbols_by_funding_frequency()
            
            self.logger.info(
                f"Starting scheduled extraction for {len(self.symbols)} symbols"
            )
            self.logger.info(f"8-hour funding symbols: {len(self.symbols_8h)}")
            self.logger.info(f"4-hour funding symbols: {len(self.symbols_4h)}")

            # Setup schedules
            self._setup_schedules()
            
            # Start scheduler thread
            self.scheduler_thread = threading.Thread(
                target=self._run_scheduler, daemon=True
            )
            self.scheduler_thread.start()

            # Trigger initial update for nearest funding cycle
            threading.Thread(target=self._initial_update, daemon=True).start()

            # Send initial notification
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
        """Stop realtime extraction
        
        Returns:
            True if stopped successfully, False otherwise
        """
        try:
            if not self.is_running:
                self.logger.warning("Realtime extraction not running")
                return True

            self.logger.info("Stopping realtime extraction")
            self.is_running = False
            
            # Clear schedules
            schedule.clear()
            
            # Wait for scheduler thread to finish
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=10)

            self.logger.info("Realtime extraction stopped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error stopping realtime extraction: {e}")
            return False

    def _categorize_symbols_by_funding_frequency(self):
        """Categorize symbols by their funding frequency (4h vs 8h)"""
        try:
            # Get funding info for all symbols
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Create symbol mapping
                symbol_funding_info = {}
                for item in data:
                    if item['symbol'] in self.symbols:
                        # Get next funding time to determine frequency
                        next_funding = int(item.get('nextFundingTime', 0))
                        symbol_funding_info[item['symbol']] = next_funding
                
                # Categorize based on funding frequency patterns
                # Most symbols have 8h funding (0h, 8h, 16h UTC)
                # Some have 4h funding (0h, 4h, 8h, 12h, 16h, 20h UTC)
                current_time = int(time.time() * 1000)
                
                for symbol in self.symbols:
                    if symbol in symbol_funding_info:
                        next_funding = symbol_funding_info[symbol]
                        time_to_next = (next_funding - current_time) / (1000 * 60 * 60)  # hours
                        
                        # Check if time to next funding suggests 4h or 8h interval
                        # This is a heuristic - in practice you might need to track patterns
                        if time_to_next <= 4:
                            self.symbols_4h.append(symbol)
                        else:
                            self.symbols_8h.append(symbol)
                    else:
                        # Default to 8h if no info available
                        self.symbols_8h.append(symbol)
                        
            else:
                # If API call fails, default all to 8h
                self.symbols_8h = self.symbols.copy()
                self.logger.warning("Failed to categorize symbols, defaulting all to 8h")
                
        except Exception as e:
            self.logger.error(f"Error categorizing symbols: {e}")
            # Default all to 8h on error
            self.symbols_8h = self.symbols.copy()

    def _setup_schedules(self):
        """Setup scheduled jobs for funding rate updates"""
        try:
            # Schedule 8-hour updates at 0h, 8h, 16h UTC
            schedule.every().day.at("00:00").do(self._update_8h_symbols)
            schedule.every().day.at("08:00").do(self._update_8h_symbols)
            schedule.every().day.at("16:00").do(self._update_8h_symbols)
            
            # Schedule 4-hour updates at 0h, 4h, 8h, 12h, 16h, 20h UTC
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
        """Run the scheduler loop"""
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)

    def _update_8h_symbols(self):
        """Update funding rates for 8-hour symbols"""
        if not self.is_running or not self.symbols_8h:
            return
            
        try:
            self.logger.info(f"Updating {len(self.symbols_8h)} symbols with 8-hour funding")
            self._fetch_and_update_funding_rates(self.symbols_8h, "8h")
        except Exception as e:
            self.logger.error(f"Error updating 8h symbols: {e}")

    def _update_4h_symbols(self):
        """Update funding rates for 4-hour symbols"""
        if not self.is_running or not self.symbols_4h:
            return
            
        try:
            self.logger.info(f"Updating {len(self.symbols_4h)} symbols with 4-hour funding")
            self._fetch_and_update_funding_rates(self.symbols_4h, "4h")
        except Exception as e:
            self.logger.error(f"Error updating 4h symbols: {e}")

    def _fetch_and_update_funding_rates(self, symbols: List[str], interval: str):
        """Fetch and update funding rates for given symbols
        
        Args:
            symbols: List of symbols to update
            interval: Funding interval (4h or 8h)
        """
        try:
            # Fetch current funding rate data
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"API request failed with status {response.status_code}")
                return
                
            data = response.json()
            
            # Filter data for our symbols
            filtered_data = []
            for item in data:
                if item['symbol'] in symbols:
                    # Transform API response to our format
                    funding_data = {
                        'symbol': item['symbol'],
                        'interval': interval,
                        'time_to_next_funding': item.get('nextFundingTime', 0),
                        'funding_rate': float(item.get('lastFundingRate', 0)),
                        'interest_rate': float(item.get('interestRate', 0)),
                        'mark_price': float(item.get('markPrice', 0)),
                        'index_price': float(item.get('indexPrice', 0)),
                        'estimated_settle_price': float(item.get('estimatedSettlePrice', 0)),
                        'funding_cap': 0.005,  # Standard Binance funding cap
                        'funding_floor': -0.005,  # Standard Binance funding floor
                        'last_update_time': int(time.time() * 1000)
                    }
                    filtered_data.append(funding_data)
            
            if not filtered_data:
                self.logger.warning(f"No data received for {interval} symbols")
                return
                
            # Transform data
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
                    
                    # Send notification for significant updates
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

    def _initial_update(self):
        """Trigger initial update for the nearest funding cycle"""
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
