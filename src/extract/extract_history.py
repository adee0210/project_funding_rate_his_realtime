import requests
import time
import threading
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from src.config.config_logging import ConfigLogging
from src.load.load_mongo import LoadMongo
from src.utils.util_convert_datetime import UtilConvertDatetime
from src.transform.transform_funding import TransformFundingData


class ExtractFundingRateHistory:
    """Trích xuất lịch sử tỷ lệ funding từ API Binance"""

    BASE_URL = "https://fapi.binance.com"

    # Các symbol đã biết gây lỗi 403 (đã bị delisted hoặc bị hạn chế)
    BLACKLISTED_SYMBOLS = {"WAVESUSDT", "LUNAUSDT", "USTUSDT", "TERRAUSDT", "ANCUSDT"}

    def __init__(self):
        self.logger = ConfigLogging.config_logging("ExtractFundingRateHistory")
        self.load_mongo = LoadMongo()
        self.util_datetime = UtilConvertDatetime()
        self.transform = TransformFundingData()
        self.session = requests.Session()
        # Rate limiting: global lock and timing
        self._request_lock = threading.Lock()
        self._last_request_time = 0
        self._min_request_interval = 1.5  # Minimum 1.5 seconds between API requests

        # Cache cho auto-detected start times
        self._symbol_start_times = {}
        self._cache_file = "symbol_start_times_cache.json"
        self._load_start_times_cache()

    def get_top_symbols(self, limit: int = 100) -> List[str]:
        """Lấy các symbol giao dịch hàng đầu theo khối lượng 24h

        Args:
            limit: Số lượng symbol trả về

        Returns:
            Danh sách tên symbol
        """
        try:
            url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()

            # Lọc các hợp đồng perpetual USDT và sắp xếp theo khối lượng
            usdt_symbols = [
                item
                for item in data
                if item["symbol"].endswith("USDT")
                and float(item["quoteVolume"]) > 0
                and item["symbol"]
                not in self.BLACKLISTED_SYMBOLS  # Lọc các symbol bị blacklist
            ]

            # Sắp xếp theo khối lượng quote 24h (giảm dần)
            usdt_symbols.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)

            symbols = [item["symbol"] for item in usdt_symbols[:limit]]
            self.logger.info(
                f"Retrieved top {len(symbols)} symbols (filtered {len(self.BLACKLISTED_SYMBOLS)} blacklisted)"
            )

            return symbols

        except Exception as e:
            self.logger.error(f"Error getting top symbols: {e}")
            return []

    def _rate_limited_request(
        self, url: str, params: Dict[str, Any] = None
    ) -> requests.Response:
        """Thực hiện yêu cầu có giới hạn tốc độ để tránh lỗi 403

        Args:
            url: URL để yêu cầu
            params: Tham số yêu cầu

        Returns:
            Đối tượng Response
        """
        with self._request_lock:
            # Tính thời gian kể từ yêu cầu cuối cùng
            current_time = time.time()
            time_since_last = current_time - self._last_request_time

            # Chờ nếu cần để duy trì khoảng thời gian tối thiểu
            if time_since_last < self._min_request_interval:
                sleep_time = self._min_request_interval - time_since_last
                self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f} seconds")
                time.sleep(sleep_time)

            # Thực hiện yêu cầu
            response = self.session.get(url, params=params, timeout=30)
            self._last_request_time = time.time()

            return response

    def _find_symbol_start_time(self, symbol: str) -> int:
        """Tự động tìm thời điểm bắt đầu có dữ liệu cho một symbol

        Args:
            symbol: Symbol để tìm

        Returns:
            Timestamp bắt đầu (milliseconds)
        """
        try:
            # Kiểm tra cache trước
            if symbol in self._symbol_start_times:
                cached_time = self._symbol_start_times[symbol]
                self.logger.info(
                    f"Using cached start time for {symbol}: {datetime.fromtimestamp(cached_time / 1000)}"
                )
                return cached_time

            # Bắt đầu từ thời điểm sớm nhất có thể (tháng 1/2017)
            earliest_possible = int(datetime(2017, 1, 1).timestamp() * 1000)
            current_time = self.util_datetime.get_current_timestamp()

            # Binary search để tìm thời điểm bắt đầu
            left = earliest_possible
            right = current_time
            found_start = None

            self.logger.info(f"Auto-detecting start time for {symbol}...")

            # Thử với các khoảng thời gian lớn trước để tìm khoảng gần đúng
            test_periods = [
                (2017, 1, 1),  # Futures bắt đầu
                (2019, 9, 1),  # BTC futures chính thức
                (2020, 1, 1),  # Nhiều altcoin
                (2021, 1, 1),  # Bull market
                (2022, 1, 1),  # Bear market
                (2023, 1, 1),  # Recent
            ]

            earliest_with_data = None
            for year, month, day in test_periods:
                test_timestamp = int(datetime(year, month, day).timestamp() * 1000)
                if test_timestamp > current_time:
                    continue

                # Thử lấy 10 records từ timestamp này
                test_data = self.get_funding_rate_history(
                    symbol, start_time=test_timestamp, limit=10
                )

                if test_data:
                    earliest_with_data = test_timestamp
                    self.logger.info(
                        f"Found data for {symbol} starting from {datetime.fromtimestamp(test_timestamp / 1000)}"
                    )
                    break

            # Nếu tìm thấy data, thử tìm chính xác hơn bằng binary search
            if earliest_with_data:
                # Binary search trong khoảng từ earliest_possible đến earliest_with_data
                left = earliest_possible
                right = earliest_with_data

                # Chỉ binary search nếu khoảng cách đủ lớn (> 30 ngày)
                if (right - left) > (30 * 24 * 60 * 60 * 1000):
                    search_count = 0
                    max_searches = 10  # Giới hạn số lần tìm kiếm

                    while left < right and search_count < max_searches:
                        search_count += 1
                        mid = (left + right) // 2

                        # Thử lấy data từ mid
                        mid_data = self.get_funding_rate_history(
                            symbol, start_time=mid, limit=5
                        )

                        if mid_data:
                            # Có data, thử tìm sớm hơn
                            right = mid
                            found_start = mid
                        else:
                            # Không có data, tìm muộn hơn
                            left = mid + 1

                        # Rate limiting cho binary search
                        time.sleep(0.5)

                # Sử dụng kết quả tìm được hoặc earliest_with_data
                found_start = found_start or earliest_with_data

            # Nếu không tìm thấy, dùng mặc định
            if found_start is None:
                found_start = int(datetime(2019, 9, 1).timestamp() * 1000)
                self.logger.warning(
                    f"Could not auto-detect start time for {symbol}, using default: 2019-09-01"
                )
            else:
                start_date = datetime.fromtimestamp(found_start / 1000)
                self.logger.info(f"Auto-detected start time for {symbol}: {start_date}")

            # Cache kết quả
            self._symbol_start_times[symbol] = found_start
            self._save_start_times_cache()
            return found_start

        except Exception as e:
            self.logger.error(f"Error finding start time for {symbol}: {e}")
            # Fallback to default
            default_time = int(datetime(2019, 9, 1).timestamp() * 1000)
            self._symbol_start_times[symbol] = default_time
            return default_time

    def _load_start_times_cache(self):
        """Load cache của start times từ file"""
        try:
            if os.path.exists(self._cache_file):
                with open(self._cache_file, "r") as f:
                    self._symbol_start_times = json.load(f)
                self.logger.info(
                    f"Loaded {len(self._symbol_start_times)} cached start times"
                )
            else:
                self.logger.info("No start times cache file found, will create new one")
        except Exception as e:
            self.logger.warning(f"Error loading start times cache: {e}")
            self._symbol_start_times = {}

    def _save_start_times_cache(self):
        """Save cache của start times vào file"""
        try:
            with open(self._cache_file, "w") as f:
                json.dump(self._symbol_start_times, f, indent=2)
            self.logger.debug(
                f"Saved {len(self._symbol_start_times)} start times to cache"
            )
        except Exception as e:
            self.logger.warning(f"Error saving start times cache: {e}")

    def get_funding_rate_history(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Lấy lịch sử tỷ lệ funding cho một symbol

        Args:
            symbol: Symbol giao dịch
            start_time: Timestamp bắt đầu (milliseconds)
            end_time: Timestamp kết thúc (milliseconds)
            limit: Số lượng bản ghi lấy (tối đa 1000)

        Returns:
            Danh sách bản ghi tỷ lệ funding
        """
        try:
            url = f"{self.BASE_URL}/fapi/v1/fundingRate"
            params = {"symbol": symbol, "limit": limit}

            # Xác thực timestamps
            current_time = self.util_datetime.get_current_timestamp()

            if start_time:
                # Đảm bảo start_time không ở tương lai
                if start_time > current_time:
                    self.logger.warning(
                        f"Start time is in the future for {symbol}, using current time"
                    )
                    start_time = current_time
                params["startTime"] = start_time

            if end_time:
                # Đảm bảo end_time không ở tương lai
                if end_time > current_time:
                    self.logger.warning(
                        f"End time is in the future for {symbol}, using current time"
                    )
                    end_time = current_time
                params["endTime"] = end_time

            response = self._rate_limited_request(url, params)
            response.raise_for_status()

            data = response.json()

            # Biến đổi dữ liệu
            funding_rates = []
            for item in data:
                try:
                    # Xử lý markPrice có thể là chuỗi rỗng cho dữ liệu cũ
                    mark_price = item.get("markPrice", "0")
                    if mark_price == "" or mark_price is None:
                        mark_price = "0"

                    funding_rate = {
                        "symbol": item["symbol"],
                        "fundingTime": int(item["fundingTime"]),
                        "fundingRate": float(item["fundingRate"]),
                        "markPrice": float(mark_price),
                        "created_at": self.util_datetime.get_current_timestamp(),
                    }
                    funding_rates.append(funding_rate)
                except (ValueError, KeyError) as e:
                    self.logger.warning(
                        f"Skipping invalid record for {symbol}: {item} - Error: {e}"
                    )
                    continue

            self.logger.debug(
                f"Retrieved {len(funding_rates)} funding rate records for {symbol}"
            )
            return funding_rates

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                self.logger.warning(
                    f"Symbol {symbol} is forbidden (403) - adding to blacklist"
                )
                self.BLACKLISTED_SYMBOLS.add(
                    symbol
                )  # Add to blacklist to avoid future attempts
            elif e.response.status_code == 429:
                self.logger.warning(f"Rate limit exceeded for {symbol}, waiting...")
                time.sleep(5)
            else:
                self.logger.error(
                    f"HTTP error getting funding rate history for {symbol}: {e}"
                )
            return []
        except requests.exceptions.RequestException as e:
            self.logger.error(
                f"Network error getting funding rate history for {symbol}: {e}"
            )
            return []
        except Exception as e:
            self.logger.error(f"Error getting funding rate history for {symbol}: {e}")
            return []

    def extract_all_history(self, symbols: List[str], days_back: int = 30) -> bool:
        """Extract funding rate history for all symbols using multiple threads

        Args:
            symbols: List of symbols to extract
            days_back: Number of days to go back in history (only for first time)

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(
                f"Starting funding rate history extraction for {len(symbols)} symbols"
            )

            success_count = 0
            max_workers = 3  # Limit concurrent threads to avoid overwhelming API

            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all symbol processing tasks
                future_to_symbol = {
                    executor.submit(
                        self._process_single_symbol, symbol, i, len(symbols)
                    ): symbol
                    for i, symbol in enumerate(symbols)
                }

                # Process completed tasks as they finish
                for future in as_completed(future_to_symbol):
                    symbol = future_to_symbol[future]
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                        else:
                            self.logger.warning(f"Failed to process {symbol}")
                    except Exception as e:
                        self.logger.error(f"Exception processing {symbol}: {e}")

            self.logger.info(
                f"Completed funding rate history extraction. Success: {success_count}/{len(symbols)}"
            )
            return success_count > 0

        except Exception as e:
            self.logger.error(f"Error in extract_all_history: {e}")
            return False

    def _process_single_symbol(
        self, symbol: str, symbol_index: int, total_symbols: int
    ) -> bool:
        """Process a single symbol for history extraction

        Args:
            symbol: Symbol to process
            symbol_index: Index of symbol (for logging)
            total_symbols: Total number of symbols

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Processing {symbol} ({symbol_index+1}/{total_symbols})")

            # Check if this is first time extraction (no data exists)
            is_first_time = not self.load_mongo.has_funding_data(symbol)

            if is_first_time:
                # Lần đầu: trích xuất TOÀN BỘ lịch sử từ thời điểm bắt đầu
                self.logger.info(
                    f"First time extraction for {symbol} - retrieving FULL history from start"
                )

                # Phương pháp mới: Tự động phát hiện thời điểm bắt đầu cho từng coin
                all_data = []

                # Tìm thời điểm bắt đầu thực tế của coin
                actual_start_timestamp = self._find_symbol_start_time(symbol)
                current_start = actual_start_timestamp
                chunk_count = 0
                max_chunks = 200  # Cho phép nhiều chunk để lấy toàn bộ lịch sử

                start_date = datetime.fromtimestamp(actual_start_timestamp / 1000)
                self.logger.info(
                    f"Starting full history extraction for {symbol} from {start_date} (auto-detected)"
                )

                while True:
                    chunk_count += 1
                    self.logger.info(
                        f"Retrieving history chunk {chunk_count} for {symbol}"
                    )

                    # Lấy dữ liệu từ current_start tiến về phía trước (1000 records mỗi lần)
                    chunk_data = self.get_funding_rate_history(
                        symbol, start_time=current_start, limit=1000
                    )

                    if not chunk_data:
                        self.logger.info(f"No more data for {symbol}")
                        break

                    # Thêm tất cả records từ chunk này
                    all_data.extend(chunk_data)
                    self.logger.info(
                        f"Retrieved {len(chunk_data)} records for {symbol}"
                    )

                    # Lấy timestamp mới nhất từ chunk này cho lần lặp tiếp theo
                    latest_timestamp = max(item["fundingTime"] for item in chunk_data)
                    latest_date = datetime.fromtimestamp(latest_timestamp / 1000)
                    self.logger.info(f"Latest timestamp in chunk: {latest_date}")

                    # Đặt thời gian bắt đầu tiếp theo là 1ms sau timestamp mới nhất
                    current_start = latest_timestamp + 1

                    # Nếu lấy được ít hơn 1000 records, đã đến cuối
                    if len(chunk_data) < 1000:
                        self.logger.info(f"Reached end of available data for {symbol}")
                        break

                    # An toàn: giới hạn số chunk tối đa để tránh vòng lặp vô tận
                    if chunk_count >= max_chunks:
                        self.logger.warning(
                            f"Reached maximum chunk limit ({max_chunks}) for {symbol}"
                        )
                        break

                if all_data:
                    # Sắp xếp dữ liệu theo timestamp để đảm bảo thứ tự thời gian
                    all_data.sort(key=lambda x: x["fundingTime"])

                    first_date = datetime.fromtimestamp(
                        min(item["fundingTime"] for item in all_data) / 1000
                    )
                    last_date = datetime.fromtimestamp(
                        max(item["fundingTime"] for item in all_data) / 1000
                    )

                    self.logger.info(
                        f"Full history for {symbol}: {len(all_data)} records from {first_date} to {last_date}"
                    )

                    if self.transform_and_save_data(all_data, symbol):
                        self.logger.info(
                            f"Successfully processed {len(all_data)} records for {symbol} (full history)"
                        )
                        return True
                    else:
                        self.logger.error(f"Error processing data for {symbol}")
                        return False
                else:
                    self.logger.info(f"No historical data found for {symbol}")
                    return True

            else:
                # Các lần sau: chỉ lấy dữ liệu mới nhất (8 giờ qua)
                self.logger.info(
                    f"Incremental extraction for {symbol} - retrieving latest 8 hours"
                )
                latest_time = self.load_mongo.get_latest_funding_time(symbol)

                # Lấy dữ liệu từ thời gian funding cuối + 1ms đến hiện tại
                end_time = self.util_datetime.get_current_timestamp()
                start_time = (
                    latest_time + 1 if latest_time else end_time - (8 * 60 * 60 * 1000)
                )

                if start_time >= end_time:
                    self.logger.info(f"Skipping {symbol} - already up to date")
                    return True

                # Lấy dữ liệu mới nhất
                data = self.get_funding_rate_history(symbol, start_time, end_time, 10)

                if data:
                    if self.transform_and_save_data(data, symbol):
                        self.logger.info(
                            f"Successfully processed {len(data)} new records for {symbol}"
                        )
                        return True
                    else:
                        self.logger.error(f"Error processing data for {symbol}")
                        return False
                else:
                    self.logger.info(f"No new data for {symbol}")
                    return True

        except Exception as e:
            self.logger.error(f"Error processing {symbol}: {e}")
            return False

    def extract_recent_history(self, symbols: List[str]) -> bool:
        """Extract recent funding rate history (last 24 hours)

        Args:
            symbols: List of symbols to extract

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(
                f"Extracting recent funding rate history for {len(symbols)} symbols"
            )

            # Get last 24 hours
            end_time = self.util_datetime.get_current_timestamp()
            start_time = end_time - (24 * 60 * 60 * 1000)

            success_count = 0

            for symbol in symbols:
                try:
                    data = self.get_funding_rate_history(
                        symbol, start_time, end_time, 100
                    )

                    if data:
                        if self.transform_and_save_data(data, symbol):
                            success_count += 1

                    time.sleep(0.1)  # Rate limiting

                except Exception as e:
                    self.logger.error(
                        f"Error processing recent history for {symbol}: {e}"
                    )
                    continue

            self.logger.info(
                f"Recent history extraction completed. Success: {success_count}/{len(symbols)}"
            )
            return success_count > 0

        except Exception as e:
            self.logger.error(f"Error in extract_recent_history: {e}")
            return False

    def run(self, symbols: List[str] = None, days_back: int = 365) -> bool:
        """Run the complete funding rate history extraction process

        Args:
            symbols: List of symbols to extract. If None, get top 100 symbols
            days_back: Number of days to go back in history

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Starting funding rate history extraction process")

            if symbols is None:
                self.logger.info("Getting top 100 symbols by volume")
                symbols = self.get_top_symbols(limit=100)

            if not symbols:
                self.logger.error("No symbols to process")
                return False

            self.logger.info(
                f"Processing {len(symbols)} symbols: {', '.join(symbols[:5])}..."
            )

            # Extract all history
            success = self.extract_all_history(symbols, days_back)

            if success:
                self.logger.info(
                    "Funding rate history extraction completed successfully"
                )
            else:
                self.logger.error("Funding rate history extraction failed")

            return success

        except Exception as e:
            self.logger.error(f"Error in run method: {e}")
            return False

    def stop(self) -> bool:
        """Stop the extraction process

        Returns:
            True if stopped successfully, False otherwise
        """
        try:
            self.logger.info("Stopping funding rate history extraction")
            # Close session
            if hasattr(self, "session"):
                self.session.close()
            self.logger.info("Extraction stopped successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error stopping extraction: {e}")
            return False

    def test(self, symbol: str = "BTCUSDT", limit: int = 10) -> bool:
        """Test the extraction functionality with a small dataset

        Args:
            symbol: Symbol to test with
            limit: Number of records to fetch

        Returns:
            True if test successful, False otherwise
        """
        try:
            self.logger.info(f"Testing extraction with {symbol}")

            # Test getting funding rate history
            end_time = self.util_datetime.get_current_timestamp()
            start_time = end_time - (24 * 60 * 60 * 1000)  # 24 hours ago

            test_data = self.get_funding_rate_history(
                symbol, start_time, end_time, limit
            )

            if test_data:
                self.logger.info(
                    f"Test successful! Retrieved {len(test_data)} records for {symbol}"
                )
                # Log sample data
                for record in test_data[:3]:
                    self.logger.info(f"Sample: {record}")
                return True
            else:
                self.logger.error(f"Test failed! No data retrieved for {symbol}")
                return False

        except Exception as e:
            self.logger.error(f"Error in test method: {e}")
            return False

    def transform_and_save_data(
        self, raw_data: List[Dict[str, Any]], symbol: str
    ) -> bool:
        """Transform raw data and save to MongoDB

        Args:
            raw_data: Raw funding rate data
            symbol: Trading symbol

        Returns:
            True if successful, False otherwise
        """
        try:
            if not raw_data:
                self.logger.warning(f"No data to transform for {symbol}")
                return False

            # Transform data
            transformed_data = self.transform.transform_funding_data(raw_data)

            if not transformed_data:
                self.logger.error(f"Failed to transform data for {symbol}")
                return False

            # Save transformed data to MongoDB
            if self.load_mongo.save_transformed_funding_data(transformed_data):
                self.logger.info(
                    f"Successfully saved {len(transformed_data)} transformed records for {symbol}"
                )
                return True
            else:
                self.logger.error(f"Failed to save transformed data for {symbol}")
                return False

        except Exception as e:
            self.logger.error(f"Error transforming and saving data for {symbol}: {e}")
            return False
