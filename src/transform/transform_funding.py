import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from src.config.config_logging import ConfigLogging
from src.load.load_mongo import LoadMongo
from src.utils.util_convert_datetime import UtilConvertDatetime


class TransformFundingData:
    """Biến đổi dữ liệu tỷ lệ funding để phân tích và lưu trữ"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("TransformFundingData")
        self.load_mongo = LoadMongo()
        self.util_datetime = UtilConvertDatetime()

    def transform_funding_data(
        self, raw_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Biến đổi dữ liệu funding thô thành định dạng chuẩn

        Args:
            raw_data: Dữ liệu tỷ lệ funding thô từ API

        Returns:
            Dữ liệu đã biến đổi với các trường bổ sung
        """
        try:
            if not raw_data:
                self.logger.warning("No data to transform")
                return []

            transformed_data = []

            for record in raw_data:
                try:
                    # Chuyển đổi timestamp thành datetime và định dạng đúng
                    funding_datetime = self.util_datetime.timestamp_to_datetime(
                        record.get("fundingTime", 0)
                    )

                    # Tạo bản ghi đã biến đổi chỉ với các trường bắt buộc
                    transformed_record = {
                        "symbol": record.get("symbol", ""),
                        "funding_date": funding_datetime.date(),
                        "funding_time": funding_datetime.time().replace(
                            microsecond=0
                        ),  # Loại bỏ microsecond
                        "fundingRate": float(record.get("fundingRate", 0)),
                        "markPrice": float(record.get("markPrice", 0)),
                    }

                    transformed_data.append(transformed_record)

                except Exception as e:
                    self.logger.warning(f"Error transforming record {record}: {e}")
                    continue

            self.logger.info(f"Transformed {len(transformed_data)} records")
            return transformed_data

        except Exception as e:
            self.logger.error(f"Error in transform_funding_data: {e}")
            return []

    def transform_realtime_data(
        self, raw_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Biến đổi dữ liệu giá mark theo thời gian thực để lưu trữ

        Args:
            raw_data: Dữ liệu thời gian thực thô từ websocket với các trường:
                      e: loại sự kiện, E: thời gian sự kiện, s: symbol, p: giá mark,
                      i: giá chỉ số, r: tỷ lệ funding, P: giá thanh toán ước tính, T: thời gian funding tiếp theo

        Returns:
            Dữ liệu đã biến đổi cho collection realtime
        """
        try:
            if not raw_data:
                self.logger.warning("No realtime data to transform")
                return []

            transformed_data = []

            for record in raw_data:
                try:
                    # Ánh xạ tên trường websocket thành tên đúng
                    event_type = record.get("e", "")  # Loại sự kiện
                    event_time = record.get("E", 0)  # Thời gian sự kiện
                    symbol = record.get("s", "")  # Symbol
                    mark_price = record.get("p", "0")  # Giá mark
                    index_price = record.get("i", "0")  # Giá chỉ số
                    funding_rate = record.get("r", "0")  # Tỷ lệ funding (trường đúng!)
                    estimated_settle_price = record.get(
                        "P", "0"
                    )  # Giá thanh toán ước tính (đã bị dùng sai làm tỷ lệ funding)
                    next_funding_time = record.get(
                        "T", 0
                    )  # Thời gian funding tiếp theo

                    # Chuyển đổi thời gian sự kiện thành datetime
                    event_datetime = self.util_datetime.timestamp_to_datetime(
                        event_time
                    )
                    next_funding_datetime = self.util_datetime.timestamp_to_datetime(
                        next_funding_time
                    )

                    # Create clean transformed record
                    transformed_record = {
                        "symbol": symbol,
                        "mark_price": float(mark_price),
                        "index_price": float(index_price),
                        "funding_rate": float(funding_rate),
                        "event_time": event_time,
                        "next_funding_time": next_funding_time,
                        "date": event_datetime.date().isoformat(),
                        "time": event_datetime.time()
                        .replace(microsecond=0)
                        .isoformat(),
                    }

                    transformed_data.append(transformed_record)

                except Exception as e:
                    self.logger.warning(
                        f"Error transforming realtime record {record}: {e}"
                    )
                    continue

            self.logger.debug(f"Transformed {len(transformed_data)} realtime records")
            return transformed_data

        except Exception as e:
            self.logger.error(f"Error in transform_realtime_data: {e}")
            return []

    def _categorize_funding_rate(self, funding_rate: float) -> str:
        """Phân loại tỷ lệ funding thành các mức khác nhau

        Args:
            funding_rate: Giá trị tỷ lệ funding

        Returns:
            Chuỗi phân loại
        """
        rate_percent = abs(funding_rate) * 100

        if rate_percent >= 1.0:
            return "very_high"
        elif rate_percent >= 0.5:
            return "high"
        elif rate_percent >= 0.1:
            return "medium"
        elif rate_percent >= 0.01:
            return "low"
        else:
            return "very_low"

    def _categorize_price(self, price: float) -> str:
        """Phân loại giá thành các khoảng khác nhau

        Args:
            price: Giá trị giá mark

        Returns:
            Chuỗi phân loại
        """
        if price >= 100000:
            return "very_high"
        elif price >= 50000:
            return "high"
        elif price >= 10000:
            return "medium"
        elif price >= 1000:
            return "low"
        else:
            return "very_low"

    def calculate_funding_stats(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Tính toán thống kê cho dữ liệu tỷ lệ funding

        Args:
            data: Dữ liệu tỷ lệ funding

        Returns:
            Từ điển thống kê
        """
        try:
            if not data:
                return {}

            df = pd.DataFrame(data)

            stats = {
                "total_records": len(data),
                "unique_symbols": df["symbol"].nunique(),
                "date_range": {
                    "start": df["funding_date"].min(),
                    "end": df["funding_date"].max(),
                },
                "funding_rate_stats": {
                    "mean": df["fundingRate"].mean(),
                    "median": df["fundingRate"].median(),
                    "std": df["fundingRate"].std(),
                    "min": df["fundingRate"].min(),
                    "max": df["fundingRate"].max(),
                },
                "funding_rate_percent_stats": {
                    "mean": df["funding_rate_percent"].mean(),
                    "median": df["funding_rate_percent"].median(),
                    "std": df["funding_rate_percent"].std(),
                    "min": df["funding_rate_percent"].min(),
                    "max": df["funding_rate_percent"].max(),
                },
                "category_distribution": df["funding_rate_category"]
                .value_counts()
                .to_dict(),
                "top_symbols_by_volume": df.groupby("symbol")["fundingRate"]
                .count()
                .nlargest(10)
                .to_dict(),
            }

            return stats

        except Exception as e:
            self.logger.error(f"Error calculating funding stats: {e}")
            return {}

    def run(self, raw_data: List[Dict[str, Any]] = None) -> bool:
        """Chạy quy trình biến đổi hoàn chỉnh

        Args:
            raw_data: Dữ liệu thô để biến đổi. Nếu None, lấy từ MongoDB

        Returns:
            True nếu thành công, False nếu không
        """
        try:
            self.logger.info("Starting funding data transformation process")

            if raw_data is None:
                # Lấy dữ liệu thô từ MongoDB (cần được triển khai)
                self.logger.info("Getting raw data from MongoDB...")
                # raw_data = self.load_mongo.get_raw_funding_data()
                raw_data = []

            if not raw_data:
                self.logger.warning("No raw data to transform")
                return False

            # Transform data
            transformed_data = self.transform_funding_data(raw_data)

            if not transformed_data:
                self.logger.error("Failed to transform data")
                return False

            # Calculate statistics
            stats = self.calculate_funding_stats(transformed_data)

            # Save transformed data to MongoDB
            if self.load_mongo.save_transformed_funding_data(transformed_data):
                self.logger.info("Transformed data saved to MongoDB")
            else:
                self.logger.error("Failed to save transformed data")
                return False

            # Log statistics
            self.logger.info("Transformation Statistics:")
            self.logger.info(f"  Total records: {stats.get('total_records', 0)}")
            self.logger.info(f"  Unique symbols: {stats.get('unique_symbols', 0)}")
            self.logger.info(".6f")
            self.logger.info(".6f")

            self.logger.info("Funding data transformation completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in run method: {e}")
            return False

    def stop(self) -> bool:
        """Dừng quy trình biến đổi

        Returns:
            True nếu dừng thành công, False nếu không
        """
        try:
            self.logger.info("Stopping funding data transformation")
            self.logger.info("Transformation stopped successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error stopping transformation: {e}")
            return False

    def test(self, sample_data: List[Dict[str, Any]] = None) -> bool:
        """Kiểm tra chức năng biến đổi

        Args:
            sample_data: Dữ liệu mẫu để kiểm tra

        Returns:
            True nếu kiểm tra thành công, False nếu không
        """
        try:
            self.logger.info("Testing transformation functionality")

            # Create sample data if not provided
            if sample_data is None:
                sample_data = [
                    {
                        "symbol": "BTCUSDT",
                        "fundingTime": self.util_datetime.get_current_timestamp()
                        - (24 * 60 * 60 * 1000),
                        "fundingRate": 0.0001,
                        "markPrice": 50000.0,
                        "created_at": self.util_datetime.get_current_timestamp(),
                    },
                    {
                        "symbol": "ETHUSDT",
                        "fundingTime": self.util_datetime.get_current_timestamp()
                        - (12 * 60 * 60 * 1000),
                        "fundingRate": -0.00005,
                        "markPrice": 2500.0,
                        "created_at": self.util_datetime.get_current_timestamp(),
                    },
                ]

            # Test transformation
            transformed_data = self.transform_funding_data(sample_data)

            if transformed_data:
                self.logger.info(
                    f"Test successful! Transformed {len(transformed_data)} records"
                )

                # Log sample transformed data
                for record in transformed_data:
                    self.logger.info(
                        f"Sample transformed: {record['symbol']} - Rate: {record['fundingRate']:.6f} - Date: {record['funding_date']} {record['funding_time']}"
                    )

                # Test statistics
                stats = self.calculate_funding_stats(transformed_data)
                self.logger.info(f"Stats calculated: {len(stats)} metrics")

                return True
            else:
                self.logger.error("Test failed! No transformed data")
                return False

        except Exception as e:
            self.logger.error(f"Error in test method: {e}")
            return False
