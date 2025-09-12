from typing import List, Dict, Any
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo import UpdateOne, InsertOne
import pandas as pd
from src.config.config_mongo import ConfigMongo
from src.config.config_logging import ConfigLogging
import datetime


class LoadMongo:
    """Tải dữ liệu lên MongoDB"""

    def __init__(self, database_name: str = "funding_rate_db"):
        self.logger = ConfigLogging.config_logging("LoadMongo")
        self.config_mongo = ConfigMongo()
        self.client = self.config_mongo.get_client()
        self.database = self.client[database_name]
        self.logger.info(f"Connected to MongoDB database: {database_name}")

    def get_collection(self, collection_name: str) -> Collection:
        """Lấy collection MongoDB

        Args:
            collection_name: Tên của collection

        Returns:
            Đối tượng collection MongoDB
        """
        return self.database[collection_name]

    def insert_funding_rate_history(
        self, symbol: str, data: List[Dict[str, Any]]
    ) -> bool:
        """Chèn dữ liệu lịch sử tỷ lệ funding

        Args:
            symbol: Symbol giao dịch (ví dụ: BTCUSDT)
            data: Danh sách dữ liệu tỷ lệ funding

        Returns:
            True nếu thành công, False nếu không
        """
        try:
            collection_name = f"funding_rate_history_{symbol.lower()}"
            collection = self.get_collection(collection_name)

            # Tạo index để truy vấn nhanh hơn - optimized
            try:
                collection.create_index(
                    "fundingTime", unique=True, background=True, sparse=True
                )
            except Exception as idx_error:
                self.logger.warning(f"Index creation warning: {idx_error}")

            if data:
                # Sử dụng upsert để tránh trùng lặp
                operations = []
                for item in data:
                    operations.append(
                        {
                            "updateOne": {
                                "filter": {"fundingTime": item["fundingTime"]},
                                "update": {"$set": item},
                                "upsert": True,
                            }
                        }
                    )

                result = collection.bulk_write(operations, ordered=False)
                self.logger.info(
                    f"Inserted {result.upserted_count} new records for {symbol}"
                )
                return True

        except BulkWriteError as e:
            self.logger.warning(f"Bulk write error for {symbol}: {e.details}")
            return True  # Some records might have been inserted
        except Exception as e:
            self.logger.error(f"Error inserting funding rate history for {symbol}: {e}")
            return False

    def insert_funding_rate_realtime(self, data: Dict[str, Any]) -> bool:
        """Chèn dữ liệu tỷ lệ funding theo thời gian thực

        Args:
            data: Dữ liệu tỷ lệ funding

        Returns:
            True nếu thành công, False nếu không
        """
        try:
            collection = self.get_collection("funding_rate_realtime")

            # Tạo compound index - optimized
            try:
                collection.create_index(
                    [("symbol", 1), ("eventTime", 1)],
                    unique=True,
                    background=True,
                    sparse=True,
                )
            except Exception as idx_error:
                self.logger.warning(f"Index creation warning: {idx_error}")

            # Upsert để tránh trùng lặp
            result = collection.update_one(
                {"symbol": data["symbol"], "eventTime": data["eventTime"]},
                {"$set": data},
                upsert=True,
            )

            if result.upserted_id:
                self.logger.debug(f"Inserted new realtime data for {data['symbol']}")

            return True

        except DuplicateKeyError:
            self.logger.debug(f"Duplicate realtime data for {data['symbol']}")
            return True
        except Exception as e:
            self.logger.error(f"Error inserting realtime funding rate: {e}")
            return False

    def insert_funding_rate_realtime_batch(self, data: List[Dict[str, Any]]) -> bool:
        """Chèn dữ liệu tỷ lệ funding theo thời gian thực theo batch

        Args:
            data: Danh sách dữ liệu tỷ lệ funding

        Returns:
            True nếu thành công, False nếu không
        """
        try:
            if not data:
                return True

            collection = self.get_collection("funding_rate_realtime")

            # Tạo compound index - optimized cho batch
            try:
                collection.create_index(
                    [("symbol", 1), ("eventTime", 1)],
                    unique=True,
                    background=True,
                    sparse=True,
                )
            except Exception as idx_error:
                self.logger.warning(f"Index creation warning: {idx_error}")

            # Chuẩn bị bulk operations
            operations = []
            for item in data:
                operations.append(
                    {
                        "updateOne": {
                            "filter": {
                                "symbol": item["symbol"],
                                "eventTime": item["eventTime"],
                            },
                            "update": {"$set": item},
                            "upsert": True,
                        }
                    }
                )

            if operations:
                result = collection.bulk_write(operations, ordered=False)
                self.logger.info(
                    f"Batch inserted {result.upserted_count} new realtime records (updated {result.modified_count})"
                )

            return True

        except Exception as e:
            self.logger.error(f"Error batch inserting realtime funding rate: {e}")
            return False

    def get_latest_funding_time(self, symbol: str) -> int:
        """Lấy thời gian funding mới nhất cho một symbol

        Args:
            symbol: Symbol giao dịch

        Returns:
            Timestamp thời gian funding mới nhất hoặc 0 nếu không có dữ liệu
        """
        try:
            # Check in the unified history collection first
            collection = self.get_collection("history")
            latest = collection.find_one(
                {"symbol": symbol}, 
                sort=[("funding_date", -1), ("funding_time", -1)]
            )
            
            if latest:
                # Convert funding_date and funding_time back to timestamp
                from datetime import datetime
                date_str = latest["funding_date"]
                time_str = latest["funding_time"]
                dt = datetime.fromisoformat(f"{date_str} {time_str}")
                return int(dt.timestamp() * 1000)
            
            # Fallback to old format (individual collections)
            collection_name = f"funding_rate_history_{symbol.lower()}"
            collection = self.get_collection(collection_name)
            latest = collection.find_one({}, sort=[("fundingTime", -1)])
            return latest["fundingTime"] if latest else 0

        except Exception as e:
            self.logger.error(f"Error getting latest funding time for {symbol}: {e}")
            return 0

    def has_funding_data(self, symbol: str) -> bool:
        """Kiểm tra xem symbol có dữ liệu funding nào không

        Args:
            symbol: Symbol giao dịch

        Returns:
            True nếu có dữ liệu, False nếu không
        """
        try:
            # Check in the unified history collection first
            collection = self.get_collection("history")
            count = collection.count_documents({"symbol": symbol}, limit=1)
            if count > 0:
                return True
            
            # Fallback to old format (individual collections)
            collection_name = f"funding_rate_history_{symbol.lower()}"
            collection = self.get_collection(collection_name)
            count = collection.count_documents({}, limit=1)
            return count > 0

        except Exception as e:
            self.logger.error(f"Error checking funding data for {symbol}: {e}")
            return False

    def get_funding_rate_stats(self) -> Dict[str, Any]:
        """Lấy thống kê tỷ lệ funding

        Returns:
            Từ điển thống kê
        """
        try:
            stats = {}

            # Lấy tên collection cho lịch sử
            collection_names = self.database.list_collection_names()
            history_collections = [
                name
                for name in collection_names
                if name.startswith("funding_rate_history_")
            ]

            stats["total_symbols"] = len(history_collections)
            stats["collections"] = {}

            for collection_name in history_collections:
                collection = self.get_collection(collection_name)
                count = collection.count_documents({})
                stats["collections"][collection_name] = count

            # Thống kê collection realtime
            realtime_collection = self.get_collection("funding_rate_realtime")
            stats["realtime_count"] = realtime_collection.count_documents({})

            return stats

        except Exception as e:
            self.logger.error(f"Error getting funding rate stats: {e}")
            return {}

    def save_transformed_funding_data(self, data: List[Dict[str, Any]]) -> bool:
        """Lưu dữ liệu tỷ lệ funding đã biến đổi vào MongoDB sử dụng pandas để xử lý hiệu quả

        Args:
            data: Dữ liệu tỷ lệ funding đã biến đổi

        Returns:
            True nếu thành công, False nếu không
        """
        try:
            if not data:
                self.logger.warning("No transformed data to save")
                return False

            # Chuyển đổi thành DataFrame để xử lý hiệu quả
            df = pd.DataFrame(data)

            # Chuyển đổi đối tượng datetime thành chuỗi
            for col in df.columns:
                if df[col].dtype == "object":
                    # Kiểm tra xem cột có chứa đối tượng datetime không
                    sample_val = df[col].iloc[0] if len(df) > 0 else None
                    if isinstance(
                        sample_val, (datetime.datetime, datetime.date, datetime.time)
                    ):
                        df[col] = df[col].astype(str)

            # Sử dụng single collection cho tất cả dữ liệu
            collection = self.get_collection("history")

            # Tạo indexes để cải thiện hiệu suất truy vấn - tối ưu RAM
            try:
                # Index chính cho history collection - compound index hiệu quả
                collection.create_index(
                    [("symbol", 1), ("funding_date", 1), ("funding_time", 1)],
                    unique=True,
                    background=True,  # Tạo index trong background để không block
                )

                # Index riêng cho queries thường dùng - sparse để tiết kiệm RAM
                collection.create_index("funding_date", background=True, sparse=True)
                collection.create_index("symbol", background=True, sparse=True)

                # Index TTL để tự động xóa dữ liệu cũ (tùy chọn)
                # collection.create_index("funding_date", expireAfterSeconds=365*24*60*60, background=True)  # 1 năm

            except Exception as idx_error:
                self.logger.warning(f"Index creation warning: {idx_error}")

            # Chuẩn bị bulk operations sử dụng pymongo UpdateOne
            operations = []
            records = df.to_dict("records")

            for item in records:
                operations.append(
                    UpdateOne(
                        filter={
                            "symbol": item["symbol"],
                            "funding_date": item["funding_date"],
                            "funding_time": item["funding_time"],
                        },
                        update={"$set": item},
                        upsert=True,
                    )
                )

            if operations:
                # Xử lý theo batch để tránh vấn đề bộ nhớ
                batch_size = 1000
                total_upserted = 0
                total_modified = 0

                for i in range(0, len(operations), batch_size):
                    batch = operations[i : i + batch_size]
                    result = collection.bulk_write(batch, ordered=False)
                    total_upserted += result.upserted_count
                    total_modified += result.modified_count

                self.logger.info(
                    f"Saved {total_upserted} new records to history collection (updated {total_modified})"
                )

            return True

        except BulkWriteError as e:
            self.logger.warning(f"Bulk write error: {e.details}")
            return True  # Some records might have been inserted
        except Exception as e:
            self.logger.error(f"Error in save_transformed_funding_data: {e}")
            return False

    def save_realtime_data(
        self, collection_name: str, data: List[Dict[str, Any]]
    ) -> bool:
        """Lưu dữ liệu tỷ lệ funding theo thời gian thực vào MongoDB sử dụng pandas để xử lý hiệu quả

        Args:
            collection_name: Tên collection để lưu
            data: Danh sách dữ liệu tỷ lệ funding theo thời gian thực

        Returns:
            True nếu thành công, False nếu không
        """
        try:
            if not data:
                self.logger.warning("No realtime data to save")
                return False

            # Convert to DataFrame for efficient processing
            df = pd.DataFrame(data)

            # Convert datetime objects to strings
            for col in df.columns:
                if df[col].dtype == "object":
                    # Check if column contains datetime objects
                    sample_val = df[col].iloc[0] if len(df) > 0 else None
                    if isinstance(
                        sample_val, (datetime.datetime, datetime.date, datetime.time)
                    ):
                        df[col] = df[col].astype(str)

            collection = self.get_collection(collection_name)

            # Tạo indexes để cải thiện hiệu suất truy vấn - tối ưu cho realtime
            try:
                # Index chính cho realtime - compound index với TTL
                collection.create_index(
                    [("symbol", 1), ("event_time", 1)], unique=True, background=True
                )

                # Index cho date queries - sparse để tiết kiệm RAM
                collection.create_index("date", background=True, sparse=True)
                collection.create_index("symbol", background=True, sparse=True)

            except Exception as idx_error:
                self.logger.warning(f"Index creation warning: {idx_error}")

            # Chuẩn bị bulk operations sử dụng pymongo UpdateOne
            operations = []
            records = df.to_dict("records")

            for item in records:
                operations.append(
                    UpdateOne(
                        filter={
                            "symbol": item["symbol"],
                            "event_time": item["event_time"],
                        },
                        update={"$set": item},
                        upsert=True,
                    )
                )

            if operations:
                # Xử lý theo batch
                batch_size = 500
                total_upserted = 0
                total_modified = 0

                for i in range(0, len(operations), batch_size):
                    batch = operations[i : i + batch_size]
                    result = collection.bulk_write(batch, ordered=False)
                    total_upserted += result.upserted_count
                    total_modified += result.modified_count

                self.logger.debug(
                    f"Saved {total_upserted} new realtime records (updated {total_modified})"
                )

            return True

        except BulkWriteError as e:
            self.logger.warning(f"Bulk write error for realtime data: {e.details}")
            return True  # Some records might have been inserted
        except Exception as e:
            self.logger.error(f"Error saving realtime data: {e}")
            return False

    def update_realtime_funding_data(self, collection_name: str, data: List[Dict[str, Any]]) -> bool:
        """Update realtime funding rate data using upsert
        
        Args:
            collection_name: Name of the collection to update
            data: List of funding rate data to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not data:
                return True

            collection = self.get_collection(collection_name)

            # Create indexes for efficient queries
            try:
                collection.create_index(
                    [("symbol", 1), ("update_date", 1)],
                    unique=True,
                    background=True,
                    sparse=True,
                )
                collection.create_index("last_update_timestamp", background=True)
            except Exception as idx_error:
                self.logger.warning(f"Index creation warning: {idx_error}")

            # Prepare bulk operations for upsert
            operations = []
            for item in data:
                operations.append(
                    UpdateOne(
                        filter={
                            "symbol": item["symbol"],
                            "update_date": item["update_date"],
                        },
                        update={"$set": item},
                        upsert=True,
                    )
                )

            if operations:
                # Process in batches
                batch_size = 100
                total_upserted = 0
                total_modified = 0

                for i in range(0, len(operations), batch_size):
                    batch = operations[i : i + batch_size]
                    result = collection.bulk_write(batch, ordered=False)
                    total_upserted += result.upserted_count
                    total_modified += result.modified_count

                self.logger.info(
                    f"Updated {total_upserted} new funding records, modified {total_modified} existing records"
                )

            return True

        except BulkWriteError as e:
            self.logger.warning(f"Bulk write error for funding data: {e.details}")
            return True  # Some records might have been processed
        except Exception as e:
            self.logger.error(f"Error updating realtime funding data: {e}")
            return False
