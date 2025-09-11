import json
import time
import asyncio
import websockets
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime
import queue
import traceback

from src.config.config_logging import ConfigLogging
from src.config.config_variable import REALTIME_CONFIG, SYSTEM_CONFIG
from src.load.load_mongo import LoadMongo
from src.transform.transform_funding import TransformFundingData
from src.utils.util_tele_bot_check import UtilTeleBotCheck


class ExtractFundingRateRealtime:
    """Trích xuất dữ liệu tỷ lệ funding theo thời gian thực qua WebSocket"""

    def __init__(self):
        self.logger = ConfigLogging.config_logging("ExtractFundingRateRealtime")
        self.load_mongo = LoadMongo()
        self.transform_funding = TransformFundingData()
        self.tele_bot = UtilTeleBotCheck()

        # Cấu hình
        self.config = REALTIME_CONFIG

        # Quản lý trạng thái
        self.is_running = False
        self.websocket = None
        self.symbols = []
        self.reconnect_attempts = 0
        self.last_heartbeat = None

        # Xử lý dữ liệu
        self.data_queue = queue.Queue()
        self.batch_data = []
        self.last_transform_time = time.time()

        # Threading
        self.websocket_thread = None
        self.transform_thread = None
        self.event_loop = None

    def start_realtime_extraction(self, symbols: List[str]) -> bool:
        """Bắt đầu trích xuất tỷ lệ funding theo thời gian thực

        Args:
            symbols: Danh sách symbol để giám sát

        Returns:
            True nếu bắt đầu thành công, False nếu không
        """
        try:
            if self.is_running:
                self.logger.warning("Realtime extraction already running")
                return True

            if not symbols:
                self.logger.error("No symbols provided for realtime extraction")
                return False

            self.symbols = symbols[: SYSTEM_CONFIG["max_symbols_per_websocket"]]
            self.is_running = True
            self.reconnect_attempts = 0

            self.logger.info(
                f"Starting realtime extraction for {len(self.symbols)} symbols"
            )

            # Bắt đầu websocket thread
            self.websocket_thread = threading.Thread(
                target=self._run_websocket_loop, daemon=True
            )
            self.websocket_thread.start()

            # Bắt đầu transform thread
            self.transform_thread = threading.Thread(
                target=self._transform_loop, daemon=True
            )
            self.transform_thread.start()

            self.logger.info("Realtime extraction started successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error starting realtime extraction: {e}")
            self.is_running = False
            return False

    def stop_realtime_extraction(self) -> bool:
        """Dừng trích xuất theo thời gian thực

        Returns:
            True nếu dừng thành công, False nếu không
        """
        try:
            if not self.is_running:
                self.logger.warning("Realtime extraction not running")
                return True

            self.logger.info("Stopping realtime extraction")
            self.is_running = False

            # Đóng websocket
            if self.websocket:
                asyncio.run_coroutine_threadsafe(
                    self.websocket.close(), self.event_loop
                ).result(timeout=5)

            # Chờ threads kết thúc
            if self.websocket_thread and self.websocket_thread.is_alive():
                self.websocket_thread.join(timeout=10)

            if self.transform_thread and self.transform_thread.is_alive():
                self.transform_thread.join(timeout=5)

            # Xử lý dữ liệu còn lại
            self._process_remaining_data()

            self.logger.info("Realtime extraction stopped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error stopping realtime extraction: {e}")
            return False

    def _run_websocket_loop(self):
        """Chạy kết nối websocket trong vòng lặp asyncio"""
        try:
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)

            while self.is_running:
                try:
                    self.event_loop.run_until_complete(self._connect_websocket())
                except Exception as e:
                    self.logger.error(f"Websocket loop error: {e}")

                if (
                    self.is_running
                    and self.reconnect_attempts < self.config["max_reconnect_attempts"]
                ):
                    self.reconnect_attempts += 1
                    self.logger.info(
                        f"Reconnecting in {self.config['reconnect_interval']}s (attempt {self.reconnect_attempts})"
                    )
                    time.sleep(self.config["reconnect_interval"])
                else:
                    break

        except Exception as e:
            self.logger.error(f"Fatal error in websocket loop: {e}")
        finally:
            if self.event_loop:
                self.event_loop.close()

    async def _connect_websocket(self):
        """Kết nối tới websocket Binance"""
        try:
            # Create stream names for all symbols
            streams = []
            for symbol in self.symbols:
                streams.append(f"{symbol.lower()}@markPrice")

            stream_names = "/".join(streams)
            uri = f"{self.config['websocket_url']}{stream_names}"

            self.logger.info(f"Connecting to websocket: {len(streams)} streams")

            async with websockets.connect(
                uri,
                ping_interval=self.config["ping_interval"],
                ping_timeout=self.config["connection_timeout"],
            ) as websocket:
                self.websocket = websocket
                self.reconnect_attempts = 0
                self.last_heartbeat = time.time()

                self.logger.info("Websocket connected successfully")
                self.tele_bot.send_message(
                    f"Websocket connected - monitoring {len(self.symbols)} symbols"
                )

                async for message in websocket:
                    if not self.is_running:
                        break

                    try:
                        data = json.loads(message)
                        self._process_websocket_message(data)
                        self.last_heartbeat = time.time()

                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Invalid JSON received: {e}")
                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}")

        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("Websocket connection closed")
        except Exception as e:
            self.logger.error(f"Websocket connection error: {e}")
            traceback.print_exc()

    def _process_websocket_message(self, data: Dict[str, Any]):
        """Xử lý tin nhắn websocket đến

        Args:
            data: Dữ liệu tin nhắn websocket
        """
        try:
            if "stream" in data and "data" in data:
                # Multi-stream format
                stream_data = data["data"]
                stream_name = data["stream"]

                if "@markPrice" in stream_name:
                    self._add_to_queue(stream_data)

            elif "s" in data and "p" in data:
                # Direct mark price data
                self._add_to_queue(data)

        except Exception as e:
            self.logger.error(f"Error processing websocket message: {e}")

    def _add_to_queue(self, data: Dict[str, Any]):
        """Thêm dữ liệu vào hàng đợi xử lý

        Args:
            data: Dữ liệu giá mark với định dạng websocket
                  e: loại sự kiện
                  E: thời gian sự kiện
                  s: symbol
                  p: giá mark
                  i: giá chỉ số
                  r: tỷ lệ funding (trường ĐÚNG)
                  P: giá thanh toán ước tính (KHÔNG phải tỷ lệ funding)
                  T: thời gian funding tiếp theo
        """
        try:
            # Giữ định dạng websocket gốc cho transform
            # Xác thực các trường bắt buộc
            if "s" in data and "p" in data and "E" in data:
                self.data_queue.put(data)
            else:
                self.logger.warning(f"Invalid websocket data format: {data}")

        except Exception as e:
            self.logger.error(f"Error adding data to queue: {e}")

    def _transform_loop(self):
        """Xử lý và biến đổi dữ liệu định kỳ"""
        while self.is_running:
            try:
                current_time = time.time()

                # Thu thập dữ liệu batch
                batch_count = 0
                while (
                    not self.data_queue.empty()
                    and batch_count < self.config["batch_size"]
                ):
                    try:
                        data = self.data_queue.get_nowait()
                        self.batch_data.append(data)
                        batch_count += 1
                    except queue.Empty:
                        break

                # Biến đổi và lưu nếu khoảng thời gian đã qua hoặc batch đầy
                if (
                    current_time - self.last_transform_time
                    >= self.config["transform_interval"]
                    or len(self.batch_data) >= self.config["batch_size"]
                ):

                    if self.batch_data:
                        self._transform_and_save_batch()
                        self.last_transform_time = current_time

                time.sleep(1)  # Check every second

            except Exception as e:
                self.logger.error(f"Error in transform loop: {e}")
                time.sleep(5)

    def _transform_and_save_batch(self):
        """Biến đổi và lưu dữ liệu batch"""
        try:
            if not self.batch_data:
                return

            # Biến đổi dữ liệu
            transformed_data = self.transform_funding.transform_realtime_data(
                self.batch_data
            )

            if transformed_data:
                # Lưu vào MongoDB
                success = self.load_mongo.save_realtime_data(
                    self.config["collection_name"], transformed_data
                )

                if success:
                    self.logger.debug(f"Saved {len(transformed_data)} realtime records")
                else:
                    self.logger.warning("Failed to save realtime data")

            # Xóa batch
            self.batch_data.clear()

        except Exception as e:
            self.logger.error(f"Error transforming and saving batch: {e}")

    def _process_remaining_data(self):
        """Xử lý bất kỳ dữ liệu còn lại trong queue và batch"""
        try:
            # Xử lý dữ liệu queue còn lại
            remaining_data = []
            while not self.data_queue.empty():
                try:
                    remaining_data.append(self.data_queue.get_nowait())
                except queue.Empty:
                    break

            # Thêm dữ liệu batch
            remaining_data.extend(self.batch_data)

            if remaining_data:
                self.batch_data = remaining_data
                self._transform_and_save_batch()
                self.logger.info(f"Processed {len(remaining_data)} remaining records")

        except Exception as e:
            self.logger.error(f"Error processing remaining data: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Lấy trạng thái trích xuất theo thời gian thực

        Returns:
            Từ điển trạng thái
        """
        try:
            return {
                "is_running": self.is_running,
                "is_connected": (
                    self.websocket is not None and not self.websocket.closed
                    if self.websocket
                    else False
                ),
                "symbols_count": len(self.symbols),
                "symbols": self.symbols[:10] if self.symbols else [],
                "reconnect_attempts": self.reconnect_attempts,
                "queue_size": self.data_queue.qsize(),
                "batch_size": len(self.batch_data),
                "last_heartbeat": self.last_heartbeat,
                "last_transform_time": self.last_transform_time,
                "threads_alive": {
                    "websocket_thread": (
                        self.websocket_thread.is_alive()
                        if self.websocket_thread
                        else False
                    ),
                    "transform_thread": (
                        self.transform_thread.is_alive()
                        if self.transform_thread
                        else False
                    ),
                },
            }

        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {"error": str(e)}
