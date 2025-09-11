import pytz
from datetime import datetime, timezone
from typing import Union


class UtilConvertDatetime:
    """Lớp tiện ích để chuyển đổi datetime"""

    @staticmethod
    def timestamp_to_datetime(
        timestamp: Union[int, float], tz: str = "UTC"
    ) -> datetime:
        """Chuyển đổi timestamp thành đối tượng datetime

        Args:
            timestamp: Unix timestamp (giây hoặc milliseconds)
            tz: Chuỗi timezone (mặc định: UTC)

        Returns:
            đối tượng datetime
        """
        # Handle milliseconds timestamp
        if timestamp > 1e10:
            timestamp = timestamp / 1000

        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        if tz != "UTC":
            target_tz = pytz.timezone(tz)
            dt = dt.astimezone(target_tz)

        return dt

    @staticmethod
    def datetime_to_timestamp(dt: datetime) -> int:
        """Chuyển đổi datetime thành timestamp (milliseconds)

        Args:
            dt: đối tượng datetime

        Returns:
            timestamp tính bằng milliseconds
        """
        return int(dt.timestamp() * 1000)

    @staticmethod
    def get_current_timestamp() -> int:
        """Lấy timestamp hiện tại tính bằng milliseconds"""
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def format_datetime(dt: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
        """Định dạng datetime thành chuỗi

        Args:
            dt: đối tượng datetime
            format_str: chuỗi định dạng

        Returns:
            chuỗi datetime đã định dạng
        """
        return dt.strftime(format_str)
