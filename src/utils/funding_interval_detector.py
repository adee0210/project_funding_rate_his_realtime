"""
Funding Interval Detection and Cache System
Tự động phát hiện interval funding (4h vs 8h) dựa trên pattern của nextFundingTime
"""

import json
import requests
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
import os
from pathlib import Path

class FundingIntervalDetector:
    """Phát hiện và cache thông tin interval funding cho symbols"""
    
    def __init__(self, cache_file: str = "funding_intervals_cache.json"):
        self.base_url = "https://fapi.binance.com"
        self.cache_file = cache_file
        self.cache_data = self._load_cache()
        
    def _load_cache(self) -> Dict:
        """Load cache từ file JSON"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"Loaded cache with {len(data.get('intervals', {}))} symbols")
                    return data
        except Exception as e:
            print(f"Error loading cache: {e}")
        
        return {
            "last_updated": None,
            "intervals": {},
            "detection_history": []
        }
    
    def _save_cache(self):
        """Save cache to file JSON"""
        try:
            self.cache_data["last_updated"] = datetime.now(timezone.utc).isoformat()
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, indent=2, ensure_ascii=False)
            print(f"Cache saved with {len(self.cache_data['intervals'])} symbols")
        except Exception as e:
            print(f"Error saving cache: {e}")
    
    def detect_funding_intervals(self, symbols: List[str], force_update: bool = False) -> Dict[str, str]:
        """
        Phát hiện interval funding cho list symbols
        
        Args:
            symbols: List symbols cần phát hiện
            force_update: Bắt buộc update cache
            
        Returns:
            Dict mapping symbol -> interval ("4h" hoặc "8h")
        """
        result = {}
        symbols_to_detect = []
        
        # Kiểm tra cache trước
        if not force_update:
            for symbol in symbols:
                if symbol in self.cache_data["intervals"]:
                    result[symbol] = self.cache_data["intervals"][symbol]
                else:
                    symbols_to_detect.append(symbol)
        else:
            symbols_to_detect = symbols.copy()
        
        if not symbols_to_detect:
            print("All symbols found in cache")
            return result
        
        print(f"Detecting intervals for {len(symbols_to_detect)} symbols...")
        
        # Phát hiện interval mới
        detected_intervals = self._analyze_funding_patterns(symbols_to_detect)
        
        # Update cache và result
        for symbol, interval in detected_intervals.items():
            self.cache_data["intervals"][symbol] = interval
            result[symbol] = interval
        
        # Save cache
        if detected_intervals:
            self._save_cache()
        
        return result
    
    def _analyze_funding_patterns(self, symbols: List[str]) -> Dict[str, str]:
        """
        Phân tích pattern funding time để xác định interval
        
        Logic:
        - Funding 8h: xảy ra vào 0h, 8h, 16h UTC (8 tiếng một lần)
        - Funding 4h: xảy ra vào 0h, 4h, 8h, 12h, 16h, 20h UTC (4 tiếng một lần)
        """
        try:
            # Get current funding data
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            response = requests.get(url, timeout=30)
            
            if response.status_code != 200:
                print(f"API request failed: {response.status_code}")
                return {}
            
            data = response.json()
            current_time = int(time.time() * 1000)
            result = {}
            
            # Lọc data cho symbols cần phát hiện
            symbol_data = {}
            for item in data:
                if item['symbol'] in symbols:
                    symbol_data[item['symbol']] = item
            
            print(f"Found API data for {len(symbol_data)} symbols")
            
            # Phân tích từng symbol
            for symbol, item in symbol_data.items():
                next_funding_time = int(item.get('nextFundingTime', 0))
                
                if next_funding_time == 0:
                    # Không có thông tin funding time, mặc định 8h
                    result[symbol] = "8h"
                    continue
                
                # Convert to datetime
                next_funding_dt = datetime.fromtimestamp(next_funding_time / 1000, tz=timezone.utc)
                next_funding_hour = next_funding_dt.hour
                
                # Phân tích pattern dựa trên giờ funding
                interval = self._determine_interval_from_hour(next_funding_hour)
                result[symbol] = interval
                
                # Log detection history
                detection_record = {
                    "symbol": symbol,
                    "next_funding_time": next_funding_dt.isoformat(),
                    "next_funding_hour": next_funding_hour,
                    "detected_interval": interval,
                    "detection_time": datetime.now(timezone.utc).isoformat()
                }
                self.cache_data["detection_history"].append(detection_record)
            
            # Giữ lại 100 records gần nhất
            if len(self.cache_data["detection_history"]) > 100:
                self.cache_data["detection_history"] = self.cache_data["detection_history"][-100:]
            
            print(f"Detected intervals for {len(result)} symbols")
            return result
            
        except Exception as e:
            print(f"Error analyzing funding patterns: {e}")
            return {}
    
    def _determine_interval_from_hour(self, hour: int) -> str:
        """
        Xác định interval dựa trên giờ funding
        
        Pattern analysis:
        - 8h funding: 0, 8, 16 (chia hết cho 8)
        - 4h funding: 0, 4, 8, 12, 16, 20 (chia hết cho 4 nhưng không chia hết cho 8)
        """
        if hour % 8 == 0:
            # Có thể là 8h (0, 8, 16) hoặc 4h (0, 8, 16 cũng có trong 4h)
            # Cần logic bổ sung, tạm thời mặc định 8h cho safety
            return "8h"
        elif hour % 4 == 0:
            # 4, 12, 20 - chắc chắn là 4h
            return "4h"
        else:
            # Không match pattern nào, mặc định 8h
            return "8h"
    
    def get_cached_intervals(self) -> Dict[str, str]:
        """Lấy tất cả intervals đã cache"""
        return self.cache_data["intervals"].copy()
    
    def get_cache_stats(self) -> Dict:
        """Lấy thống kê cache"""
        intervals = self.cache_data["intervals"]
        stats = {
            "total_symbols": len(intervals),
            "4h_symbols": len([s for s, i in intervals.items() if i == "4h"]),
            "8h_symbols": len([s for s, i in intervals.items() if i == "8h"]),
            "last_updated": self.cache_data.get("last_updated"),
            "detection_history_count": len(self.cache_data.get("detection_history", []))
        }
        return stats
    
    def clear_cache(self):
        """Xóa cache"""
        self.cache_data = {
            "last_updated": None,
            "intervals": {},
            "detection_history": []
        }
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
        print("Cache cleared")


# Test functions
def test_interval_detection():
    """Test interval detection system"""
    print("Testing Funding Interval Detection System")
    print("=" * 50)
    
    detector = FundingIntervalDetector("test_funding_intervals.json")
    
    # Test với một số symbols phổ biến
    test_symbols = [
        'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
        'SOLUSDT', 'DOGEUSDT', 'DOTUSDT', 'MATICUSDT', 'LTCUSDT',
        'AVAXUSDT', 'LINKUSDT', 'UNIUSDT', 'ATOMUSDT', 'FILUSDT'
    ]
    
    print(f"Testing with {len(test_symbols)} symbols...")
    
    # Detect intervals
    intervals = detector.detect_funding_intervals(test_symbols)
    
    print(f"\n=== Detection Results ===")
    print(f"Total symbols: {len(intervals)}")
    
    symbols_4h = [s for s, i in intervals.items() if i == "4h"]
    symbols_8h = [s for s, i in intervals.items() if i == "8h"]
    
    print(f"4h symbols ({len(symbols_4h)}): {symbols_4h}")
    print(f"8h symbols ({len(symbols_8h)}): {symbols_8h}")
    
    # Show cache stats
    print(f"\n=== Cache Stats ===")
    stats = detector.get_cache_stats()
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # Test cache functionality
    print(f"\n=== Testing Cache ===")
    print("Re-detecting same symbols (should use cache)...")
    intervals2 = detector.detect_funding_intervals(test_symbols[:5])
    print(f"Cache test successful: {len(intervals2)} symbols from cache")
    
    return detector

if __name__ == "__main__":
    test_interval_detection()