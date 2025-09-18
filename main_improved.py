#!/usr/bin/env python3
"""
Script chính sử dụng hệ thống lập lịch tỷ lệ funding đã được cải tiến
"""

import sys
import signal
import time
from pathlib import Path

# Thêm thư mục gốc của project vào Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.utils.funding_rate_scheduler import FundingRateScheduler
from src.utils.util_tele_bot_check import UtilTeleBotCheck
from src.config.config_logging import ConfigLogging
from src.config.config_variable import SYSTEM_CONFIG

# Global scheduler instance for signal handling
scheduler_instance = None

def signal_handler(signum, frame):
    """Xử lý tín hiệu tắt một cách êm dịu"""
    global scheduler_instance
    print(f"\n📡 Received signal {signum}, shutting down gracefully...")
    
    if scheduler_instance:
        scheduler_instance.stop_scheduler()
    
    print("👋 Funding rate scheduler stopped. Goodbye!")
    sys.exit(0)

def get_top_symbols(count: int = 100) -> list:
    """Lấy danh sách top symbols giao dịch từ Binance

    Args:
        count: Số lượng symbols muốn lấy

    Returns:
        Danh sách tên symbol
    """
    import requests
    
    try:
        # Get 24hr ticker statistics to find top symbols by volume
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Sort by quote volume (USDT volume) and take top symbols
            sorted_symbols = sorted(
                data, 
                key=lambda x: float(x.get('quoteVolume', 0)), 
                reverse=True
            )
            
            # Filter USDT perpetual contracts and get symbol names
            usdt_symbols = [
                item['symbol'] 
                for item in sorted_symbols 
                if item['symbol'].endswith('USDT') and 'quoteVolume' in item
            ]
            
            return usdt_symbols[:count]
        else:
            print(f"❌ Failed to fetch symbols from Binance API: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Error fetching top symbols: {e}")
        return []

def main():
    """Main function"""
    global scheduler_instance
    
    # Thiết lập logging
    logger = ConfigLogging.config_logging("FundingRateMain")
    
    # Thiết lập các trình xử lý tín hiệu để tắt êm dịu
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("🚀 Starting Advanced Funding Rate Monitoring System")
    print("=" * 60)
    
    # Khởi tạo Telegram bot
    tele_bot = UtilTeleBotCheck()
    
    # Kiểm tra kết nối Telegram
    if not tele_bot.test_connection():
        print("⚠️  Telegram bot connection failed - notifications will be disabled")
    else:
        print("✅ Telegram bot connected successfully")
    
    # Lấy top symbols
    symbol_count = SYSTEM_CONFIG.get("top_symbols_count", 100)
    print(f"📊 Fetching top {symbol_count} symbols...")
    
    symbols = get_top_symbols(symbol_count)
    
    if not symbols:
        # Fallback to hardcoded symbols
        symbols = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
            'SOLUSDT', 'DOGEUSDT', 'DOTUSDT', 'MATICUSDT', 'LTCUSDT',
            'AVAXUSDT', 'LINKUSDT', 'UNIUSDT', 'ATOMUSDT', 'FILUSDT'
        ]
        print(f"⚠️  Using fallback symbols: {len(symbols)} symbols")
    else:
        print(f"✅ Successfully fetched {len(symbols)} symbols")
    
    # Tạo và khởi động scheduler
    print(f"🔧 Initializing funding rate scheduler...")
    scheduler_instance = FundingRateScheduler(symbols)
    
    # Start the scheduler
    if scheduler_instance.start_scheduler():
        print("✅ Funding rate scheduler started successfully!")
        
        # Send startup notification
        tele_bot.send_alert(
            "Funding Rate System Started",
            f"Advanced funding rate monitoring system is now running\\n\\n"
            f"🔹 Total symbols: {len(symbols)}\\n"
            f"🔹 8h symbols: {len(scheduler_instance.symbols_8h)}\\n"
            f"🔹 4h symbols: {len(scheduler_instance.symbols_4h)}\\n"
            f"🔹 Intelligent scheduling enabled\\n"
            f"🔹 Data verification enabled\\n"
            f"🔹 Conditional alerts enabled",
            "SUCCESS"
        )
        
    # Hiển thị trạng thái scheduler
        status = scheduler_instance.get_status()
        print(f"\\n📈 Scheduler Status:")
        for key, value in status.items():
            print(f"  {key}: {value}")
        
        print(f"\\n🎯 Funding cycles will be executed at:")
        if scheduler_instance.symbols_8h:
            print(f"  8h cycles: 00:00, 08:00, 16:00 UTC ({len(scheduler_instance.symbols_8h)} symbols)")
        if scheduler_instance.symbols_4h:
            print(f"  4h cycles: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC ({len(scheduler_instance.symbols_4h)} symbols)")
        
        print(f"\\n🔍 Data verification will run 5 minutes after each funding cycle")
        print(f"📱 Telegram alerts will be sent only when issues are detected")
        
        print(f"\\n🔄 System is running... Press Ctrl+C to stop")
        
        # Keep the main thread alive
        try:
            while True:
                time.sleep(60)
                
                # Periodic status check (every hour)
                if int(time.time()) % 3600 == 0:
                    status = scheduler_instance.get_status()
                    logger.info(f"Periodic status check: {status}")
        
        except KeyboardInterrupt:
            print(f"\\n🛑 Received keyboard interrupt")
            signal_handler(signal.SIGINT, None)
    
    else:
        print("❌ Failed to start funding rate scheduler")
        tele_bot.send_alert(
            "Funding Rate System Failed",
            "Failed to start the funding rate monitoring system",
            "ERROR"
        )
        sys.exit(1)

if __name__ == "__main__":
    main()