
import sys
import signal
import time
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scheduler.advanced_funding_scheduler import AdvancedFundingRateScheduler
from src.utils.util_tele_bot_check import UtilTeleBotCheck
from src.config.config_logging import ConfigLogging
from src.config.config_variable import SYSTEM_CONFIG

# Global scheduler instance for signal handling
scheduler_instance = None

def signal_handler(signum, frame):
    """Xử lý tín hiệu tắt một cách nhẹ nhàng"""
    global scheduler_instance
    print(f"\nReceived signal {signum}, shutting down gracefully...")
    
    if scheduler_instance:
        scheduler_instance.stop_scheduler()
    
    print("👋 Advanced funding rate scheduler stopped. Goodbye!")
    sys.exit(0)

def get_top_symbols(count: int = 100) -> list:
    """Lấy các symbol giao dịch hàng đầu từ Binance"""
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
            print(f"Failed to fetch symbols from Binance API: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Error fetching top symbols: {e}")
        return []

def main():
    """Hàm chính (entrypoint)"""
    global scheduler_instance
    
    # Thiết lập logging
    logger = ConfigLogging.config_logging("AdvancedFundingMain")
    
    # Thiết lập trình xử lý tín hiệu để tắt nhẹ nhàng
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Starting Advanced Multi-Interval Funding Rate System")
    print("=" * 70)
    
    # Khởi tạo Telegram bot
    tele_bot = UtilTeleBotCheck()
    
    # Kiểm tra kết nối Telegram
    if not tele_bot.test_connection():
        print("Telegram bot connection failed - notifications will be disabled")
    else:
        print("Telegram bot connected successfully")
    
    # Lấy danh sách symbol hàng đầu
    symbol_count = SYSTEM_CONFIG.get("top_symbols_count", 100)
    print(f" Fetching top {symbol_count} symbols...")
    
    symbols = get_top_symbols(symbol_count)
    
    if not symbols:
        # Fallback to hardcoded symbols
        symbols = [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
            'SOLUSDT', 'DOGEUSDT', 'DOTUSDT', 'MATICUSDT', 'LTCUSDT',
            'AVAXUSDT', 'LINKUSDT', 'UNIUSDT', 'ATOMUSDT', 'FILUSDT',
            'SHIBUSDT', 'TRXUSDT', 'NEARUSDT', 'APTUSDT', 'OPUSDT'
        ]
        print(f"Using fallback symbols: {len(symbols)} symbols")
    else:
        print(f" Successfully fetched {len(symbols)} symbols")
    
    # Tạo và khởi động bộ lập lịch nâng cao
    print(f" Initializing advanced funding rate scheduler...")
    scheduler_instance = AdvancedFundingRateScheduler(symbols)
    
    # Start the scheduler
    if scheduler_instance.start_scheduler():
        print(" Advanced funding rate scheduler started successfully!")
        
    # Gửi thông báo khởi động
        tele_bot.send_alert(
            "Advanced Funding Rate System Started",
            f"Multi-interval funding rate monitoring system is now active\\n\\n"
            f" Total symbols: {len(symbols)}\\n"
            f" 1h monitoring: {len(scheduler_instance.symbols_1h)} symbols\\n"
            f" 4h funding cycles: {len(scheduler_instance.symbols_4h)} symbols\\n"
            f" 8h funding cycles: {len(scheduler_instance.symbols_8h)} symbols\\n"
            f" Real-time data monitoring: \\n"
            f" Multi-interval verification: \\n"
            f" Intelligent notifications: ",
            "SUCCESS"
        )
        
    # Hiển thị trạng thái bộ lập lịch
        status = scheduler_instance.get_status()
        print(f"\\n Advanced Scheduler Status:")
        for key, value in status.items():
            print(f"  {key}: {value}")
        
        print(f"\\n Multi-Interval Schedule:")
        print(f"   1h monitoring: Every hour (real-time data)")
        if scheduler_instance.symbols_4h:
            print(f"   4h funding: 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC ({len(scheduler_instance.symbols_4h)} symbols)")
        if scheduler_instance.symbols_8h:
            print(f"   8h funding: 00:00, 08:00, 16:00 UTC ({len(scheduler_instance.symbols_8h)} symbols)")
        
        print(f"\\n🔍 Verification runs 5 minutes after each cycle")
        print(f" Smart alerts: Only sent when issues detected")
        print(f"  Adaptive thresholds for different intervals")
        
        print(f"\\n Advanced system is running... Press Ctrl+C to stop")
        
    # Giữ luồng chính chạy liên tục
        try:
            while True:
                time.sleep(60)
                
                # Periodic status check (every 6 hours)
                if int(time.time()) % 21600 == 0:
                    status = scheduler_instance.get_status()
                    logger.info(f"Periodic status check: {status}")
        
        except KeyboardInterrupt:
            print(f"\\n Received keyboard interrupt")
            signal_handler(signal.SIGINT, None)
    
    else:
        tele_bot.send_alert(
            "Advanced Funding Rate System Failed",
            "Failed to start the advanced funding rate monitoring system",
            "ERROR"
        )
        sys.exit(1)

if __name__ == "__main__":
    main()