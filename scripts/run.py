#!/usr/bin/env python3
"""
Script chính để chạy Funding Rate Manager

Sử dụng:
    python scripts/run.py start    # Bắt đầu hệ thống
    python scripts/run.py stop     # Dừng hệ thống
    python scripts/run.py restart  # Khởi động lại hệ thống
    python scripts/run.py status   # Lấy trạng thái hệ thống
    python scripts/run.py run      # Chạy vĩnh viễn (chặn)
"""

import sys
import os
import json
import time
import argparse
from pathlib import Path

# Thêm thư mục src vào Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from funding_rate_manager import FundingRateManager
from config.config_logging import ConfigLogging
from config.config_variable import SYSTEM_CONFIG


def main():
    """Hàm chính của script"""
    parser = argparse.ArgumentParser(description="Funding Rate Manager")
    parser.add_argument(
        "command",
        choices=["start", "stop", "restart", "status", "run", "test"],
        nargs="?",  # Make command optional
        default="run",  # Default to 'run' command
        help="Command to execute (default: run)",
    )
    parser.add_argument(
        "--symbols",
        type=int,
        default=100,
        help="Number of top symbols to monitor (default: 100)",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=30,
        help="Number of days of history to extract initially (default: 30)",
    )

    args = parser.parse_args()

    # Thiết lập logging
    logger = ConfigLogging.config_logging("MainScript")

    try:
        # Khởi tạo instance manager với giá trị cấu hình
        manager = FundingRateManager()
        manager.top_symbols_count = SYSTEM_CONFIG["top_symbols_count"]

        print(f"Funding Rate Manager")
        print(f"Monitoring top {manager.top_symbols_count} symbols")
        print(f"Using configuration from config_variable.py")
        print()

        if args.command == "start":
            logger.info("Starting Funding Rate Manager...")
            success = manager.start()
            if success:
                print("Funding Rate Manager started successfully!")
                print("Use 'python scripts/run.py status' to check status")
                print("Use 'python scripts/run.py stop' to stop the system")
            else:
                print("Failed to start Funding Rate Manager")
                sys.exit(1)

        elif args.command == "stop":
            logger.info("Stopping Funding Rate Manager...")
            success = manager.stop()
            if success:
                print("Funding Rate Manager stopped successfully!")
            else:
                print("Failed to stop Funding Rate Manager")
                sys.exit(1)

        elif args.command == "restart":
            logger.info("Restarting Funding Rate Manager...")
            success = manager.restart()
            if success:
                print("Funding Rate Manager restarted successfully!")
            else:
                print("Failed to restart Funding Rate Manager")
                sys.exit(1)

        elif args.command == "status":
            logger.info("Getting system status...")
            status = manager.get_status()

            print("\nFunding Rate Manager Status")
            print("=" * 40)
            print(json.dumps(status, indent=2, default=str))

        elif args.command == "run":
            logger.info("Running Funding Rate Manager forever...")
            print("Starting Funding Rate Manager...")
            print("Press Ctrl+C to stop")
            manager.run_forever()

        elif args.command == "test":
            logger.info("Testing system components...")
            print("Testing Funding Rate Manager components...")

            # Kiểm tra kết nối MongoDB
            try:
                from load.load_mongo import LoadMongo

                load_mongo = LoadMongo()
                stats = load_mongo.get_funding_rate_stats()
                print("MongoDB connection: OK")
            except Exception as e:
                print(f"MongoDB connection: FAILED - {e}")

            # Kiểm tra Binance API
            try:
                from extract.extract_history import ExtractFundingRateHistory

                extract_history = ExtractFundingRateHistory()
                symbols = extract_history.get_top_symbols(5)
                if symbols:
                    print(f"Binance API: OK - Got {len(symbols)} symbols")
                else:
                    print("Binance API: FAILED - No symbols returned")
            except Exception as e:
                print(f"Binance API: FAILED - {e}")

            # Kiểm tra Telegram bot
            try:
                from utils.util_tele_bot_check import UtilTeleBotCheck

                tele_bot = UtilTeleBotCheck()
                if tele_bot.test_connection():
                    print("Telegram Bot: OK")
                else:
                    print("Telegram Bot: Not configured or failed")
            except Exception as e:
                print(f"Telegram Bot: FAILED - {e}")

            print("\nTest completed!")

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        manager.stop()
        print("\nShutting down gracefully...")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
