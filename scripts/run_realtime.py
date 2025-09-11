#!/usr/bin/env python3
"""
Script để chạy realtime manager riêng biệt
Chỉ xử lý realtime WebSocket streams cho top 100 coins
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.funding_rate_manager import FundingRateManager
from src.config.config_logging import ConfigLogging


def main():
    """Main entry point cho realtime manager"""
    logger = ConfigLogging.config_logging("RealtimeRunner")
    logger.info("Starting Funding Rate Realtime Manager")

    try:
        # Khởi tạo và chạy realtime manager
        manager = FundingRateManager()

        if manager.start():
            logger.info("Realtime manager started successfully")
            # Keep running until interrupted
            try:
                while manager.is_running:
                    import time

                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Stopping realtime manager...")
                manager.stop()
        else:
            logger.error("Failed to start realtime manager")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Realtime manager stopped by user (Ctrl+C)")
        print("\n📊 Realtime Manager stopped by user")

    except Exception as e:
        logger.error(f"Error in realtime manager: {e}")
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("🚀 Starting Funding Rate Realtime Manager...")
    print("📊 Processing top 100 symbols in realtime")
    print("🌐 WebSocket streams for live funding rate data")
    print("⏰ History extraction handled by separate scheduler")
    print("🛑 Press Ctrl+C to stop")
    print("-" * 50)

    main()
