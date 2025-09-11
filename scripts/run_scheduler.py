#!/usr/bin/env python3
"""
Script để chạy scheduler 8h riêng biệt
Chạy vào 0h, 8h, 16h mỗi ngày để trích xuất data vào history collections
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from scheduler.scheduler_8h import FundingRateScheduler
from src.config.config_logging import ConfigLogging


def main():
    """Main entry point cho history scheduler"""
    logger = ConfigLogging.config_logging("SchedulerRunner")
    logger.info("Starting Funding Rate History Scheduler")

    try:
        # Khởi tạo và chạy scheduler
        scheduler = FundingRateScheduler()
        scheduler.start_scheduler()

    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user (Ctrl+C)")
        print("\nHistory Scheduler stopped by user")

    except Exception as e:
        logger.error(f"Error in scheduler: {e}")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    print("Starting Funding Rate History Scheduler...")
    print("Schedule: Every 8 hours (00:00, 08:00, 16:00)")
    print("Data will be saved to history collections")
    print("Press Ctrl+C to stop")
    print("-" * 50)

    main()
