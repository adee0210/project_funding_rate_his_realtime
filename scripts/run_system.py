#!/usr/bin/env python3
"""
Tập hợp các script để chạy hệ thống Funding Rate
"""

import sys
import os
import subprocess
import time
from pathlib import Path


def print_menu():
    """In menu lựa chọn"""
    print("\n" + "=" * 60)
    print("🚀 FUNDING RATE SYSTEM MANAGER")
    print("=" * 60)
    print("1. 📊 Chạy Realtime Manager (WebSocket streams)")
    print("2. ⏰ Chạy History Scheduler (0h, 8h, 16h)")
    print("3. 🔥 Chạy cả hai (Full System)")
    print("4. 🛑 Thoát")
    print("=" * 60)


def run_realtime():
    """Chạy realtime manager"""
    script_path = Path(__file__).parent / "run_realtime.py"
    print("🚀 Starting Realtime Manager...")
    subprocess.run([sys.executable, str(script_path)])


def run_scheduler():
    """Chạy history scheduler"""
    script_path = Path(__file__).parent / "run_scheduler.py"
    print("🚀 Starting History Scheduler...")
    subprocess.run([sys.executable, str(script_path)])


def run_full_system():
    """Chạy cả hai processes"""
    print("🚀 Starting Full System...")
    print("📊 Realtime: Top 100 symbols WebSocket monitoring")
    print("⏰ History: Scheduled extraction at 0h, 8h, 16h")
    print("💾 Data: Separate realtime + history collections")
    print("🛑 Press Ctrl+C to stop")
    print("-" * 60)

    try:
        # Start realtime in background
        realtime_script = Path(__file__).parent / "run_realtime.py"
        realtime_process = subprocess.Popen([sys.executable, str(realtime_script)])

        # Start scheduler in background
        scheduler_script = Path(__file__).parent / "run_scheduler.py"
        scheduler_process = subprocess.Popen([sys.executable, str(scheduler_script)])

        print("✅ Both processes started successfully!")
        print("🔍 Check logs for detailed status...")

        # Wait for processes
        try:
            realtime_process.wait()
            scheduler_process.wait()
        except KeyboardInterrupt:
            print("\n🛑 Stopping both processes...")
            realtime_process.terminate()
            scheduler_process.terminate()
            print("✅ System stopped!")

    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    """Main function"""
    while True:
        try:
            print_menu()
            choice = input("👉 Chọn lựa chọn (1-4): ").strip()

            if choice == "1":
                run_realtime()
            elif choice == "2":
                run_scheduler()
            elif choice == "3":
                run_full_system()
            elif choice == "4":
                print("👋 Tạm biệt!")
                break
            else:
                print("❌ Lựa chọn không hợp lệ!")

        except KeyboardInterrupt:
            print("\n👋 Tạm biệt!")
            break
        except Exception as e:
            print(f"❌ Lỗi: {e}")


if __name__ == "__main__":
    main()
