#!/usr/bin/env python3
"""
Test script cho hệ thống Funding Rate Manager đã được cập nhật với scheduler nâng cao
Kiểm tra toàn bộ hệ thống funding rate manager
"""

import sys
import os
import time
from datetime import datetime, timezone

# Add the project root to Python path
sys.path.append('/home/duc_le/project_funding_rate_his_realtime')

from src.funding_rate_manager import FundingRateManager
from scheduler.advanced_funding_scheduler import AdvancedFundingRateScheduler


def test_advanced_scheduler_standalone():
    """Kiểm tra scheduler nâng cao độc lập"""
    print("Testing Advanced Scheduler (Standalone)...")
    print("=" * 60)
    
    test_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT']
    
    scheduler = AdvancedFundingRateScheduler(test_symbols)
    
    print(f"Testing with {len(test_symbols)} symbols...")
    
    # Kiểm tra phân loại
    scheduler._categorize_symbols()
    
    print(f"Categorization Results:")
    print(f"  1h monitoring: {len(scheduler.symbols_1h)} symbols")
    print(f"  4h funding: {len(scheduler.symbols_4h)} symbols")
    print(f"  8h funding: {len(scheduler.symbols_8h)} symbols")
    
    # Kiểm tra thiết lập lịch
    try:
        scheduler._setup_funding_schedules()
        scheduler._setup_verification_schedules()
        print("✅ Schedule setup successful")
    except Exception as e:
        print(f"❌ Schedule setup failed: {e}")
        return False
    
    # Lấy trạng thái
    status = scheduler.get_status()
    print(f"\\nScheduler Status:")
    for key, value in status.items():
        print(f"  {key}: {value}")
    
    return True


def test_funding_rate_manager():
    """Kiểm tra Funding Rate Manager đã được cập nhật"""
    print("\\nKiểm tra Funding Rate Manager đã cập nhật...")
    print("=" * 60)
    
    try:
    # Tạo instance manager
        manager = FundingRateManager()
        
        print("✅ Manager created successfully")
        
    # Kiểm tra khởi tạo (không start toàn bộ hệ thống)
        if manager.initialize():
            print("✅ Manager initialized successfully")
            print(f"  Loaded {len(manager.symbols)} symbols")
        else:
            print("❌ Manager initialization failed")
            return False
        
    # Kiểm tra trạng thái trước khi start
        status = manager.get_status()
        print(f"\\nManager Status (before start):")
        print(f"  is_running: {status.get('is_running')}")
        print(f"  symbols_count: {status.get('symbols_count')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Manager test failed: {e}")
        return False


def test_system_integration():
    """Kiểm tra tích hợp hệ thống mà không cần start đầy đủ"""
    print("\\nKiểm tra tích hợp hệ thống...")
    print("=" * 60)
    
    try:
    # Kiểm tra tạo scheduler nâng cao với các symbol từ manager
        manager = FundingRateManager()
        manager.initialize()
        
    # Tạo scheduler với các symbol lấy từ manager
        test_symbols = manager.symbols[:10]  # Use first 10 symbols
        scheduler = AdvancedFundingRateScheduler(test_symbols)
        
        print(f"Testing integration with {len(test_symbols)} symbols...")
        
    # Kiểm tra phân loại
        scheduler._categorize_symbols()
        
        print(f"Integration Results:")
        print(f"  Manager symbols loaded: {len(manager.symbols)}")
        print(f"  Scheduler symbols: {len(test_symbols)}")
        print(f"  1h monitoring: {len(scheduler.symbols_1h)}")
        print(f"  4h funding: {len(scheduler.symbols_4h)}")
        print(f"  8h funding: {len(scheduler.symbols_8h)}")
        
    # Xác minh phân loại đúng (phần lớn sẽ là 8h)
        if len(scheduler.symbols_8h) > 0 and len(scheduler.symbols_1h) == len(test_symbols):
            print("✅ Symbol categorization working correctly")
            print(f"  All symbols get 1h monitoring: {len(scheduler.symbols_1h) == len(test_symbols)}")
            print(f"  Most symbols use 8h funding: {len(scheduler.symbols_8h) > len(scheduler.symbols_4h)}")
            return True
        else:
            print("❌ Symbol categorization has issues")
            return False
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False


def test_telegram_integration():
    """Kiểm tra tích hợp Telegram với hệ thống mới"""
    print("\\nKiểm tra tích hợp Telegram...")
    print("=" * 60)
    
    try:
        from src.utils.util_tele_bot_check import UtilTeleBotCheck
        
        tele_bot = UtilTeleBotCheck()
        
        if not tele_bot.test_connection():
            print("⚠️  Telegram not configured, skipping notification tests")
            return True
        
        print("✅ Telegram connection successful")
        
    # Kiểm tra các phương thức thông báo mới
        print("📤 Testing advanced system notification...")
        result = tele_bot.send_alert(
            "Advanced System Test",
            "Testing the updated funding rate system\\n\\n"
            "✅ Advanced scheduler integrated\\n"
            "✅ Multi-interval support (1h, 4h, 8h)\\n" 
            "✅ Intelligent notifications enabled",
            "SUCCESS"
        )
        
        print(f"   Notification result: {'✅ Success' if result else '❌ Failed'}")
        return result
        
    except Exception as e:
        print(f"❌ Telegram integration test failed: {e}")
        return False


def main():
    """Chạy toàn bộ các bài kiểm tra"""
    print("🚀 Bắt đầu kiểm tra hệ thống Funding Rate đã cập nhật")
    print("=" * 80)
    
    test_results = []
    
    # Test 1: Advanced scheduler standalone
    try:
        result1 = test_advanced_scheduler_standalone()
        test_results.append(("Advanced Scheduler", result1))
    except Exception as e:
        print(f"❌ Advanced scheduler test error: {e}")
        test_results.append(("Advanced Scheduler", False))
    
    # Test 2: Updated funding rate manager
    try:
        result2 = test_funding_rate_manager()
        test_results.append(("Updated Manager", result2))
    except Exception as e:
        print(f"❌ Manager test error: {e}")
        test_results.append(("Updated Manager", False))
    
    # Test 3: System integration
    try:
        result3 = test_system_integration()
        test_results.append(("System Integration", result3))
    except Exception as e:
        print(f"❌ Integration test error: {e}")
        test_results.append(("System Integration", False))
    
    # Test 4: Telegram integration
    try:
        result4 = test_telegram_integration()
        test_results.append(("Telegram Integration", result4))
    except Exception as e:
        print(f"❌ Telegram test error: {e}")
        test_results.append(("Telegram Integration", False))
    
    # Summary
    print(f"\\n" + "=" * 80)
    print("📊 COMPLETE SYSTEM TEST SUMMARY")
    print("=" * 80)
    
    passed = 0
    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<25}: {status}")
        if result:
            passed += 1
    
    print(f"\\nOverall: {passed}/{len(test_results)} tests passed")
    
    if passed == len(test_results):
        print("🎉 All tests passed! The updated system is ready.")
        print("\\n📋 Summary of changes:")
        print("  ✅ Replaced old scheduler with advanced multi-interval scheduler")
        print("  ✅ Added 1h monitoring for real-time data")
        print("  ✅ Fixed symbol categorization (now shows correct 8h/4h/1h counts)")
        print("  ✅ Integrated intelligent notifications")
        print("  ✅ Updated system monitoring to show accurate status")
        print("\\n🚀 System now supports: 1h monitoring + 4h/8h funding cycles")
    else:
        print("💥 Some tests failed. Please check the implementation.")
        sys.exit(1)


if __name__ == "__main__":
    main()