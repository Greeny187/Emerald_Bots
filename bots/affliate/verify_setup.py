#!/usr/bin/env python3
"""
Affiliate Bot Setup Verification Script
========================================

Überprüfung dass alle notwendigen Komponenten für den Affiliate Bot
vorhanden und konfiguriert sind.

Usage:
    python verify_setup.py

Should output:
    ✅ All checks passed! Ready to start bot with: python bot.py
"""

import sys
from pathlib import Path

class SetupVerifier:
    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = []

    def check(self, condition, message, critical=False):
        """Run a check and print result"""
        if condition:
            print(f"✅ {message}")
            self.checks_passed += 1
            return True
        else:
            if critical:
                print(f"❌ {message}")
            else:
                print(f"⚠️  {message}")
            self.checks_failed += 1
            return False

    def run_all_checks(self):
        """Run all verification checks"""
        print("\n" + "="*60)
        print("🔍 AFFILIATE BOT SETUP VERIFICATION")
        print("="*60 + "\n")

        # 1. Environment & Config
        print("📋 1. ENVIRONMENT & CONFIG")
        print("-" * 40)
        
        env_file = Path(".env")
        self.check(env_file.exists(), ".env file exists")
        
        if env_file.exists():
            env_content = env_file.read_text()
            self.check("TELEGRAM_BOT_TOKEN=" in env_content, "  ├─ TELEGRAM_BOT_TOKEN set")
            self.check("DATABASE_URL=" in env_content, "  ├─ DATABASE_URL set")
            self.check("AFFILIATE_API_BASE_URL=" in env_content, "  ├─ AFFILIATE_API_BASE_URL set (CRITICAL)")
            self.check("BOT8_USERNAME=" in env_content, "  └─ BOT8_USERNAME set")

        # 2. Python Modules
        print("\n📦 2. REQUIRED PYTHON MODULES")
        print("-" * 40)
        
        required_modules = {
            "telegram": "python-telegram-bot",
            "aiohttp": "aiohttp",
            "psycopg2": "psycopg2-binary",
            "dotenv": "python-dotenv",
        }
        
        for module_name, package_name in required_modules.items():
            try:
                __import__(module_name)
                self.check(True, f"  ├─ {package_name} installed")
            except ImportError:
                self.check(False, f"  ├─ {package_name} NOT installed", critical=True)
                self.warnings.append(f"Install with: pip install {package_name}")
        
        # 3. Directory Structure
        print("\n📁 3. DIRECTORY STRUCTURE")
        print("-" * 40)
        
        directories = {
            "Frontend HTML": "../../miniapp/appaffiliate.html",
            "Backend app.py": "app.py",
            "Backend database.py": "database.py",
            "Backend miniapp.py": "miniapp.py",
            "Backend handlers.py": "handlers.py",
            "Config file": "config.py",
        }
        
        for name, path in directories.items():
            full_path = Path(path)
            self.check(full_path.exists(), f"  ├─ {name}: {path}")

        # 4. Code Quality
        print("\n🔧 4. CODE QUALITY CHECKS")
        print("-" * 40)
        
        # Check miniapp.py has critical functions
        if Path("miniapp.py").exists():
            miniapp_content = Path("miniapp.py").read_text()
            self.check("ensure_commission_row" in miniapp_content, "  ├─ ensure_commission_row imported")
            self.check("get_stats" in miniapp_content, "  ├─ get_stats function exists")
            self.check("get_referral_list" in miniapp_content, "  ├─ get_referral_list function exists")
            self.check("get_tonconnect_manifest" in miniapp_content, "  ├─ TON Connect manifest route exists")
            self.check("async def get_pending" in miniapp_content, "  └─ get_pending function defined properly")
        
        # Check handlers.py structure
        if Path("handlers.py").exists():
            handlers_content = Path("handlers.py").read_text()
            self.check("build_miniapp_url" in handlers_content, "  ├─ build_miniapp_url function exists")
            self.check("WebAppInfo" in handlers_content, "  ├─ WebAppInfo imported (for dashboard button)")
            self.check("AFFILIATE_API_BASE_URL" in handlers_content, "  ├─ AFFILIATE_API_BASE_URL configured")
            self.check("ensure_commission_row" in handlers_content, "  └─ Commission row creation implemented")

        # 5. Frontend Configuration
        print("\n🌐 5. FRONTEND CONFIGURATION")
        print("-" * 40)
        
        html_path = Path("../../miniapp/appaffiliate.html")
        if html_path.exists():
            html_content = html_path.read_text()
            self.check("tonconnect-ui" in html_content, "  ├─ TON Connect UI library imported")
            self.check("fetchStats" in html_content, "  ├─ fetchStats function exists")
            self.check("getApiBase" in html_content, "  ├─ API Base detection implemented")
            self.check("tgWebAppData" in html_content, "  └─ Telegram data fallback implemented")

        # 6. Database
        print("\n🗄️  6. DATABASE CONFIGURATION")
        print("-" * 40)
        
        if Path("database.py").exists():
            db_content = Path("database.py").read_text()
            tables = [
                "aff_commissions",
                "aff_referrals",
                "aff_conversions",
                "aff_payouts",
                "aff_user_referrers",
            ]
            for table in tables:
                self.check(f'"{table}"' in db_content, f"  ├─ {table} table defined")

        # 7. Summary
        print("\n" + "="*60)
        print("📊 VERIFICATION SUMMARY")
        print("="*60)
        print(f"✅ Passed: {self.checks_passed}")
        print(f"❌ Failed: {self.checks_failed}")
        
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  • {warning}")
        
        print("\n" + "-"*60)
        
        if self.checks_failed == 0:
            print("✨ All checks passed! Your setup is ready.\n")
            print("🚀 To start the bot, run:")
            print("   python bot.py\n")
            return True
        else:
            print("❌ Some checks failed. Please fix the issues above.\n")
            return False

def main():
    verifier = SetupVerifier()
    success = verifier.run_all_checks()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

