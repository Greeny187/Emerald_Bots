import os

BOT_TOKEN = os.getenv("TRADE_API_BOT_TOKEN") or os.getenv("BOT2_TOKEN") or os.getenv("BOT_TRADE_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
APP_BASE_URL = (os.getenv("APP_BASE_URL") or "").rstrip("/")
TELEGRAM_LOGIN_TTL_SECONDS = int(os.getenv("TELEGRAM_LOGIN_TTL_SECONDS", "86400"))

# Free vs Pro
PRO_DEFAULT = os.getenv("PRO_DEFAULT", "false").lower() in ("1","true","yes","y")
PRO_USERS = {int(x) for x in (os.getenv("PRO_USERS","").split(",") if os.getenv("PRO_USERS") else [])}

ALLOWED_PROVIDERS = [
    {"id": "kraken",   "name": "Kraken"},
    {"id": "coinbase", "name": "Coinbase"},
    {"id": "mexc",     "name": "MEXC"},
]
