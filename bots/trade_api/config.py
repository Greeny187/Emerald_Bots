import os

BOT_TOKEN = os.getenv("TRADE_API_BOT_TOKEN") or os.getenv("BOT2_TOKEN") or os.getenv("BOT_TRADE_API_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
APP_BASE_URL = (os.getenv("APP_BASE_URL") or "").rstrip("/")
TELEGRAM_LOGIN_TTL_SECONDS = int(os.getenv("TELEGRAM_LOGIN_TTL_SECONDS", "86400"))

# MiniApp URL (falls du NICHT /static/apptradeapi.html vom eigenen Server auslieferst)
MINIAPP_URL = os.getenv(
    "MINIAPP_URL",
    "https://greeny187.github.io/EmeraldContentBots/miniapp/apptradeapi.html",
)

# Runtime / API Tuning
API_REQUEST_TIMEOUT_SECONDS = float(os.getenv("API_REQUEST_TIMEOUT_SECONDS", "12"))
MARKET_CACHE_TTL_SECONDS = int(os.getenv("MARKET_CACHE_TTL_SECONDS", "60"))
ALERT_CHECK_INTERVAL_SECONDS = int(os.getenv("ALERT_CHECK_INTERVAL_SECONDS", "60"))
SIGNAL_ATR_MULT_STOP = float(os.getenv("SIGNAL_ATR_MULT_STOP", "1.5"))
SIGNAL_ATR_MULT_TP = float(os.getenv("SIGNAL_ATR_MULT_TP", "3.0"))

# Free vs Pro
PRO_DEFAULT = os.getenv("PRO_DEFAULT", "false").lower() in ("1","true","yes","y")
PRO_USERS = {int(x) for x in (os.getenv("PRO_USERS","").split(",") if os.getenv("PRO_USERS") else [])}

ALLOWED_PROVIDERS = [
    {"id": "kraken",   "name": "Kraken"},
    {"id": "coinbase", "name": "Coinbase"},
    {"id": "mexc",     "name": "MEXC"},
]
