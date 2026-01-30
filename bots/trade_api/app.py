"""Trade API Bot - Application Setup"""

import logging
import os
from telegram.ext import Application

logger = logging.getLogger("bot.trade_api")
logger.info("[INIT] Trade API Bot module loaded")

# Import bot modules
try:
    from . import handlers
    from . import miniapp
    from . import database
    logger.debug("[INIT] All modules imported successfully")
except ImportError as e:
    logger.error(f"[INIT] Failed to import modules: {e}")
    handlers = miniapp = database = None


def register(app: Application):
    """Register all Trade API handlers"""
    logger.info("[REGISTER] Starting Trade API Bot handler registration...")
    
    if handlers and hasattr(handlers, "register_handlers"):
        try:
            logger.debug("[REGISTER] Registering handlers")
            handlers.register_handlers(app)
            logger.info("✅ [REGISTER] Trade API handlers registered")
        except Exception as e:
            logger.error(f"❌ [REGISTER] Failed to register handlers: {e}", exc_info=True)
    
    if miniapp and hasattr(miniapp, "register_miniapp"):
        try:
            logger.debug("[REGISTER] Registering miniapp")
            miniapp.register_miniapp(app)
            logger.info("✅ [REGISTER] Trade API miniapp registered")
        except Exception as e:
            logger.error(f"❌ [REGISTER] Failed to register miniapp: {e}", exc_info=True)


def register_jobs(app: Application):
    """Register background jobs"""
    logger.info("[JOBS] Registering Trade API Bot background jobs...")
    logger.debug("[JOBS] Available jobs: market_data_update, alert_check, portfolio_health")
    # Job: Check for triggered alerts
    # Job: Update market data
    # Job: Check portfolio health
    logger.info("[JOBS] Trade API Bot job registration complete")


def init_schema():
    """Initialize database schema"""
    logger.info("[SCHEMA] Initializing Trade API Bot database schema...")
    if database and hasattr(database, "init_all_schemas"):
        try:
            logger.debug("[SCHEMA] Calling database.init_all_schemas()")
            database.init_all_schemas()
            logger.info("✅ [SCHEMA] Trade API database schema initialized successfully")
        except Exception as e:
            logger.error(f"❌ [SCHEMA] Failed to initialize schema: {e}", exc_info=True)
