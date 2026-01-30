"""Learning Bot - Application Setup"""

import logging
from telegram.ext import Application

logger = logging.getLogger("bot.learning")
logger.info("[INIT] Learning Bot module loaded")

try:
    from . import handlers
    from . import miniapp
    from . import database
    logger.debug("[INIT] All modules imported successfully")
except ImportError as e:
    logger.error(f"[INIT] Failed to import modules: {e}")
    handlers = miniapp = database = None


def register(app: Application):
    """Register all Learning handlers"""
    logger.info("[REGISTER] Starting Learning Bot handler registration...")
    
    if handlers and hasattr(handlers, "register_handlers"):
        try:
            logger.debug("[REGISTER] Registering handlers")
            handlers.register_handlers(app)
            logger.info("✅ [REGISTER] Learning handlers registered")
        except Exception as e:
            logger.error(f"❌ [REGISTER] Failed to register handlers: {e}", exc_info=True)
    
    if miniapp and hasattr(miniapp, "register_miniapp"):
        try:
            logger.debug("[REGISTER] Registering miniapp")
            miniapp.register_miniapp(app)
            logger.info("✅ [REGISTER] Learning miniapp registered")
        except Exception as e:
            logger.error(f"❌ [REGISTER] Failed to register miniapp: {e}", exc_info=True)


def register_jobs(app: Application):
    """Register background jobs"""
    logger.info("[JOBS] Learning Bot has no scheduled jobs")


def init_schema():
    """Initialize database schema"""
    logger.info("[SCHEMA] Initializing Learning Bot database schema...")
    if database and hasattr(database, "init_all_schemas"):
        try:
            logger.debug("[SCHEMA] Calling database.init_all_schemas()")
            database.init_all_schemas()
            logger.info("✅ [SCHEMA] Learning database schema initialized successfully")
        except Exception as e:
            logger.error(f"❌ [SCHEMA] Failed to initialize schema: {e}", exc_info=True)
