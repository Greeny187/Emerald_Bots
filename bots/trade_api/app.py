"""Trade API Bot - Application Setup"""

import logging
import os
from telegram.ext import Application

logger = logging.getLogger(__name__)

# Import bot modules
try:
    from . import handlers
    from . import miniapp
    from . import database
except ImportError as e:
    logger.error(f"Failed to import modules: {e}")
    handlers = miniapp = database = None


def register(app: Application):
    """Register all Trade API handlers"""
    if handlers and hasattr(handlers, "register_handlers"):
        handlers.register_handlers(app)
        logger.info("Trade API handlers registered")
    
    if miniapp and hasattr(miniapp, "register_miniapp"):
        miniapp.register_miniapp(app)
        logger.info("Trade API miniapp registered")


def register_jobs(app: Application):
    """Register background jobs"""
    # Job: Check for triggered alerts
    # Job: Update market data
    # Job: Check portfolio health
    pass


def init_schema():
    """Initialize database schema"""
    if database and hasattr(database, "init_all_schemas"):
        database.init_all_schemas()
        logger.info("Trade API database schema initialized")
