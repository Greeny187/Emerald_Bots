"""Trade API Bot - Application Setup"""

import logging
import os
from telegram.ext import Application

logger = logging.getLogger(__name__)

# Import bot modules
try:
    from . import miniapp
    from . import database
    from . import server
except ImportError as e:
    logger.error(f"Failed to import modules: {e}")
    miniapp = database = server = None


def register(app: Application):
    """Register Telegram commands + background jobs."""
    if server and hasattr(server, "register"):
        server.register(app)
        logger.info("Trade API Telegram handlers registered")

    # Optional: a dedicated /tradeapi command that just opens the miniapp
    if miniapp and hasattr(miniapp, "register_miniapp"):
        miniapp.register_miniapp(app)
        logger.info("Trade API miniapp command registered")

    if server and hasattr(server, "register_jobs"):
        server.register_jobs(app)


def register_jobs(app: Application):
    """Backwards-compatible shim."""
    if server and hasattr(server, "register_jobs"):
        server.register_jobs(app)


def init_schema():
    """Initialize database schema"""
    if database and hasattr(database, "init_schema"):
        database.init_schema()
        logger.info("Trade API database schema initialized")


def register_miniapp_routes(webapp, application: Application):
    """Expose aiohttp routes for the MiniApp (called by your main web server)."""
    if server and hasattr(server, "register_miniapp_routes"):
        server.register_miniapp_routes(webapp, application)
