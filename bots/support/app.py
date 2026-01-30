# app.py - Emerald Support Bot Main Module (v1.0 - Production Ready)
"""
Emerald Support Bot - Main entry point for telegram bot integration.
Registers handlers, jobs, and initializes database schema.
Comprehensive logging enabled for function verification.
"""

import logging
import os
import asyncio
from telegram.ext import Application

logger = logging.getLogger("bot.support")
logger.info("[INIT] Support Bot module loaded")

# Import handlers and database
from . import handlers
logger.debug("[INIT] Handlers module imported")

try:
    from . import database as sql
    logger.info("[INIT] Database module imported successfully")
except ImportError as e:
    logger.warning(f"[INIT] Could not import database module: {e}")
    sql = None


async def init_all_schemas():
    """Initialize all database schemas with comprehensive logging"""
    logger.info("[SCHEMA] Starting database schema initialization...")
    if sql:
        try:
            logger.debug("[SCHEMA] Calling db.init_all_schemas()")
            res = sql.init_all_schemas()
            if asyncio.iscoroutine(res):
                logger.debug("[SCHEMA] Awaiting coroutine result")
                await res
            logger.info("✅ [SCHEMA] Database schemas initialized successfully")
        except Exception as e:
            logger.error(f"❌ [SCHEMA] Failed to initialize schemas: {e}", exc_info=True)
            raise
    else:
        logger.warning("⚠️ [SCHEMA] SQL module not available, skipping schema initialization")


async def register(app: Application):
    """Register Support Bot handlers into main Application"""
    logger.info("[REGISTER] Starting handler registration...")
    try:
        logger.debug("[REGISTER] Initializing schemas")
        await init_all_schemas()
        logger.debug("[REGISTER] Calling handlers.register()")
        handlers.register(app)
        logger.info("✅ [REGISTER] Support Bot handlers registered successfully")
    except Exception as e:
        logger.error(f"❌ [REGISTER] Failed to register handlers: {e}", exc_info=True)
        raise


async def register_jobs(app: Application):
    """Register scheduled jobs"""
    logger.info("[JOBS] Starting job registration...")
    try:
        logger.debug("[JOBS] Calling handlers.register_jobs()")
        handlers.register_jobs(app)
        logger.info("✅ [JOBS] Support Bot jobs registered successfully")
    except Exception as e:
        logger.error(f"❌ [JOBS] Failed to register jobs: {e}", exc_info=True)


async def init_schema():
    """Initialize database schema"""
    await init_all_schemas()  # mit await