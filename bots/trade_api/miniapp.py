"""Trade API Bot - MiniApp Integration

This file keeps the MiniApp entrypoint (/tradeapi) and delegates all HTTP routes
to server.py (single source of truth).
"""

import logging

from aiohttp import web
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import Application, CommandHandler

from .config import MINIAPP_URL
from . import server

logger = logging.getLogger(__name__)


async def cmd_open_tradeapi(update, context):
    """Open the Trade API MiniApp."""
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📊 Trade API öffnen", web_app=WebAppInfo(url=MINIAPP_URL))]]
    )
    await update.message.reply_text(
        "📊 **Trade API MiniApp**\n\nKlicke auf den Button, um zu starten.",
        reply_markup=keyboard,
        parse_mode="Markdown",
    )


def register_miniapp(app: Application):
    """Register the /tradeapi command."""
    app.add_handler(CommandHandler("tradeapi", cmd_open_tradeapi))


async def register_miniapp_routes(webapp: web.Application, app: Application):
    """Register all HTTP API routes for the miniapp.

    Backwards compatible with older code that calls this async function.
    """
    server.register_miniapp_routes(webapp, app)
    logger.info("Trade API miniapp routes registered")