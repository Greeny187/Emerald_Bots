"""Trade API Bot - Mini-App Integration"""

import logging
import os
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import CommandHandler, Application

logger = logging.getLogger(__name__)

MINIAPP_URL = os.getenv(
    "MINIAPP_URL",
    "https://greeny187.github.io/EmeraldContentBots/miniapp/apptradeapi.html"
)


async def cmd_open_tradeapi(update, context):
    """Open Trade API Mini-App"""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "📊 Trade API öffnen",
            web_app=WebAppInfo(url=MINIAPP_URL)
        )]
    ])
    await update.message.reply_text(
        "📊 **Trade API Mini-App**",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )


def register_miniapp(app: Application):
    """Register miniapp command"""
    app.add_handler(CommandHandler("tradeapi", cmd_open_tradeapi))
    
    logger.info("Trade API miniapp routes registered")
