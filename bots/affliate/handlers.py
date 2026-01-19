"""Affiliate Bot - Referral System"""

import logging
from multiprocessing import context
import os
from turtle import update
from urllib.parse import quote
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler
from .database import (
    create_referral, get_referral_stats, get_tier_info, 
    verify_ton_wallet, request_payout, get_pending_payouts, ensure_commission_row
)

logger = logging.getLogger(__name__)

try:
    from shared.emrd_rewards_integration import award_points
except ImportError:
    async def award_points(*args, **kwargs):
        pass

# Configuration
AFFILIATE_BOT_USERNAME = os.getenv("AFFILIATE_BOT_USERNAME", os.getenv("BOT_USERNAME", "emerald_affiliate_bot")).lstrip("@")
AFFILIATE_API_BASE_URL = os.getenv("AFFILIATE_API_BASE_URL", "").rstrip("/")
AFFILIATE_MINIAPP_URL = os.getenv(
    "AFFILIATE_MINIAPP_URL",
    "https://greeny187.github.io/EmeraldContentBots/miniapp/appaffiliate.html"
)
MINIMUM_PAYOUT = int(os.getenv("AFFILIATE_MINIMUM_PAYOUT", "1000"))  # EMRD
EMRD_CONTRACT = os.getenv("EMRD_CONTRACT", "EQA0rJDTy_2sS30KxQW8HO0_ERqmOGUhMWlwdL-2RpDmCrK5")


def build_referral_link(referrer_id: int) -> str:
    """Build Telegram deep link for the affiliate bot."""
    return f"https://t.me/{AFFILIATE_BOT_USERNAME}?start=aff_{referrer_id}"


def build_miniapp_url() -> str:
    """Optionally append ?api=<backend> so the static GitHub Pages miniapp can reach your backend."""
    url = AFFILIATE_MINIAPP_URL
    if AFFILIATE_API_BASE_URL and "api=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}api={quote(AFFILIATE_API_BASE_URL, safe=':/?&=')}"
    return url


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Affiliate Bot - Referral Program"""
    user = update.effective_user
    chat = update.effective_chat
    logger.info(f"/start from user={user.id} chat_type={chat.type} args={context.args}")

    # Ensure the starter has a commission row (so stats/link always work)
    ensure_commission_row(user.id)

    # Check for referral parameter
    if context.args and context.args[0].startswith("aff_"):
        try:
            referrer_id = int(context.args[0].replace("aff_", ""))
        except Exception:
            referrer_id = None

        if referrer_id and referrer_id != user.id:
            ensure_commission_row(referrer_id)
            # store the referrer's link for traceability (optional, but useful)
            create_referral(referrer_id, user.id, referral_link=build_referral_link(referrer_id))
            logger.info(f"New referral: {user.id} referred by {referrer_id}")

    if chat.type == "private":
        welcome_text = (
            "💚 **Emerald Affiliate Program**\n\n"
            "Verdiene EMRD mit Referrals!\n\n"
            "✨ **Features:**\n"
            "• 🔗 Einzigartige Links\n"
            "• 📊 Echtzeit Tracking\n"
            "• 💰 Provisionen (5-20%)\n"
            "• 🏆 Tier-System\n"
            "• 🪙 TON Connect & Claiming\n\n"
            "🎯 **Verdienmodell:**\n"
            "🥉 Bronze: 5% (0-999 EMRD)\n"
            "🥈 Silver: 10% (1000-4999 EMRD)\n"
            "🥇 Gold: 15% (5000-9999 EMRD)\n"
            "🔶 Platinum: 20% (10000+ EMRD)"
        )
        await update.message.reply_text(welcome_text, parse_mode="Markdown")
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "💰 Dashboard",
                web_app=WebAppInfo(url=build_miniapp_url())
            )],
            [
                InlineKeyboardButton("📊 Stats", callback_data="aff_stats"),
                InlineKeyboardButton("🪙 Wallet", callback_data="aff_wallet")
            ],
            [
                InlineKeyboardButton("💬 Support", callback_data="aff_help"),
                InlineKeyboardButton("🔗 Link", callback_data="aff_link")
            ]
        ])
        await update.message.reply_text("Wähle eine Option:", reply_markup=keyboard)
        
        # Show the user's reflink immediately (owner-friendly + zero confusion)
        my_link = build_referral_link(user.id)
        await update.message.reply_text(
            f"🔗 **Dein Reflink**\n\n`{my_link}`\n\n📌 Tipp: Teile genau diesen Link, damit Story-Sharing & Rewards korrekt zugeordnet werden.",
            parse_mode="Markdown"
        )
        
    else:
        await update.message.reply_text("💚 Affiliate Program aktiv!")


async def cmd_my_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get affiliate link"""
    user = update.effective_user
    
    link = build_referral_link(user.id)
    
    link_text = (
        f"🔗 **Dein Affiliate Link**\n\n"
        f"`{link}`\n\n"
        f"📋 Klick zum Kopieren"
    )
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Kopieren", callback_data="aff_copy_link")],
        [InlineKeyboardButton("📤 Teilen", url=f"https://t.me/share/url?url={quote(link, safe='')}")],
    ])
    
    await update.message.reply_text(link_text, parse_mode="Markdown", reply_markup=keyboard)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get referral stats"""
    user = update.effective_user
    
    stats = get_referral_stats(user.id)
    tier_info = get_tier_info(user.id)
    
    stats_text = (
        f"📊 **Deine Affiliate Stats**\n\n"
        f"👥 **Referrals:** {stats.get('total_referrals', 0)}\n"
        f"✅ **Aktive:** {stats.get('active_referrals', 0)}\n\n"
        f"💰 **Verdienst:**\n"
        f"Gesamt: {stats.get('total_earned', 0):.2f} EMRD\n"
        f"Verfügbar: {stats.get('pending', 0):.2f} EMRD\n"
        f"Ausgezahlt: {stats.get('total_earned', 0) - stats.get('pending', 0):.2f} EMRD\n\n"
    )
    
    if tier_info:
        stats_text += (
            f"🏆 **Tier:** {tier_info['tier_emoji']} {tier_info['tier'].upper()}\n"
            f"📈 **Provision:** {tier_info['commission_rate']*100:.0f}%"
        )
    
    await update.message.reply_text(stats_text, parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help"""
    help_text = """
💚 **Affiliate Bot - Hilfe**

**Befehle:**
/start - Willkommen
/link - Affiliate Link
/stats - Statistiken
/help - Hilfe

**Provisionen:**
🥉 Bronze (0-999 EMRD): 5% pro Referral
🥈 Silver (1-4999 EMRD): 10% pro Referral
🥇 Gold (5-9999 EMRD): 15% pro Referral
🔶 Platinum (10000+ EMRD): 20% pro Referral

**Minimale Auszahlung:** 1.000 EMRD

**Features:**
✅ Echtzeit Tracking
✅ TON Connect Wallet
✅ EMRD Token Claiming
✅ Instant Payout
✅ Tier Bonuses

Fragen? Wende dich an @emeraldecosystem
"""
    await update.message.reply_text(help_text, parse_mode="Markdown")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Button callbacks"""
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    
    if query.data == "aff_help":
        await cmd_help(update, context)
    elif query.data == "aff_stats":
        await cmd_stats(update, context)
    elif query.data == "aff_link":
        await cmd_my_link(update, context)
    elif query.data == "aff_wallet":
        wallet_text = (
            "🪙 **TON Connect Wallet**\n\n"
            "Öffne das Dashboard um dein TON Wallet zu verbinden.\n\n"
            "✅ Wallet Connected\n"
            "✅ EMRD Claiming enabled\n"
            "✅ Instant Payout aktiv"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "💰 Dashboard",
                web_app=WebAppInfo(url=build_miniapp_url())
            )],
        ])
        await query.edit_message_text(wallet_text, parse_mode="Markdown", reply_markup=keyboard)


def register_handlers(app):
    """Register handlers"""
    app.add_handler(CommandHandler("start", cmd_start), group=0)
    app.add_handler(CommandHandler("affiliate", cmd_start), group=0)
    app.add_handler(CommandHandler("link", cmd_my_link), group=0)
    app.add_handler(CommandHandler("stats", cmd_stats), group=0)
    app.add_handler(CommandHandler("help", cmd_help), group=0)
    
    app.add_handler(CallbackQueryHandler(button_callback), group=1)
