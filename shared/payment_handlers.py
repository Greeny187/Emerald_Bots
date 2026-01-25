import logging
import asyncio
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiohttp import web
import httpx

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CallbackQueryHandler, Application

logger = logging.getLogger(__name__)

# Imports from shared
try:
    from shared.payments import (
        create_checkout, handle_webhook, PROVIDERS, PLANS
    )
except ImportError:
    logger.warning("[payment_handlers] Could not import shared.payments")
    def create_checkout(*args, **kwargs): return {}
    def handle_webhook(*args, **kwargs): return False

async def handle_pro_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback für PRO Payment Buttons aus Miniapp.
    Pattern: pay:<provider>:<plan_key>
    """
    query = update.callback_query
    await query.answer()
    
    try:
        parts = query.data.split(":")
        if len(parts) < 3:
            await query.answer("❌ Ungültige Anfrage", show_alert=True)
            return
        
        provider = parts[1]  # walletconnect_ton oder paypal
        plan_key = parts[2]  # pro_monthly, pro_yearly
        
        user_id = query.from_user.id
        chat_id = query.message.chat_id if query.message else None
        
        logger.info(f"[payment] User {user_id} requesting {provider} payment for {plan_key}")
        
        # Webhook URL ist nur noch für spätere Provider relevant (aktuell nicht genutzt)
        webhook_url = context.bot_data.get("webhook_base_url", "https://emeraldcontent.com/webhook")
        
        # Erstelle Checkout (TonConnect / PayPal)
        result = create_checkout(chat_id or user_id, provider, plan_key, user_id, webhook_url)
        
        if "error" in result:
            logger.warning(f"[payment] Checkout failed: {result['error']}")
            await query.answer(f"❌ {result['error']}", show_alert=True)
            return
        
          # Provider-spezifische Behandlung
        if provider == "walletconnect_ton":
            # Deep Link zu Wallet
            uri = result.get("uri", "")
            if uri:
                await query.edit_message_text(
                    text=f"🔗 **Zahlung mit {PROVIDERS[provider]['label']}**\n\n"
                         f"Plan: {PLANS.get(plan_key, {}).get('label', plan_key)}\n"
                         f"Betrag: €{result.get('price', 'N/A')}\n\n"
                         f"👆 Tippe auf den Link oben, um Zahlung zu initiieren:\n"
                         f"`{uri}`\n\n"
                         f"Order ID: `{result.get('order_id')}`",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Aktualisieren", callback_data=f"pay:status")],
                    ]),
                    parse_mode="Markdown"
                )
            else:
                await query.answer("❌ Wallet-Link konnte nicht erstellt werden", show_alert=True)
        
        elif provider == "paypal":
            # Externer Payment-Link (PayPal)
            url = result.get("url", "")
            if url:
                await query.edit_message_text(
                    text=f"🔗 **Zahlung via {PROVIDERS.get(provider, {}).get('label', provider)}**\n\n"
                         f"Plan: {PLANS.get(plan_key, {}).get('label', plan_key)}\n"
                         f"Betrag: €{result.get('price', 'N/A')}\n\n"
                         f"👉 [Zur Zahlungsseite]({url})\n\n"
                         f"Order ID: `{result.get('order_id')}`",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔗 Zahlung starten", url=url)],
                        [InlineKeyboardButton("🔄 Status prüfen", callback_data="pay:status")],
                    ]),
                    parse_mode="Markdown"
                )
            else:
                await query.answer("❌ PayPal-Link nicht verfügbar", show_alert=True)
        
        else:
            await query.answer(f"❌ Provider {provider} wird nicht unterstützt", show_alert=True)
    
    except Exception as e:
        logger.exception(f"[payment] Callback error: {e}")
        await query.answer(f"❌ Fehler: {str(e)[:50]}", show_alert=True)

async def handle_pro_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Zeige PRO Payment Status des Users.
    """
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    chat_id = query.message.chat_id if query.message else None
    
    try:
        # TODO: Hole Abo-Status aus DB
        await query.edit_message_text(
            text=f"📋 **Dein PRO Status**\n\n"
                 f"User ID: {user_id}\n"
                 f"Chat ID: {chat_id}\n\n"
                 f"Status: ℹ️ Keine Subscription aktiv\n\n"
                 f"Wähle einen Plan um zu upgraden.",
            reply_markup=None
        )
    except Exception as e:
        logger.warning(f"[payment] Status check error: {e}")
        await query.answer("❌ Fehler beim Status abrufen", show_alert=True)

# === Webhook Handler für externe Payment-Provider ===

async def route_webhook_generic(request: web.Request) -> web.Response:
    """
    Generic webhook handler für andere Payment-Provider.
    """
    try:
        provider = request.match_info.get("provider", "unknown")
        data = await request.json() if request.content_type == "application/json" else {}
        
        logger.info(f"[webhook] {provider}: {list(data.keys())}")
        
        ok = handle_webhook(provider, data)
        return web.Response(status=200 if ok else 400, text="OK" if ok else "Failed")
    
    except Exception as e:
        logger.exception(f"[webhook] Error: {e}")
        return web.Response(status=500, text="Error")

# === Registrierung ===

def register_payment_handlers(app: Application):
    """
    Registriere Payment Callbacks in der Telegram App.
    """
    app.add_handler(CallbackQueryHandler(handle_pro_payment_callback, pattern=r"^pay:"), group=-2)
    app.add_handler(CallbackQueryHandler(handle_pro_status_callback, pattern=r"^pay:status$"), group=-2)

def register_payment_routes(webapp: web.Application):
    """
    Registriere Webhook Routes in der aiohttp Web App.
    """
    webapp.router.add_post("/webhooks/{provider}", route_webhook_generic)
    
    logger.info("[payment_handlers] Payment routes registered")
