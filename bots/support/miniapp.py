# miniapp.py — Emerald Support Bot MiniApp Hub (v1.1)
"""
ZIEL:
✅ Alles läuft über die MiniApp (kein Ticket-Flow im Chat)
✅ Keine Group-Settings / Stats / Admin-Setup
✅ Nur 1 DB-Datei: database.py (optional tenant ensure)
✅ Logging an wichtigen Stellen

MiniApp nutzt i.d.R. HTTP API (support_api.py) via fetch().
WEB_APP_DATA (Telegram.WebApp.sendData) wird nur noch geloggt + freundlich beantwortet.
"""

import os
import json
import logging
import asyncio
from urllib.parse import urlencode

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import (
    Application,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
)

log = logging.getLogger("bot.support.miniapp")

# WebApp URL (MiniApp)
WEBAPP_URL = os.getenv(
    "SUPPORT_WEBAPP_URL",
    "https://greeny187.github.io/EmeraldContentBots/miniapp/appsupport.html"
)

# Optional: API base to pass to MiniApp as ?api=...
# (Hilft gegen "failed to fetch", wenn du mehrere Umgebungen hast)
SUPPORT_API_BASE = os.getenv("SUPPORT_API_BASE") or os.getenv("WEBHOOK_URL") or ""


def _build_webapp_url(chat_id: int | None, title: str | None, user_id: int | None, tab: str | None = None) -> str:
    """Build MiniApp url with safe query params: cid/title/uid/tab/api"""
    log.debug(f"[URL_BUILD] Building WebApp URL - chat_id={chat_id} user_id={user_id} tab={tab}")
    params = {}

    if chat_id is not None:
        params["cid"] = str(chat_id)

    if title:
        # Telegram liefert title bereits als string; urlencode übernimmt safe encoding
        params["title"] = title

    if user_id is not None:
        params["uid"] = str(user_id)

    if tab:
        params["tab"] = tab

    if SUPPORT_API_BASE:
        params["api"] = SUPPORT_API_BASE.rstrip("/")

    if not params:
        log.debug(f"[URL_BUILD] No params, returning base URL")
        return WEBAPP_URL

    url = WEBAPP_URL + ("&" if "?" in WEBAPP_URL else "?") + urlencode(params)
    log.debug(f"[URL_BUILD] Generated URL with {len(params)} params")
    return url


async def _ensure_tenant_best_effort(chat_id: int, title: str | None) -> None:
    """
    Optional: Tenant/Chat-Mapping beim Öffnen der MiniApp sicherstellen.
    Nutzt database.py (sync) via asyncio.to_thread.
    """
    log.debug(f"[TENANT_ENSURE] Starting tenant ensure for chat_id={chat_id} title={title}")
    try:
        from . import database as db  # single DB file
    except Exception as e:
        log.debug(f"[TENANT_ENSURE] Failed to import db from package: {e}")
        try:
            import database as db  # type: ignore
        except Exception as e2:
            log.debug(f"[TENANT_ENSURE] Failed to import db standalone: {e2}")
            db = None  # type: ignore

    if not db:
        log.warning("[TENANT_ENSURE] Database module not available, skipping tenant ensure")
        return

    try:
        # schema/init optional – wenn du das nicht willst, Zeile entfernen
        log.debug("[TENANT_ENSURE] Calling db.init_all_schemas()")
        await asyncio.to_thread(db.init_all_schemas)
        log.debug("[TENANT_ENSURE] Calling db.ensure_tenant_for_chat()")
        tid = await asyncio.to_thread(db.ensure_tenant_for_chat, chat_id, title, None)
        log.info(f"✅ [TENANT_ENSURE] Tenant ensured: tenant_id={tid} chat_id={chat_id}")
    except Exception as e:
        # Best effort: MiniApp darf trotzdem öffnen
        log.exception(f"⚠️ [TENANT_ENSURE] Tenant ensure failed (best effort): chat_id={chat_id}: {e}")


async def _send_open_miniapp(update: Update, tab: str | None = None) -> None:
    """Send a message + button opening the MiniApp."""
    chat = update.effective_chat
    user = update.effective_user

    chat_id = getattr(chat, "id", None)
    title = getattr(chat, "title", None)
    user_id = getattr(user, "id", None)
    
    log.debug(f"[MINIAPP_SEND] Opening MiniApp - user={user_id} chat={chat_id} tab={tab}")

    url = _build_webapp_url(chat_id, title, user_id, tab=tab)

    # tenant ensure (optional)
    if chat_id is not None:
        log.debug(f"[MINIAPP_SEND] Ensuring tenant for chat_id={chat_id}")
        await _ensure_tenant_best_effort(chat_id, title)

    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔧 Support MiniApp öffnen", web_app=WebAppInfo(url=url))]]
    )

    text = (
        "🎫 **Emerald Support**\n\n"
        "Support läuft komplett über die **MiniApp**.\n"
        "Dort kannst du:\n"
        "• Tickets erstellen\n"
        "• Tickets ansehen\n"
        "• KB durchsuchen\n"
    )

    log.debug(f"[MINIAPP_SEND] Sending MiniApp button to user={user_id}")
    # update.message existiert bei CommandHandler + normalen Texten
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)
        log.debug(f"[MINIAPP_SEND] Sent via message reply")
    # fallback (falls mal Callback etc. genutzt wird)
    elif update.effective_chat:
        await update.effective_chat.send_message(text, parse_mode="Markdown", reply_markup=kb)
        log.debug(f"[MINIAPP_SEND] Sent via chat.send_message()")


# ---------- Commands (alles leitet zur MiniApp) ----------

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    log.info(f"[CMD] /start invoked - user={getattr(update.effective_user, 'id', None)} chat={getattr(update.effective_chat, 'id', None)} type={getattr(update.effective_chat, 'type', None)}")
    await _send_open_miniapp(update, tab=None)

async def cmd_support(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    log.info(f"[CMD] /support invoked - user={getattr(update.effective_user, 'id', None)} chat={getattr(update.effective_chat, 'id', None)}")
    await _send_open_miniapp(update, tab=None)

async def cmd_ticket(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    log.info(f"[CMD] /ticket invoked - user={getattr(update.effective_user, 'id', None)} chat={getattr(update.effective_chat, 'id', None)}")
    await _send_open_miniapp(update, tab="create")

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    log.info(f"[CMD] /status invoked - user={getattr(update.effective_user, 'id', None)} chat={getattr(update.effective_chat, 'id', None)}")
    await _send_open_miniapp(update, tab="mine")

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    log.info(f"[CMD] /help invoked - user={getattr(update.effective_user, 'id', None)} chat={getattr(update.effective_chat, 'id', None)}")
    await _send_open_miniapp(update, tab=None)


# ---------- WEB_APP_DATA (optional, legacy) ----------

async def on_web_app_data(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Falls deine MiniApp Telegram.WebApp.sendData() nutzt:
    - Wir loggen es
    - und schicken den Nutzer zurück in die MiniApp
    """
    msg = update.effective_message
    user = update.effective_user
    chat = update.effective_chat

    raw = None
    try:
        raw = msg.web_app_data.data if msg and msg.web_app_data else ""
        payload = json.loads(raw) if raw else {}
        log.info(
            f"[DATA] WEB_APP_DATA received - user={getattr(user, 'id', None)} chat={getattr(chat, 'id', None)} keys={list(payload.keys())[:25]}"
        )
        log.debug(f"[DATA] Payload preview: {str(raw)[:200]}")
    except Exception as e:
        log.exception(f"[DATA] WEB_APP_DATA parse failed - user={getattr(user, 'id', None)} chat={getattr(chat, 'id', None)}: {e}")

    # MiniApp ist der Hauptweg – einfach öffnen lassen
    log.debug(f"[DATA] Redirecting to MiniApp")
    await _send_open_miniapp(update, tab=None)


# ---------- Text Fallback (alles über MiniApp) ----------

async def text_fallback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Egal was geschrieben wird → MiniApp Button.
    (Damit wirklich alles über die MiniApp läuft)
    """
    txt = update.message.text if update.message else ""
    log.info(
        f"[TEXT] Fallback handler invoked - user={getattr(update.effective_user, 'id', None)} chat={getattr(update.effective_chat, 'id', None)} text_len={len(txt) if txt else 0}"
    )
    log.debug(f"[TEXT] Text content (first 100 chars): {(txt or '')[:100]}")
    await _send_open_miniapp(update, tab=None)


def register(app: Application):
    """Register MiniApp hub handlers."""
    log.info("[REGISTER] Registering MiniApp hub handlers (support-only, miniapp-first)...")

    # Commands → always open MiniApp
    log.debug("[REGISTER] Adding command handlers: /start, /support, /miniapp")
    app.add_handler(CommandHandler(["start", "support", "miniapp"], cmd_start), group=0)
    log.debug("[REGISTER] Adding command handler: /ticket")
    app.add_handler(CommandHandler("ticket", cmd_ticket), group=0)
    log.debug("[REGISTER] Adding command handler: /status")
    app.add_handler(CommandHandler("status", cmd_status), group=0)
    log.debug("[REGISTER] Adding command handler: /help")
    app.add_handler(CommandHandler("help", cmd_help), group=0)

    # Optional legacy web_app_data
    log.debug("[REGISTER] Adding WEB_APP_DATA handler")
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, on_web_app_data), group=1)

    # Any other text → MiniApp
    log.debug("[REGISTER] Adding text fallback handler")
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_fallback), group=2)

    log.info("✅ [REGISTER] MiniApp hub registered successfully")
