import os, sys
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # .../app
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import hashlib
import asyncio
import logging
from typing import Dict, Any, Optional
from telegram import Update, File
from telegram.ext import ContextTypes
from bots.crossposter.database import get_pool
from bots.crossposter.models import log_event
from bots.crossposter.x_client import post_text as x_post_text, post_with_media as x_post_with_media
import httpx

logger = logging.getLogger(__name__)

async def _hash_message(update: Update) -> str:
    """Create a unique hash for deduplication."""
    text = update.effective_message.text or update.effective_message.caption or ""
    media_id = None
    if update.effective_message.photo:
        media_id = update.effective_message.photo[-1].file_unique_id
    elif update.effective_message.document:
        media_id = update.effective_message.document.file_unique_id
    elif update.effective_message.video:
        media_id = update.effective_message.video.file_unique_id
    payload = f"{update.effective_chat.id}|{update.effective_message.message_id}|{text}|{media_id or ''}"
    return hashlib.sha256(payload.encode()).hexdigest()

async def _apply_transform(text: str, transform: Dict[str, Any]) -> str:
    """Apply text transformations (prefix, suffix, plain text)."""
    if transform.get("plain_text"):
        # Remove markdown characters
        for ch in ["*","_","`","[","]","{","}","~",">"]:
            text = text.replace(ch,"")
    prefix = transform.get('prefix','')
    suffix = transform.get('suffix','')
    return f"{prefix}{text}{suffix}"

async def discord_post(webhook_url: str, content: str, username: str = None, avatar_url: str = None):
    """Post message to Discord webhook."""
    if not webhook_url:
        raise ValueError("Discord webhook URL erforderlich")
    data = {"content": content}
    if username: data["username"] = username
    if avatar_url: data["avatar_url"] = avatar_url
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(webhook_url, json=data)
            r.raise_for_status()
            logger.info(f"Discord post erfolgreich: {webhook_url[:50]}...")
    except httpx.HTTPError as e:
        logger.exception(f"Discord post fehlgeschlagen: {e}")
        raise

async def _get_x_access_token(tenant_id: int):
    """Get X (Twitter) access token with proper validation."""
    try:
        pool = await get_pool()
        if pool:
            row = await pool.fetchrow(
                "SELECT config FROM connectors WHERE tenant_id=$1 AND type='x' AND active=TRUE ORDER BY id DESC LIMIT 1",
                tenant_id
            )
            if row and row.get("config"):
                config = row["config"]
                if isinstance(config, dict) and config.get("access_token"):
                    token = config["access_token"]
                    # Basic token validation (should be at least 10 chars)
                    if isinstance(token, str) and len(token) >= 10:
                        logger.debug(f"Using database X token for tenant {tenant_id}")
                        return token
                    else:
                        logger.warning(f"Invalid X token format for tenant {tenant_id}")
    except Exception as e:
        logger.warning(f"Error fetching X token from database: {e}")

    # Fallback to environment variable
    token = os.environ.get("X_ACCESS_TOKEN")
    if not token:
        logger.error("X_ACCESS_TOKEN environment variable not set")
        raise ValueError("X_ACCESS_TOKEN not configured")

    if len(token) < 10:
        logger.error("X_ACCESS_TOKEN format invalid")
        raise ValueError("X_ACCESS_TOKEN format invalid")

    logger.debug("Using environment X token")
    return token

async def _get_media_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[bytes]:
    """Download media from message if available."""
    try:
        if update.effective_message.photo:
            file = await context.bot.get_file(update.effective_message.photo[-1].file_id)
            return await file.download_as_bytearray()
        elif update.effective_message.document:
            file = await context.bot.get_file(update.effective_message.document.file_id)
            return await file.download_as_bytearray()
        elif update.effective_message.video:
            file = await context.bot.get_file(update.effective_message.video.file_id)
            return await file.download_as_bytearray()
    except Exception as e:
        logger.warning(f"Media download fehlgeschlagen: {e}")
    return None

async def route_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main message routing handler."""
    chat_id = update.effective_chat.id
    msg_id = getattr(update.effective_message, "message_id", None)
    has_media = bool(update.effective_message.photo or update.effective_message.document or update.effective_message.video)
    logger.debug(f"Incoming message chat_id={chat_id} msg_id={msg_id} has_media={has_media}")

    # Get database pool with error handling
    try:
        pool = await asyncio.wait_for(get_pool(), timeout=5)
        if not pool:
            logger.error("Database pool unavailable")
            return
    except asyncio.TimeoutError:
        logger.error("Database timeout getting pool")
        return
    except Exception:
        logger.exception("Failed to get database pool")
        return

    # Fetch active routes for this chat
    try:
        routes = await asyncio.wait_for(
            pool.fetch(
                "SELECT id, tenant_id, destinations, transform, filters FROM crossposter_routes WHERE source_chat_id=$1 AND active=TRUE",
                chat_id
            ),
            timeout=5
        )
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching routes for chat {chat_id}")
        return
    except Exception:
        logger.exception(f"Error fetching routes for chat {chat_id}")
        return

    if not routes:
        logger.debug(f"Keine aktiven Routen für chat_id={chat_id}")
        return

    dedup_hash = await _hash_message(update)
    text = update.effective_message.text or update.effective_message.caption or ""

    logger.info(
        f"Crosspost check: chat_id={chat_id} msg_id={msg_id} routes={len(routes)} dedup={dedup_hash[:10]}..."
    )

    # Optional: get media
    media_bytes = None
    if context.bot and (update.effective_message.photo or update.effective_message.document):
        media_bytes = await _get_media_file(update, context)

    # Process each route
    for r in routes:
        try:
            logger.debug(
                f"Route {r['id']} tenant={r['tenant_id']} destinations={len(r['destinations'] or [])}"
            )
            # Apply filters
            wl = set(r["filters"].get("hashtags_whitelist", []))
            bl = set(r["filters"].get("hashtags_blacklist", []))
            tags = {t for t in text.split() if t.startswith('#')}

            # Check whitelist
            if wl and not (tags & set(f"#{t}" for t in wl)):
                await log_event(r["tenant_id"], r["id"], chat_id, update.effective_message.message_id,
                              {"type": "filter"}, "skipped", "Whitelist nicht getroffen", dedup_hash)
                logger.debug(f"Route {r['id']} skipped (whitelist) tags={list(tags)[:10]}")
                continue

            # Check blacklist
            if bl and (tags & set(f"#{t}" for t in bl)):
                await log_event(r["tenant_id"], r["id"], chat_id, update.effective_message.message_id,
                              {"type": "filter"}, "skipped", "Blacklist getroffen", dedup_hash)
                logger.debug(f"Route {r['id']} skipped (blacklist) tags={list(tags)[:10]}")
                continue

            final_text = await _apply_transform(text, r["transform"])

            # Send to each destination
            for dest in r["destinations"]:
                try:
                    logger.debug(f"Route {r['id']} -> dest={dest.get('type')} payload_keys={list(dest.keys())}")
                    if dest.get("type") == "telegram" and dest.get("chat_id"):
                        try:
                            if update.effective_message.photo or update.effective_message.document:
                                await context.bot.copy_message(
                                    chat_id=dest["chat_id"],
                                    from_chat_id=chat_id,
                                    message_id=update.effective_message.message_id,
                                    caption=final_text or None
                                )
                            else:
                                await context.bot.send_message(dest["chat_id"], final_text or text)
                            await log_event(r["tenant_id"], r["id"], chat_id, update.effective_message.message_id,
                                          dest, "sent", None, dedup_hash)
                            logger.info(f"Telegram crosspost erfolgreich: dest_chat_id={dest['chat_id']}")
                        except Exception as te:
                            raise Exception(f"Telegram error: {str(te)}")

                    elif dest.get("type") == "x":
                        try:
                            token = await _get_x_access_token(r["tenant_id"])
                            if media_bytes and len(media_bytes) > 0:
                                await x_post_with_media(token, final_text or text, media_bytes)
                            else:
                                await x_post_text(token, final_text or text)
                            await log_event(r["tenant_id"], r["id"], chat_id, update.effective_message.message_id,
                                          dest, "sent", None, dedup_hash)
                            logger.info("X post erfolgreich")
                        except Exception as xe:
                            raise Exception(f"X error: {str(xe)}")

                    elif dest.get("type") in ("discord", "discord_webhook"):
                        try:
                            url = dest.get("webhook_url")
                            if not url:
                                raise ValueError("Discord webhook URL fehlt")
                            await discord_post(url, final_text or text, dest.get("username"), dest.get("avatar_url"))
                            await log_event(r["tenant_id"], r["id"], chat_id, update.effective_message.message_id,
                                          dest, "sent", None, dedup_hash)
                            logger.info("Discord post erfolgreich")
                        except Exception as de:
                            raise Exception(f"Discord error: {str(de)}")

                except Exception as dest_error:
                    logger.exception(f"Destination error: {dest_error}")
                    await log_event(r["tenant_id"], r["id"], chat_id, update.effective_message.message_id,
                                  dest, "error", str(dest_error), dedup_hash)

        except Exception as route_error:
            logger.exception(f"Route processing error for route {r['id']}: {route_error}")
            await log_event(r["tenant_id"], r["id"], chat_id, update.effective_message.message_id,
                          {"type": "route"}, "error", str(route_error), dedup_hash)
