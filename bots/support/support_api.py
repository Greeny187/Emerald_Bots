import os, hmac, hashlib, json, logging
from urllib.parse import unquote
from typing import Dict, Any, Optional

import anyio
from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, validator

from . import database as db  # ✅ single DB file

logger = logging.getLogger("bot.support.api")
logger.info("[API_INIT] Support API module initialized")

router = APIRouter(prefix="/api/support", tags=["support"])
BOT_TOKEN = os.getenv("BOT6_TOKEN") or os.getenv("BOT_TOKEN")
logger.debug(f"[API_INIT] BOT_TOKEN configured: {bool(BOT_TOKEN)}")


def _verify_init_data(init_data: str) -> Dict[str, Any]:
    logger.debug("[VERIFY] Starting init data verification")
    
    if not init_data:
        logger.warning("[VERIFY] Missing X-Telegram-Init-Data header")
        raise HTTPException(status_code=401, detail="Missing X-Telegram-Init-Data")

    pairs = [p for p in init_data.split("&") if "=" in p]
    logger.debug(f"[VERIFY] Parsed {len(pairs)} key-value pairs")
    
    data = {}
    for p in pairs:
        k, v = p.split("=", 1)
        data[k] = unquote(v)

    hash_recv = data.pop("hash", None)
    if not hash_recv:
        logger.warning("[VERIFY] Missing hash in init data")
        raise HTTPException(status_code=401, detail="Missing hash in init data")

    check_string = "\n".join(f"{k}={data[k]}" for k in sorted(data.keys()))

    if not BOT_TOKEN:
        logger.error("[VERIFY] BOT_TOKEN not configured")
        raise HTTPException(status_code=500, detail="Server configuration error")

    secret_key = hashlib.sha256(b"WebAppData" + BOT_TOKEN.encode()).digest()
    calc_hash = hmac.new(secret_key, check_string.encode(), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(calc_hash, hash_recv):
        logger.warning(f"[VERIFY] Bad signature attempt - calc_hash mismatch")
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        user_obj = json.loads(data.get("user", "{}"))
        if not user_obj or "id" not in user_obj:
            logger.warning("[VERIFY] Missing or invalid user in init data")
            raise HTTPException(status_code=401, detail="Missing or invalid user in init data")

        result = {
            "user_id": int(user_obj["id"]),
            "username": user_obj.get("username"),
            "first_name": user_obj.get("first_name"),
            "last_name": user_obj.get("last_name"),
            "language_code": user_obj.get("language_code", "de"),
        }
        logger.debug(f"[VERIFY] ✅ Verification successful - user_id={result['user_id']}")
        return result
    except Exception as e:
        logger.exception(f"[VERIFY] Error parsing user from init data: {e}")
        raise HTTPException(status_code=401, detail="Invalid init data format")


class TicketCreate(BaseModel):
    category: str = Field(default="allgemein", max_length=48)
    subject: str = Field(..., min_length=4, max_length=140)
    body: str = Field(..., min_length=10, max_length=4000)

    @validator("category")
    def validate_category(cls, v):
        allowed = ["allgemein", "technik", "zahlungen", "konto", "feedback"]
        if v not in allowed:
            raise ValueError(f"Category must be one of {allowed}")
        return v


class TicketReply(BaseModel):
    text: str = Field(..., min_length=1, max_length=4000)


@router.get("/health")
async def health():
    logger.debug("[HEALTH] Health check requested")
    return {"ok": True, "service": "emerald-support"}


@router.post("/tickets")
async def create_ticket(
    request: Request,
    payload: TicketCreate,
    x_telegram_init_data: str = Header(None),
    cid: Optional[int] = Query(None, description="Chat/Group ID (optional)"),
    title: Optional[str] = Query(None, description="Chat title (optional)"),
):
    logger.info("[TICKET_CREATE] POST /tickets - starting request processing")
    try:
        user = _verify_init_data(x_telegram_init_data)

        logger.info(
            f"[TICKET_CREATE] user_id={user['user_id']} origin={request.headers.get('origin')} cid={cid} category={payload.category}"
        )
        logger.debug(f"[TICKET_CREATE] Subject: {payload.subject[:50]}...")

        # init schema best effort (optional – kannst du später rausnehmen)
        logger.debug("[TICKET_CREATE] Initializing schema")
        await anyio.to_thread.run_sync(db.init_all_schemas)

        logger.debug(f"[TICKET_CREATE] Upserting user {user['user_id']}")
        ok = await anyio.to_thread.run_sync(db.upsert_user, user)
        if not ok:
            logger.error(f"[TICKET_CREATE] Failed to upsert user {user['user_id']}")
            raise HTTPException(status_code=500, detail="Failed to upsert user")

        tenant_id = None
        if cid is not None:
            try:
                logger.debug(f"[TICKET_CREATE] Ensuring tenant for chat {cid}")
                tenant_id = await anyio.to_thread.run_sync(db.ensure_tenant_for_chat, cid, title, None)
                logger.debug(f"[TICKET_CREATE] Tenant ID: {tenant_id}")
            except Exception as e:
                logger.exception(f"[TICKET_CREATE] ensure_tenant_for_chat failed (continuing unscoped): cid={cid}: {e}")
                tenant_id = None

        logger.debug(f"[TICKET_CREATE] Creating ticket for user {user['user_id']} with category {payload.category}")
        tid = await anyio.to_thread.run_sync(
            db.create_ticket,
            user["user_id"],
            payload.category,
            payload.subject,
            payload.body,
            tenant_id,
        )

        if not tid:
            logger.error(f"[TICKET_CREATE] Failed to create ticket for user {user['user_id']}")
            raise HTTPException(status_code=500, detail="Failed to create ticket")

        logger.info(f"✅ [TICKET_CREATE] Ticket created successfully: id={tid} user={user['user_id']}")
        return {"ok": True, "ticket_id": tid}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [TICKET_CREATE] Error creating ticket: {e}")
        raise HTTPException(status_code=500, detail="Failed to create ticket")


@router.get("/tickets")
async def list_my_tickets(
    request: Request,
    x_telegram_init_data: str = Header(None),
    cid: Optional[int] = Query(None),
    limit: int = Query(30, ge=1, le=100),
):
    logger.info(f"[TICKET_LIST] GET /tickets - starting request")
    try:
        user = _verify_init_data(x_telegram_init_data)
        logger.debug(
            f"[TICKET_LIST] user_id={user['user_id']} origin={request.headers.get('origin')} cid={cid} limit={limit}"
        )

        tenant_id = None
        if cid is not None:
            logger.debug(f"[TICKET_LIST] Resolving tenant for chat {cid}")
            tenant_id = await anyio.to_thread.run_sync(db.resolve_tenant_id_by_chat, cid)
            logger.debug(f"[TICKET_LIST] Tenant resolved: {tenant_id}")

        logger.debug(f"[TICKET_LIST] Fetching tickets for user {user['user_id']}")
        tickets = await anyio.to_thread.run_sync(db.get_my_tickets, user["user_id"], limit, tenant_id)
        logger.info(f"✅ [TICKET_LIST] Retrieved {len(tickets or [])} tickets for user {user['user_id']}")
        return {"ok": True, "tickets": tickets or []}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [TICKET_LIST] Error listing tickets: {e}")
        raise HTTPException(status_code=500, detail="Failed to list tickets")


@router.get("/tickets/{ticket_id}")
async def get_ticket(
    request: Request,
    ticket_id: int,
    x_telegram_init_data: str = Header(None),
):
    logger.info(f"[TICKET_GET] GET /tickets/{ticket_id} - starting request")
    try:
        user = _verify_init_data(x_telegram_init_data)
        logger.debug(
            f"[TICKET_GET] user_id={user['user_id']} ticket_id={ticket_id} origin={request.headers.get('origin')}"
        )

        logger.debug(f"[TICKET_GET] Fetching ticket {ticket_id}")
        ticket = await anyio.to_thread.run_sync(db.get_ticket, user["user_id"], ticket_id)
        if not ticket:
            logger.warning(f"[TICKET_GET] Ticket {ticket_id} not found or access denied for user {user['user_id']}")
            raise HTTPException(status_code=404, detail="Ticket not found or access denied")
        
        logger.info(f"✅ [TICKET_GET] Retrieved ticket {ticket_id} for user {user['user_id']}")
        return {"ok": True, "ticket": ticket}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [TICKET_GET] Error fetching ticket: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch ticket")


@router.post("/tickets/{ticket_id}/messages")
async def add_message(
    request: Request,
    ticket_id: int,
    payload: TicketReply,
    x_telegram_init_data: str = Header(None),
):
    logger.info(f"[MESSAGE_ADD] POST /tickets/{ticket_id}/messages - starting request")
    try:
        user = _verify_init_data(x_telegram_init_data)
        logger.debug(
            f"[MESSAGE_ADD] user_id={user['user_id']} ticket_id={ticket_id} text_len={len(payload.text)} origin={request.headers.get('origin')}"
        )

        logger.debug(f"[MESSAGE_ADD] Adding message to ticket {ticket_id}")
        ok = await anyio.to_thread.run_sync(db.add_public_message, user["user_id"], ticket_id, payload.text)
        if not ok:
            logger.warning(f"[MESSAGE_ADD] Not allowed to add message to ticket {ticket_id} for user {user['user_id']}")
            raise HTTPException(status_code=403, detail="Not allowed to add message to this ticket")
        
        logger.info(f"✅ [MESSAGE_ADD] Message added to ticket {ticket_id} by user {user['user_id']}")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [MESSAGE_ADD] Error adding message: {e}")
        raise HTTPException(status_code=500, detail="Failed to add message")


@router.get("/kb/search")
async def kb_search(
    request: Request,
    q: str = Query(..., min_length=2),
    limit: int = Query(8, ge=1, le=20),
    x_telegram_init_data: str = Header(None),
):
    logger.info(f"[KB_SEARCH] GET /kb/search - starting request")
    try:
        user = _verify_init_data(x_telegram_init_data)
        logger.debug(
            f"[KB_SEARCH] user_id={user['user_id']} query_len={len(q)} limit={limit} origin={request.headers.get('origin')}"
        )
        logger.debug(f"[KB_SEARCH] Query: {q[:100]}")

        logger.debug(f"[KB_SEARCH] Searching KB for user {user['user_id']}")
        results = await anyio.to_thread.run_sync(db.kb_search, q, limit)
        logger.info(f"✅ [KB_SEARCH] Found {len(results or [])} results for user {user['user_id']}")
        return {"ok": True, "results": results or []}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [KB_SEARCH] KB search failed: {e}")
        raise HTTPException(status_code=500, detail="KB search failed")
