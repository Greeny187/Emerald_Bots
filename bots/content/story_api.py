"""
Story Sharing API Endpoints (Emerald)
- Settings pro Gruppe (database.get_story_settings)
- Rate-Limit pro User/Tag (pro Gruppe)
- Admin-Check für Share-Erstellung (Miniapp)
"""

import logging
import os
import json
import urllib.parse
from aiohttp import web
from typing import Optional, List
import hmac
import hashlib
from .story_sharing import (
    create_story_share,
    track_story_click,
    record_story_conversion,
    get_share_stats,
    get_user_shares,
    get_top_shares,
    init_story_sharing_schema,
    STORY_TEMPLATES,
    get_share_by_id,
    count_shares_today,
    set_click_rewarded_steps,
)
from .story_card_generator import generate_share_card
from .database import get_story_settings, get_group_title
try:
    from shared.emrd_rewards import get_pending_rewards, create_reward_claim
except Exception:
    get_pending_rewards = None
    create_reward_claim = None

logger = logging.getLogger(__name__)

try:
    from shared.emrd_rewards_integration import award_points  # type: ignore
except Exception:  # pragma: no cover
    award_points = None  # type: ignore

def _token_secrets_from_request(request: web.Request) -> List[bytes]:
    """Sammelt mögliche BOT-Token-Secrets (sha256(token)) aus Env + PTB-App."""
    secrets: List[bytes] = []

    # 1) Env Tokens (Multi-Bot)
    for k in ("BOT1_TOKEN","BOT2_TOKEN","BOT3_TOKEN","BOT4_TOKEN","BOT5_TOKEN","BOT6_TOKEN","BOT_TOKEN"):
        t = os.getenv(k)
        if t:
            try:
                secrets.append(hashlib.sha256(t.encode()).digest())
            except Exception:
                pass

    # 2) PTB-App Token (falls vorhanden)
    try:
        ptb_app = request.app.get("ptb_app")
        tok = getattr(getattr(ptb_app, "bot", None), "token", None)
        if tok:
            secrets.append(hashlib.sha256(str(tok).encode()).digest())
    except Exception:
        pass

    # de-dupe
    uniq = []
    seen = set()
    for s in secrets:
        if s not in seen:
            uniq.append(s)
            seen.add(s)
    return uniq


def _verify_init_data(init_data: str, secret: bytes) -> int:
    """Telegram WebApp initData Signatur-Prüfung (HMAC-SHA256)."""
    parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
    recv_hash = parsed.get("hash")
    if not recv_hash:
        return 0
    items = [(k, v) for k, v in parsed.items() if k != "hash"]
    check_str = "\n".join(f"{k}={v}" for k, v in sorted(items))
    calc = hmac.new(secret, msg=check_str.encode(), digestmod=hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, recv_hash):
        return 0
    try:
        user = json.loads(parsed.get("user") or "{}")
        return int(user.get("id") or 0)
    except Exception:
        return 0

def parse_webapp_user_id(request: web.Request, init_data: Optional[str]) -> int:
    """Extrahiert user.id aus Telegram WebApp initData – NUR wenn Signatur gültig."""
    if not init_data:
        return 0
    for secret in _token_secrets_from_request(request):
        uid = _verify_init_data(init_data, secret)
        if uid:
            return uid
    return 0


def _resolve_uid(request: web.Request) -> int:
    init_str = (
        request.headers.get("X-Telegram-Init-Data")
        or request.headers.get("x-telegram-web-app-data")
        or request.query.get("init_data")
    )
    uid = parse_webapp_user_id(request, init_str)
    if uid > 0:
        return uid

    # Fallback: uid in der Query (für Browser-Tests)
    q_uid = request.query.get("uid")
    if os.getenv("ALLOW_BROWSER_DEV", "0") == "1" and q_uid and str(q_uid).lstrip("-").isdigit():
        return int(q_uid)

    # Optionaler Dev-Bypass
    if (
        os.getenv("ALLOW_BROWSER_DEV") == "1"
        and request.headers.get("X-Dev-Token") == os.getenv("DEV_TOKEN", "")
    ):
        return int(request.headers.get("X-Dev-User-Id", "0") or 0)

    return 0


def _cors_headers(request: web.Request) -> dict:
    origin = request.headers.get("Origin", "")
    allowed = request.app.get("allowed_origin") or "*"
    if allowed == "*" or (origin and origin.startswith(allowed)):
        return {
            "Access-Control-Allow-Origin": origin if origin else allowed,
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
            "Access-Control-Allow-Credentials": "true",
        }
    return {}

async def _cors_ok(request: web.Request) -> web.Response:
    return web.Response(status=200, headers=_cors_headers(request))

def _json(request: web.Request, payload: dict, status: int = 200) -> web.Response:
    return web.json_response(payload, status=status, headers=_cors_headers(request))


async def _is_admin(app, chat_id: int, user_id: int) -> bool:
    try:
        ptb_app = app.get("ptb_app")
        if not ptb_app:
            return False
        member = await ptb_app.bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


def register_story_api(webapp):
    # Schema init
    init_story_sharing_schema()

    # Routes
    webapp.router.add_get("/api/stories/templates", get_templates)
    webapp.router.add_post("/api/stories/create", create_share)
    webapp.router.add_post("/api/stories/click", click_share)
    webapp.router.add_post("/api/stories/convert", convert_share)

    # Rewards / EMRD (optional)
    webapp.router.add_get("/api/rewards/balance", get_rewards_balance)
    webapp.router.add_post("/api/rewards/claim", create_rewards_claim)

    webapp.router.add_get("/api/stories/stats/{share_id}", get_stats)
    webapp.router.add_get("/api/stories/user/{user_id}", get_user_stories)
    webapp.router.add_get("/api/stories/top", get_top)

    webapp.router.add_get("/api/stories/card/{template}", get_card_image)
    webapp.router.add_get("/api/stories/card/share/{share_id}", get_share_card_image)

    # OPTIONS (CORS)
    for p in (
        "/api/stories/templates",
        "/api/stories/create",
        "/api/stories/click",
        "/api/stories/convert",
        "/api/rewards/balance",
        "/api/rewards/claim",
        "/api/stories/top",
        "/api/stories/card/{template}",
        "/api/stories/card/share/{share_id}",
        "/api/stories/stats/{share_id}",
        "/api/stories/user/{user_id}",
    ):
        webapp.router.add_route("OPTIONS", p, _cors_ok)

    logger.info("✅ Story sharing API routes registered")


async def get_templates(request: web.Request) -> web.Response:
    try:
        templates = []
        for key, template in STORY_TEMPLATES.items():
            templates.append({
                "id": key,
                "title": template["title"],
                "description": template["description"],
                "emoji": template["emoji"],
                "color": template["color"],
                "reward_points": template.get("reward_points", 0),
            })
        return _json(request, {"success": True, "templates": templates})
    except Exception as e:
        logger.error("Get templates error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def create_share(request: web.Request) -> web.Response:
    """POST /api/stories/create – nur Admins der Gruppe."""
    try:
        data = await request.json()
        uid = _resolve_uid(request)
        chat_id = int(data.get("chat_id", 0) or 0)
        template = (data.get("template") or "group_bot").strip()
        group_name = (data.get("group_name") or "").strip() or "Meine Gruppe"

        if uid <= 0:
            return _json(request, {"success": False, "error": "auth_required"}, status=403)
        if chat_id == 0:
            return _json(request, {"success": False, "error": "chat_id erforderlich"}, status=400)
        if template not in STORY_TEMPLATES:
            return _json(request, {"success": False, "error": f"unknown_template:{template}"}, status=400)

        if not await _is_admin(request.app, chat_id, uid):
            return _json(request, {"success": False, "error": "forbidden"}, status=403)

        sharing = {}
        try:
            sharing = get_story_settings(chat_id) or {}
        except Exception:
            sharing = {}

        if not bool(sharing.get("enabled", True)):
            return _json(request, {"success": False, "error": "story_sharing_disabled"}, status=403)

        templates_cfg = sharing.get("templates") or {}
        if template in templates_cfg and not bool(templates_cfg.get(template)):
            return _json(request, {"success": False, "error": "template_disabled"}, status=403)

        # Rate-Limit (pro User, pro Gruppe, pro Tag)
        try:
            limit = int(sharing.get("daily_limit", 5) or 5)
        except Exception:
            limit = 5
        used = count_shares_today(uid, chat_id)
        if used >= limit:
            return _json(request, {"success": False, "error": "rate_limited", "limit": limit, "used": used}, status=429)

        share = create_story_share(uid, chat_id, template, group_name=group_name)
        if not share:
            return _json(request, {"success": False, "error": "create_failed"}, status=500)

        # optional: reward for sharing
        if award_points:
            try:
                reward = int(sharing.get("reward_share", 0) or 0)
                if reward <= 0:
                    reward = int(STORY_TEMPLATES.get(template, {}).get("reward_points", 0) or 0)
                if reward > 0:
                    award_points(
                        user_id=uid,
                        chat_id=chat_id,
                        event_type="story_shared",
                        custom_points=reward,
                        metadata={"template": template, "via_story": True},
                    )
            except Exception as e:
                logger.warning("Could not award story share points: %s", e)

        origin = f"{request.scheme}://{request.host}"
        card_url = f"{origin}/api/stories/card/share/{share['share_id']}"

        return _json(request, {
            "success": True,
            "share": {
                "share_id": share["share_id"],
                "chat_id": chat_id,
                "template": share["template"],
                "referral_link": share["referral_link"],
                "card_url": card_url,
                "title": share.get("title"),
                "description": share.get("description"),
            }
        })
    except Exception as e:
        logger.error("Create share error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def get_share_card_image(request: web.Request) -> web.Response:
    """GET /api/stories/card/share/{share_id} – PNG Card für Telegram Story."""
    try:
        share_id = int(request.match_info.get("share_id", "0") or 0)
        row = get_share_by_id(share_id)
        if not row:
            return _json(request, {"success": False, "error": "not_found"}, status=404)

        template = row.get("template") or "group_bot"
        referral_link = row.get("referral_link") or ""
        chat_id = int(row.get("chat_id") or 0)

        group_name = "Meine Gruppe"
        try:
            name = get_group_title(chat_id)
            if name:
                group_name = str(name)
        except Exception:
            pass

        card_data = generate_share_card(template, group_name, referral_link)
        if not card_data:
            return _json(request, {"success": False, "error": "card_failed"}, status=500)

        return web.Response(
            body=card_data,
            content_type="image/png",
            headers={**_cors_headers(request), "Cache-Control": "public, max-age=3600"},
        )
    except Exception as e:
        logger.error("Get share card image error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def click_share(request: web.Request) -> web.Response:
    """POST /api/stories/click – Click Tracking (public)."""
    try:
        data = await request.json()
        share_id = int(data.get("share_id", 0) or 0)
        source = (data.get("source") or "story")[:50]

        visitor_id = int(data.get("visitor_id", 0) or 0)
        if visitor_id <= 0:
            visitor_id = _resolve_uid(request) or 0

        if not share_id:
            return _json(request, {"success": False, "error": "share_id erforderlich"}, status=400)

        ok = track_story_click(share_id, visitor_id or 0, source)
        if not ok:
            return _json(request, {"success": False, "error": "track_failed"}, status=500)

        # Optional: Click-Milestone Rewards (pro 10 neue Clicks), abgesichert über click_rewarded_steps
        if award_points:
            try:
                row = get_share_by_id(share_id) or {}
                chat_id = int(row.get("chat_id") or 0)
                referrer = int(row.get("user_id") or 0)
                clicks = int(row.get("clicks") or 0)
                rewarded_steps = int(row.get("click_rewarded_steps") or 0)

                sharing = get_story_settings(chat_id) if chat_id else {}
                if sharing and int(sharing.get("reward_clicks", 0) or 0) > 0:
                    step = clicks // 10
                    if step > rewarded_steps and referrer > 0:
                        reward = int(sharing.get("reward_clicks") or 0)
                        award_points(
                            user_id=referrer,
                            chat_id=chat_id,
                            event_type="story_click_milestone",
                            custom_points=reward,
                            metadata={"share_id": share_id, "step": step, "via_story": True},
                        )
                        set_click_rewarded_steps(share_id, step)
            except Exception as e:
                logger.debug("click milestone reward skipped: %s", e)

        return _json(request, {"success": True})
    except Exception as e:
        logger.error("Click share error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def convert_share(request: web.Request) -> web.Response:
    """POST /api/stories/convert – Conversion Tracking (public).
    Sicherheitsregel: referrer_id kommt NICHT aus dem Client, sondern aus share_id.
    """
    try:
        data = await request.json()
        share_id = int(data.get("share_id", 0) or 0)
        conversion_type = (data.get("conversion_type") or "joined_group")[:50]

        visitor_id = int(data.get("visitor_id", 0) or 0)
        if visitor_id <= 0:
            visitor_id = _resolve_uid(request) or 0

        if not share_id or visitor_id <= 0:
            return _json(request, {"success": False, "error": "share_id/visitor_id erforderlich"}, status=400)

        row = get_share_by_id(share_id)
        if not row:
            return _json(request, {"success": False, "error": "not_found"}, status=404)

        referrer_id = int(row.get("user_id") or 0)
        chat_id = int(row.get("chat_id") or 0)

        if referrer_id <= 0 or chat_id == 0:
            return _json(request, {"success": False, "error": "invalid_share"}, status=400)

        if visitor_id == referrer_id:
            return _json(request, {"success": False, "error": "self_ref_not_allowed"}, status=400)

        sharing = {}
        try:
            sharing = get_story_settings(chat_id) or {}
        except Exception:
            sharing = {}

        if not bool(sharing.get("enabled", True)):
            return _json(request, {"success": False, "error": "story_sharing_disabled"}, status=403)
        if not bool(sharing.get("referral_tracking", True)):
            return _json(request, {"success": False, "error": "referral_tracking_disabled"}, status=403)

        reward_points = int(sharing.get("reward_referral", 100) or 100)

        ok = record_story_conversion(
            share_id=share_id,
            referrer_id=referrer_id,
            visitor_id=visitor_id,
            conversion_type=conversion_type,
            reward_points=float(reward_points),
        )
        if not ok:
            return _json(request, {"success": False, "error": "convert_failed"}, status=500)

        if award_points:
            try:
                if reward_points > 0:
                    award_points(
                        user_id=referrer_id,
                        chat_id=chat_id,
                        event_type="referred_user",
                        custom_points=int(reward_points),
                        metadata={"conversion_type": conversion_type, "via_story": True, "share_id": share_id},
                    )
            except Exception as e:
                logger.warning("Could not award referral points: %s", e)

        return _json(request, {"success": True})
    except Exception as e:
        logger.error("Convert share error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def get_stats(request: web.Request) -> web.Response:
    """GET /api/stories/stats/{share_id} – nur Share-Owner oder Admin."""
    try:
        share_id = int(request.match_info.get("share_id", 0) or 0)
        uid = _resolve_uid(request)
        if not share_id:
            return _json(request, {"success": False, "error": "share_id erforderlich"}, status=400)

        row = get_share_by_id(share_id)
        if not row:
            return _json(request, {"success": False, "error": "not_found"}, status=404)

        chat_id = int(row.get("chat_id") or 0)
        owner = int(row.get("user_id") or 0)
        if uid <= 0:
            return _json(request, {"success": False, "error": "auth_required"}, status=403)

        if uid != owner and not await _is_admin(request.app, chat_id, uid):
            return _json(request, {"success": False, "error": "forbidden"}, status=403)

        stats = get_share_stats(share_id)
        if not stats:
            return _json(request, {"success": False, "error": "not_found"}, status=404)

        return _json(request, {"success": True, "stats": stats})
    except Exception as e:
        logger.error("Get stats error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def get_user_stories(request: web.Request) -> web.Response:
    """GET /api/stories/user/{user_id} – nur der User selbst."""
    try:
        user_id = int(request.match_info.get("user_id", 0) or 0)
        uid = _resolve_uid(request)
        if uid <= 0 or uid != user_id:
            return _json(request, {"success": False, "error": "forbidden"}, status=403)

        limit = int(request.rel_url.query.get("limit", 10) or 10)
        chat_id = int(request.rel_url.query.get("chat_id", 0) or 0) or None

        shares = get_user_shares(user_id, limit=limit, chat_id=chat_id)
        return _json(request, {"success": True, "shares": shares, "count": len(shares)})
    except Exception as e:
        logger.error("Get user stories error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def get_top(request: web.Request) -> web.Response:
    """GET /api/stories/top?chat_id=... – nur Admins, wenn Leaderboard aktiv."""
    try:
        uid = _resolve_uid(request)
        chat_id = int(request.rel_url.query.get("chat_id", 0) or 0)
        days = int(request.rel_url.query.get("days", 7) or 7)
        limit = int(request.rel_url.query.get("limit", 10) or 10)

        if uid <= 0:
            return _json(request, {"success": False, "error": "auth_required"}, status=403)
        if chat_id == 0:
            return _json(request, {"success": False, "error": "chat_id erforderlich"}, status=400)
        if not await _is_admin(request.app, chat_id, uid):
            return _json(request, {"success": False, "error": "forbidden"}, status=403)

        sharing = get_story_settings(chat_id) or {}
        if not bool(sharing.get("leaderboard_enabled", False)):
            return _json(request, {"success": False, "error": "leaderboard_disabled"}, status=403)

        shares = get_top_shares(days=days, limit=limit, chat_id=chat_id)
        return _json(request, {"success": True, "shares": shares, "period_days": days, "chat_id": chat_id})
    except Exception as e:
        logger.error("Get top error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def get_card_image(request: web.Request) -> web.Response:
    """GET /api/stories/card/{template}?group=...&link=... – generisches Card Template."""
    try:
        template = (request.match_info.get("template") or "group_bot").strip()
        group_name = (request.rel_url.query.get("group") or "Meine Gruppe")[:80]
        referral_link = (request.rel_url.query.get("link") or "")[:400]

        card_data = generate_share_card(template, group_name, referral_link)
        if not card_data:
            return _json(request, {"success": False, "error": "card_failed"}, status=500)

        return web.Response(
            body=card_data,
            content_type="image/png",
            headers={**_cors_headers(request), "Cache-Control": "public, max-age=3600"},
        )
    except Exception as e:
        logger.error("Get card image error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)

async def get_rewards_balance(request: web.Request) -> web.Response:
    """GET /api/rewards/balance – zeigt ausstehende Rewards (Option 2: rewards_pending) global pro User."""
    try:
        uid = _resolve_uid(request)
        if uid <= 0:
            return _json(request, {"success": False, "error": "auth_required"}, status=403)
        if not callable(get_pending_rewards):
            return _json(request, {"success": False, "error": "rewards_not_configured"}, status=501)
        bal = float(get_pending_rewards(uid))
        return _json(request, {"success": True, "user_id": uid, "balance": bal})
    except Exception as e:
        logger.error("get_rewards_balance error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)


async def create_rewards_claim(request: web.Request) -> web.Response:
    """POST /api/rewards/claim – erstellt einen Claim (Queue). Auszahlung erfolgt später über Worker."""
    try:
        uid = _resolve_uid(request)
        data = await request.json()
        chat_id = int(data.get("chat_id", 0) or 0)
        amount = float(data.get("amount", 0) or 0)
        wallet = (data.get("wallet_address") or "").strip()

        if uid <= 0:
            return _json(request, {"success": False, "error": "auth_required"}, status=403)
        if chat_id == 0:
            return _json(request, {"success": False, "error": "chat_id erforderlich"}, status=400)
        if amount <= 0:
            return _json(request, {"success": False, "error": "amount muss > 0 sein"}, status=400)
        if not wallet:
            return _json(request, {"success": False, "error": "wallet_address erforderlich"}, status=400)
        if not callable(create_reward_claim):
            return _json(request, {"success": False, "error": "rewards_not_configured"}, status=501)

        result = await create_reward_claim(uid, wallet, claim_type="manual")
        if not result or not result.get("success"):
            return _json(request, {"success": False, "error": result.get("message","claim_failed") if result else "claim_failed"}, status=400)

        return _json(request, {"success": True, "claim_id": int(result.get("claim_id"))})
    except Exception as e:
        logger.error("create_rewards_claim error: %s", e, exc_info=True)
        return _json(request, {"success": False, "error": str(e)}, status=500)