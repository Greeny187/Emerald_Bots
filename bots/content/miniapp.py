import base64
import json
import os
import urllib.parse
import logging
import asyncio
import hmac
import hashlib
import inspect
from io import BytesIO
from aiohttp import web
from aiohttp.web_response import Response
from typing import List, Tuple, Optional
from zoneinfo import ZoneInfo
from datetime import date, timedelta, datetime
import datetime as dt
from decimal import Decimal
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, InputFile
from telegram.constants import ChatMemberStatus
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from functools import partial
from bots.content.database import upsert_forum_topic
from .access import parse_webapp_user_id, is_admin_or_owner
from .patchnotes import __version__, PATCH_NOTES
from shared.payments import create_payment_order 

logger = logging.getLogger(__name__)

# === Konfiguration ============================================================
MINIAPP_URL = os.getenv(
    "MINIAPP_URL",
    # dein neuer Pfad:
    "https://greeny187.github.io/EmeraldContentBots/miniapp/appcontent.html"
)
MINIAPP_API_BASE = os.getenv("MINIAPP_API_BASE", "").rstrip("/")
BOT_TOKEN = (os.getenv("BOT1_TOKEN"))
SECRET = hashlib.sha256(BOT_TOKEN.encode()).digest() if BOT_TOKEN else None

TOKENS = [
    os.getenv("BOT1_TOKEN"),
    os.getenv("BOT2_TOKEN"),
    os.getenv("BOT3_TOKEN"),
    os.getenv("BOT4_TOKEN"),
    os.getenv("BOT5_TOKEN"),
    os.getenv("BOT6_TOKEN"),
]
TOKENS = [t for t in TOKENS if t]  # nur gesetzte Tokens

# Sammelbecken für alle Tokens (auch dynamisch von PTB-Apps)
_ALL_TOKENS: set[str] = set(TOKENS)

routes = web.RouteTableDef()

def _all_token_secrets() -> list[bytes]:
    secs: list[bytes] = []
    for t in list(_ALL_TOKENS):
        try:
            secs.append(hashlib.sha256(t.encode()).digest())
        except Exception:
            pass
    return secs

def _verify_with_secret(init_data: str, secret: bytes) -> int:
    # Nach Telegram-Doku: data_check_string = sortierte key=value-Liste ohne 'hash'
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

def _verify_init_data_any(init_data: str) -> int:
    """
    Verifiziert Telegram WebApp initData.
    SECURITY: Ohne gültige Hash-Verifikation wird KEIN user.id akzeptiert.
    (Spoofing-Schutz)
    """
    if not init_data:
        return 0

    # Offizieller Weg: hash-basierte Verifikation gegen bekannte BOT_TOKENs
    try:
        for secret in _all_token_secrets():  
            uid = _verify_with_secret(init_data, secret)  
            if uid:
                return uid
    except Exception:
        pass

    # Kein unsicherer Fallback in PROD.
    return 0

def _resolve_uid(request: web.Request) -> int:
    # 1) WebApp InitData aus Header / Query ziehen
    init_str = (
        request.headers.get("X-Telegram-Init-Data")
        or request.headers.get("x-telegram-web-app-data")
        or request.query.get("init_data")
    )

    uid = parse_webapp_user_id(init_str)
    if uid > 0:
        return uid

    # 2) Fallback: uid in der Query (für Browser-Tests)
    q_uid = request.query.get("uid")
    if os.getenv("ALLOW_BROWSER_DEV", "0") == "1" and q_uid and str(q_uid).lstrip("-").isdigit():
        return int(q_uid)

    # 3) Optionaler Dev-Bypass
    if (
        os.getenv("ALLOW_BROWSER_DEV") == "1"
        and request.headers.get("X-Dev-Token") == os.getenv("DEV_TOKEN", "")
    ):
        return int(request.headers.get("X-Dev-User-Id", "0") or 0)

    return 0


# Helfer für JSON (datetime-fest) + CORS
def _json_default(o):
    # Einheitlich über 'dt' (das Modul) prüfen – kein Shadowing mehr:
    if isinstance(o, (dt.datetime, dt.date)):
        return o.isoformat()
    return None

def _cors_json(data, status=200):
    return web.json_response(
        data,
        dumps=lambda obj, **kw: json.dumps(obj, default=_json_default, **kw),
        status=status,
        headers={
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Vary": "Origin"
        }
    )
# (neu/vereinheitlicht) – belasse die Funktion wie sie ist:

async def _cors_ok(request):
    return web.json_response(
        {}, status=204,
        headers={
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
        }
    )

# Für jede Route zusätzlich:
@routes.options('/miniapp/groups')
async def route_groups_preflight(request):
    return await _cors_ok(request)

@routes.options('/miniapp/state')
async def route_state_preflight(request):
    return await _cors_ok(request)

@routes.options('/miniapp/stats')
async def route_stats_preflight(request):
    return await _cors_ok(request)

@routes.options('/miniapp/apply')
async def route_apply_preflight(request):
    return await _cors_ok(request)

@routes.options('/miniapp/file')
async def route_file_preflight(request):
    return await _cors_ok(request)

@routes.get('/miniapp/groups')
async def route_groups(request: web.Request):
    """
    Laden NUR der ADMIN-Gruppen des Users.
    SICHERHEIT: Diese Route **MUSS** einen Admin-Check durchführen!
    
    Admin-Check erfolgt JETZT HIER – nicht später!
    """
    webapp = request.app
    uid = _resolve_uid(request)
    logger.info(f"[miniapp/groups] Request from uid={uid}")
    
    if uid <= 0:
        logger.warning(f"[miniapp/groups] Authentication failed: uid={uid}")
        return _cors_json({"groups": []}, 403)
    
    try:
        db = _db()
        fetch_func = db.get("get_registered_groups")
        if not fetch_func:
            logger.warning("[miniapp/groups] get_registered_groups nicht verfügbar")
            return _cors_json({"groups": []})
        
        # Hole alle registrierten Gruppen aus der DB
        admin_groups = []
        try:
            rows = fetch_func()
        except Exception as e:
            logger.error(f"[miniapp/groups] Failed to fetch groups: {e}")
            rows = []
        
        # **KRITISCH**: Filtere nach Admin-Status für DIESEN User
        for r in rows or []:
            try:
                # Parse Zeile (tuple oder dict)
                if isinstance(r, dict):
                    gid = r.get("chat_id") or r.get("id")
                    title = r.get("title") or r.get("name") or ""
                else:
                    gid = r[0] if len(r) > 0 else None
                    title = r[1] if len(r) > 1 else ""
                
                if not gid:
                    continue
                
                try:
                    gid = int(gid)
                except (ValueError, TypeError):
                    continue
                
                # **SECURITY CHECK**: Prüfe, ob dieser User Admin/Owner in dieser Gruppe ist!
                if not await _is_admin(webapp, gid, uid):
                    logger.debug(f"[miniapp/groups] User {uid} ist NICHT admin in {gid}")
                    continue
                
                # Nur Admin-Gruppen zurückgeben
                admin_groups.append({"id": gid, "title": title or f"Gruppe {gid}"})
                logger.debug(f"[miniapp/groups] User {uid} ist admin in {gid}")
                
            except Exception as e:
                logger.debug(f"[miniapp/groups] Error processing group {r}: {e}")
                continue
        
        logger.info(f"[miniapp/groups] Returning {len(admin_groups)}/{len(rows or [])} admin groups for uid={uid}")
        return _cors_json({"groups": admin_groups})
    
    except Exception as e:
        logger.error(f"[miniapp/groups] Exception: {e}", exc_info=True)
        return _cors_json({"groups": []})

def _parse_init_user(request):
    """
    Liest X-Telegram-Init-Data und gibt (uid, ok) zurück.
    Falls du bereits eine eigene verify/parse-Funktion hast, nutze die hier.
    """
    init = request.headers.get("X-Telegram-Init-Data", "")
    try:
        # Minimal-Parse auf user.id (die Signatur-Prüfung machst du evtl. schon in Middleware)
        from urllib.parse import parse_qs
        qs = parse_qs(init, keep_blank_values=True)
        user_json = qs.get("user", [None])[0]
        if not user_json:
            return (None, False)
        import json as _json
        uid = _json.loads(user_json)["id"]
        return (int(uid), True)
    except Exception:
        return (None, False)
    
def _clean_dict_empty_to_none(d: dict) -> dict:
    """Konvertiert leere Strings in einem dict zu None."""
    return {k: (None if (isinstance(v, str) and v.strip() == "") else v) for k, v in d.items()}

def _topic_id_or_none(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("", "0", "none", "null"):
        return None
    try:
        return int(s)
    except Exception:
        return None

def _none_if_blank(v):
    return None if (v is None or (isinstance(v, str) and v.strip() == "")) else v

async def _upload_get_file_id(app: Application, target_chat_id: int, base64_str: str) -> str | None:
    """
    Nimmt eine Data-URL/Base64 und lädt das Bild via Bot hoch. Gibt file_id zurück.
    Wichtig: Upload NICHT mehr in die Gruppe, sondern per DM an den Admin (target_chat_id=uid),
    damit nichts in der Gruppe gepostet wird.
    
    Validiert:
    - Maximale Bildgröße (10MB)
    - Erlaubte Bildformate (JPG, PNG, GIF)
    - Base64-Format
    """
    if not base64_str:
        return None
        
    # Format-Validierung
    try:
        content_type = base64_str.split(";")[0].split(":")[1].lower()
        if not any(fmt in content_type for fmt in ["jpeg", "jpg", "png", "gif"]):
            logger.warning(f"[miniapp] Ungültiges Bildformat: {content_type}")
            return None
    except Exception:
        logger.warning("[miniapp] Ungültiges Base64-Format")
        return None
        
    try:
        b64 = base64_str.split(",", 1)[-1]
        raw = base64.b64decode(b64)
        
        # Größen-Check (10MB)
        if len(raw) > 10 * 1024 * 1024:
            logger.warning(f"[miniapp] Bild zu groß: {len(raw)} bytes")
            return None
            
        bio = BytesIO(raw)
        bio.name = f"upload.{content_type.split('/')[-1]}"
        # bevorzugt DM an den Admin (leise)
        msg = await app.bot.send_photo(target_chat_id, InputFile(bio), disable_notification=True)
    except Exception as e:
        # Fallback: versuche nochmal den gleichen Chat (kein Gruppen-Spam!)
        logger.warning(f"[miniapp] DM-Upload fehlgeschlagen: {e}")
        return None
    return msg.photo[-1].file_id if msg.photo else None

# ---------- Gemeinsame Speicherroutine (von beiden Wegen nutzbar) ----------
async def _save_from_payload(cid:int, uid:int, data:dict, app:Application|None) -> list[str]:
    db = _db()
    errors: list[str] = []
 
    # --- Vorverarbeitung: Kompatibilität für direkte {*_image_base64} Keys -----------
    # Frontend kann senden: welcome_image_base64, rules_image_base64, farewell_image_base64
    for key_type in ("welcome", "rules", "farewell"):
        key_name = f"{key_type}_image_base64"
        if key_name in data:
            d = data.setdefault(key_type, {})
            d["img_base64"] = data.pop(key_name)
    
    # --- Vorverarbeitung: Kompatibilität für { images: { welcome|rules|farewell } } -----------
    try:
        imgs = data.get("images") or {}
        for k in ("welcome","rules","farewell"):
            val = imgs.get(k)
            if isinstance(val, dict):
                d = data.setdefault(k, {})
                if val.get("clear") is True:
                    d["img_base64"] = ""   # explizit Foto löschen
                elif val.get("img_base64"):
                    d["img_base64"] = val["img_base64"]
    except Exception:
        pass

    # Hilfsfunktion: nie auto-löschen, nur bei expliziten Flags
    async def _upsert_media(kind:str, block:dict):
        # bestehende Werte laden
        try:
            getter = {"welcome":"get_welcome","rules":"get_rules","farewell":"get_farewell"}[kind]
            setter = {"welcome":"set_welcome","rules":"set_rules","farewell":"set_farewell"}[kind]
            deleter= {"welcome":"delete_welcome","rules":"delete_rules","farewell":"delete_farewell"}[kind]
        except KeyError:
            return
        try:
            existing_photo, existing_text = db[getter](cid) or (None, None)
        except Exception:
            existing_photo, existing_text = (None, None)

        on_present = ("on" in block)
        on_value   = bool(block.get("on")) if on_present else None

        # Text: nur übernehmen, wenn im Payload vorhanden, sonst beibehalten
        text = (block.get("text") if "text" in block else existing_text)
        if text is not None:
            text = _none_if_blank(text)

        # Bild: nur ändern, wenn img_base64 im Payload vorhanden
        photo_id = existing_photo
        if "img_base64" in block:
            v = block.get("img_base64")
            if isinstance(v, str) and v == "":
                photo_id = None                       # nur Foto löschen
            elif app and _none_if_blank(v):
                tmp = await _upload_get_file_id(app, uid, v)  # DM-Upload
                if tmp: photo_id = tmp

        # Nur löschen, wenn Nutzer *bewusst* in diesem Block agiert hat:
        # - on:false und (Text-Feld vorhanden ODER img_base64-Feld vorhanden)
        # So vermeiden wir, dass fehlende UI-Initialisierung (Checkbox = false)
        # bestehende Inhalte wegputzt.
        if on_present and on_value is False and (("text" in block) or ("img_base64" in block)):
            db[deleter](cid)
            return

        # Speichern, falls Text oder Foto da ist – sonst NIX (kein Auto-Delete!)
        if (text or photo_id):
            db[setter](cid, photo_id, text)
        # Falls explizit on:true aber ohne Inhalt → trotzdem nichts löschen    
        
    # --- Captcha ---
    try:
        if "captcha" in data:
            c = data["captcha"] or {}
            # bestehende Werte holen, damit wir nie NOT NULL verletzen
            try:
                old_enabled, old_type, old_behavior = db["get_captcha_settings"](cid)
            except Exception:
                old_enabled, old_type, old_behavior = (False, "button", "kick")
            enabled  = bool(c.get("enabled")) if ("enabled" in c) else bool(old_enabled)
            ctype    = (c.get("type") or old_type or "button")
            if ctype not in ("button", "math"): ctype = old_type or "button"
            behavior = (c.get("behavior") or old_behavior or "kick")
            if behavior not in ("kick", "mute", "none"): behavior = old_behavior or "kick"
            db["set_captcha_settings"](cid, enabled, ctype, behavior)
    except Exception as e:
        errors.append(f"Captcha: {e}")

    # --- Welcome ---
    try:
        if "welcome" in data:
            w_data = data.get("welcome") or {}
            await _upsert_media("welcome", w_data)
            # Handle captcha flag if present in welcome data
            if "captcha" in w_data:
                try:
                    old_enabled, old_type, old_behavior = db["get_captcha_settings"](cid)
                except Exception:
                    old_enabled, old_type, old_behavior = (False, "button", "kick")
                enabled = bool(w_data.get("captcha"))
                db["set_captcha_settings"](cid, enabled, old_type or "button", old_behavior or "kick")
                logger.info(f"[miniapp] Captcha from welcome set cid={cid} enabled={enabled}")
    except Exception as e:
        errors.append(f"Welcome: {e}")

    # --- Rules ---
    try:
        if "rules" in data:
            await _upsert_media("rules", data.get("rules") or {})
    except Exception as e:
        errors.append(f"Rules: {e}")

    # --- Farewell ---
    try:
        if "farewell" in data:
            await _upsert_media("farewell", data.get("farewell") or {})
    except Exception as e:
        errors.append(f"Farewell: {e}")

    # --- Links/Spam ---
        # Quick-Actions aus der UI (ohne die restlichen Settings anzufassen)
    try:
        rst = data.get("spam_topic_reset")
        if isinstance(rst, dict) and str(rst.get("topic_id","")).strip().lstrip("-").isdigit():
            tid = int(str(rst.get("topic_id")).strip())
            db["delete_spam_policy_topic"](cid, tid)
    except Exception as e:
        errors.append(f"Spam Reset: {e}")

    try:
        if data.get("topics_sync") is True:
            try:
                db["sync_user_topic_names"](cid)
            except Exception:
                pass
    except Exception as e:
        errors.append(f"Topics Sync: {e}")

        # Haupt-Speicherlogik    
    try:
        sp = data.get("spam") or {}

        # 1) Linkschutz (separat!) – NICHT automatisch beim Spamfilter aktivieren
        admins_only = bool(sp.get("block_links") or data.get("admins_only"))
        db["set_link_settings"](cid, admins_only=admins_only)

        # 2) Globaler Spamfilter (spam_policy)
        on = bool(sp.get("on"))
        level = (sp.get("level") or "").strip().lower()
        if not on:
            level = "off"
        if on and level not in ("light", "medium", "strict"):
            level = "medium"

        # User-Whitelist (IDs) – pro User individuelle Ausnahme vom Spamfilter
        def _to_int_list(v):
            if v is None:
                return []
            if isinstance(v, list):
                raw = v
            else:
                raw = []
                for line in str(v).splitlines():
                    raw.extend([x.strip() for x in line.split(",")])
            out = []
            for s in raw:
                if not s:
                    continue
                if str(s).strip().lstrip("-").isdigit():
                    out.append(int(str(s).strip()))
            return sorted(list(dict.fromkeys(out)))

        user_wl = _to_int_list(sp.get("user_whitelist", ""))

        db["set_spam_policy"](cid, level=level, user_whitelist=user_wl)

        # 3) Topic-Override Editor (spam_policy_topic) – optional, mit Topic-Auswahl
        t_raw = str(sp.get("policy_topic") or "0").strip()
        topic_id = int(t_raw) if t_raw.lstrip("-").isdigit() else 0

        def _to_str_list(v):
            if isinstance(v, list):
                return [str(x).strip() for x in v if str(x).strip()]
            if isinstance(v, str):
                parts = []
                for line in v.splitlines():
                    parts.extend([s.strip() for s in line.split(",") if s.strip()])
                return parts
            return []

        fields = {}

        # Level pro Topic (optional)
        t_level = (sp.get("topic_level") or "").strip().lower()
        if t_level in ("off", "light", "medium", "strict"):
            fields["level"] = t_level

        # Link-Listen / Action
        fields["link_whitelist"] = _to_str_list(sp.get("whitelist", ""))
        fields["domain_blacklist"] = _to_str_list(sp.get("blacklist", ""))

        act = (sp.get("action") or "").strip().lower()
        if act in ("delete", "warn", "mute"):
            fields["action_primary"] = act

        lim = str(sp.get("per_user_daily_limit") or "").strip()
        if lim.isdigit():
            fields["per_user_daily_limit"] = int(lim)
        else:
            fields["per_user_daily_limit"] = 0

        qn = (sp.get("quota_notify") or "").strip().lower()
        if qn in ("off", "smart", "always"):
            fields["quota_notify"] = qn

        meaningful = (
            int(topic_id) == 0 or
            bool(fields.get("link_whitelist")) or bool(fields.get("domain_blacklist")) or
            bool(fields.get("action_primary")) or int(fields.get("per_user_daily_limit") or 0) > 0 or
            bool(fields.get("quota_notify")) or bool(fields.get("level"))
        )

        if meaningful:
            db["set_spam_policy_topic"](cid, int(topic_id), **fields)

        # 4) Direkt-Aktionen aus Buttons
        if isinstance(data.get("spam_topic_del"), dict):
            tid = data["spam_topic_del"].get("topic_id")
            if str(tid).lstrip("-").isdigit():
                db["delete_spam_policy_topic"](cid, int(tid))

        if isinstance(data.get("topic_user_set"), dict):
            u = data["topic_user_set"].get("user_id")
            t = data["topic_user_set"].get("topic_id")
            if str(u).lstrip("-").isdigit() and str(t).lstrip("-").isdigit():
                # topic_name optional auflösen
                topic_name = None
                try:
                    topics = db["list_forum_topics"](cid, limit=200, offset=0) or []
                    for (tid, name, _ls) in topics:
                        if int(tid) == int(t):
                            topic_name = name
                            break
                except Exception:
                   pass
                db["assign_topic"](cid, int(u), int(t), topic_name)

        if isinstance(data.get("topic_user_del"), dict):
            u = data["topic_user_del"].get("user_id")
            if str(u).lstrip("-").isdigit():
                t = data["topic_user_del"].get("topic_id")
                if str(t).lstrip("-").isdigit():
                    db["remove_topic"](cid, int(u), int(t))
                else:
                    db["remove_topic"](cid, int(u))

    except Exception as e:
        errors.append(f"Spam/Links: {e}")
        
    # --- RSS add/del/update ---
    if "rss" in data or "rss_update" in data or "rss_del" in data:
        r = data.get("rss") or {}
        if (r.get("url") or "").strip():
            url = r.get("url").strip()
            topic = int(r.get("topic") or 0)
            try: db["set_rss_topic"](cid, topic)
            except Exception: pass
            db["add_rss_feed"](cid, url, topic)
            db["set_rss_feed_options"](cid, url, post_images=bool(r.get("post_images")), enabled=bool(r.get("enabled", True)))
            logger.info(f"[miniapp] RSS add cid={cid} url={url} topic={topic} post_images={bool(r.get('post_images'))} enabled={bool(r.get('enabled', True))}")
        upd = data.get("rss_update") or None
        if upd and (upd.get("url") or "").strip():
            url=upd.get("url").strip()
            db["set_rss_feed_options"](cid, url, post_images=upd.get("post_images"), enabled=upd.get("enabled"))
        if data.get("rss_del"):
            del_url = data.get("rss_del")
            db["remove_rss_feed"](cid, del_url)
            logger.info(f"[miniapp] RSS del cid={cid} url={del_url}")
        if "rss_update" in data:
            u = data["rss_update"]; logger.info(f"[miniapp] RSS update cid={cid} {u}")

    # --- KI (Assistent/FAQ) ---
    try:
        ai = data.get("ai") or {}
        faq_on = bool(ai.get("on") or (ai.get("faq") or "").strip())
        db["set_ai_settings"](cid, faq=faq_on, rss=None)
    except Exception as e:
        errors.append(f"KI: {e}")

    # --- FAQ add/del ---
    try:
        faq_add = data.get("faq_add") or None
        if faq_add and (faq_add.get("q") or "").strip():
            db["upsert_faq"](cid, faq_add["q"].strip(), (faq_add.get("a") or "").strip())
        faq_del = data.get("faq_del") or None
        if faq_del and (faq_del.get("q") or "").strip():
            db["delete_faq"](cid, faq_del["q"].strip())
    except Exception as e:
        errors.append(f"FAQ: {e}")

    # --- KI-Moderation ---
    try:
        aimod = data.get("ai_mod") or {}
        if aimod:
            # Leere Strings zu None konvertieren!
            aimod_clean = _clean_dict_empty_to_none(aimod)
            allowed = {
              "enabled","shadow_mode","action_primary","mute_minutes","warn_text","appeal_url",
              "max_per_min","cooldown_s","exempt_admins","exempt_topic_owner",
              "toxicity","hate","sexual","harassment","selfharm","violence",
              "tox_thresh","hate_thresh","sex_thresh","harass_thresh","selfharm_thresh","violence_thresh"
            }
            payload={}
            for k in allowed:
                if k in aimod_clean and aimod_clean[k] is not None:
                    payload[k]=aimod_clean[k]
            alias={"toxicity":"tox_thresh","hate":"hate_thresh","sexual":"sex_thresh",
                   "harassment":"harass_thresh","selfharm":"selfharm_thresh","violence":"violence_thresh"}
            for k,v in list(payload.items()):
                if k in alias: payload[alias[k]]=v; del payload[k]
            db["set_ai_mod_settings"](cid, 0, **payload)
            logger.info("[miniapp] AIMOD: %s", payload)
    except Exception as e:
        errors.append(f"AI-Mod: {e}")
    # Daily Report
    try:
        if "report" in data and isinstance(data["report"], dict):
            db["set_daily_stats"](cid, bool(data["report"].get("enabled")))
        elif "daily_stats" in data:
            db["set_daily_stats"](cid, bool(data.get("daily_stats")))
    except Exception as e:
        errors.append(f"Daily-Report: {e}")
        
    # --- Statistik Sofort-Senden ---
    try:
        if data.get("report_send_now") and app:
            cfg = data.get("report", {}) or {}
            dest = (cfg.get("dest") or "dm").lower()
            topic_id = int(cfg.get("topic") or 0) or None

            d1 = date.today()
            d0 = d1  # „heute“; optional auf 7d erweitern
            summary = db["get_agg_summary"](cid, d0, d1)
            top = db["get_top_responders"](cid, d0, d1, 5) or []

            def _fmt_ms(ms):
                if ms is None: return "–"
                s = int(ms//1000)
                return f"{s//60}m {s%60}s" if s>=60 else f"{s}s"

            lines = [
            "📊 <b>Statistik</b> (heute)",
            f"• Nachrichten: <b>{summary['messages_total']}</b>",
            f"• Aktive Nutzer: <b>{summary['active_users']}</b>",
            f"• Joins/Leaves/Kicks: <b>{summary['joins']}/{summary['leaves']}/{summary['kicks']}</b>",
            f"• Antwortzeiten p50/p90: <b>{_fmt_ms(summary['reply_median_ms'])}/{_fmt_ms(summary['reply_p90_ms'])}</b>",
            f"• Assist-Hits/Helpful: <b>{summary['autoresp_hits']}/{summary['autoresp_helpful']}</b>",
            f"• Moderation (Spam/Nacht): <b>{summary['spam_actions']}/{summary['night_deletes']}</b>",
            ]
            if top:
                lines.append("<b>Top-Responder</b>")
                for (u, answers, avg_ms) in top:
                    s = int((avg_ms or 0)//1000)
                    s_str = f"{s//60}m {s%60}s" if s>=60 else f"{s}s"
                    lines.append(f"• <code>{u}</code>: <b>{answers}</b> Antworten, Ø {s_str}")
            text = "\n".join(lines)

            target_chat = cid if dest=="topic" else uid
            kw = {}
            if dest=="topic" and topic_id: kw["message_thread_id"] = topic_id

            await app.bot.send_message(chat_id=target_chat, text=text, parse_mode="HTML", **kw)
            logger.info(f"[miniapp] Statistik gesendet: dest={dest} chat={target_chat} topic={topic_id}")
    except Exception as e:
        logger.error(f"[miniapp] Fehler beim Senden der Statistik: {e}")
    # --- Mood ---
    try:
        mood = data.get("mood") or {}
        if mood:
            question = (mood.get("question") or "Wie ist deine Stimmung?").strip()
            topic_id = _topic_id_or_none(mood.get("topic", 0))
            db["set_mood_question"](cid, question)
            if topic_id:
                db["set_mood_topic"](cid, topic_id)
        if data.get("mood_send_now") and app:
            question = db["get_mood_question"](cid) or "Wie ist deine Stimmung?"
            topic_id = db["get_mood_topic"](cid) or None
            kb = InlineKeyboardMarkup([[  # wie gehabt …
                InlineKeyboardButton("👍", callback_data="mood_like"),
                InlineKeyboardButton("👎", callback_data="mood_dislike"),
                InlineKeyboardButton("🤔", callback_data="mood_think"),
            ]])
            await app.bot.send_message(chat_id=cid, text=question,
                                    message_thread_id=topic_id if topic_id else None,
                                    reply_markup=kb)
            logger.info(f"[miniapp] Mood prompt gesendet in {cid} (topic={topic_id})")
    except Exception as e:
        errors.append(f"Mood: {e}")

    # --- Sprache ---
    try:
        lang=(data.get("language") or "").strip()
        if lang: db["set_group_language"](cid, lang[:5])
    except Exception as e:
        errors.append(f"Sprache: {e}")

    # --- Clean Deleted: Scheduler speichern & Sofort-Aktion ---
    try:
        cd = data.get("clean_deleted") or None
        if cd is not None:
            hh, mm = 3, 0
            try:
                hh, mm = map(int, (cd.get("time") or "03:00").split(":"))
            except Exception:
                pass
            wd = cd.get("weekday", None)
            if wd == "" or wd is False:
                wd = None
            db["set_clean_deleted_settings"](cid,
                enabled = bool(cd.get("enabled")),
                hh = int(hh), mm = int(mm),
                weekday = wd if wd is None else int(wd),
                demote = bool(cd.get("demote")),
                notify = bool(cd.get("notify")),
            )
        if data.get("clean_delete_now"):
            from .utils import clean_delete_accounts_for_chat
            
            async def _clean_delete_task():
                try:
                    result = await clean_delete_accounts_for_chat(cid, app.bot)
                    logger.info(f"[miniapp] Clean delete completed for cid={cid}, removed={result}")
                except Exception as e:
                    logger.error(f"[miniapp] Clean delete failed for cid={cid}: {e}", exc_info=True)
            
            asyncio.create_task(_clean_delete_task())
    except Exception as e:
        errors.append(f"CleanDelete: {e}")
    
    # Nachtmodus
    try:
        night = data.get("night") or {}
        if ("on" in night) or ("start" in night) or ("end" in night) or ("timezone" in night) \
           or ("override_until" in night) or ("write_lock" in night) or ("lock_message" in night):
            def _hm_to_min(s, default):
                try:
                    h, m = str(s or "").split(":")
                    return int(h) * 60 + int(m)
                except Exception:
                    return default

            enabled = bool(night.get("on"))
            start_m = _hm_to_min(night.get("start") or "22:00", 1320)
            end_m   = _hm_to_min(night.get("end") or "07:00", 360)

            override_until = night.get("override_until")
            if isinstance(override_until, str) and override_until.strip() == "":
                override_until = None

            # Lock-Text säubern (leer = None → DB-Default verwenden)
            lock_msg = night.get("lock_message")
            if isinstance(lock_msg, str):
                lock_msg = lock_msg.strip() or None

            db["set_night_mode"](
                cid,
                enabled=enabled,
                start_minute=start_m,
                end_minute=end_m,
                delete_non_admin_msgs=night.get("delete_non_admin_msgs"),
                warn_once=night.get("warn_once"),
                timezone=night.get("timezone"),
                hard_mode=night.get("hard_mode"),
                override_until=override_until,
                write_lock=night.get("write_lock"),
                lock_message=lock_msg,
            )
    except Exception as e:
        errors.append(f"Nachtmodus: {e}")
        
    # --- Topic Router ---
    try:
        if "router_add" in data:
            r = data["router_add"]
            db["add_topic_router_rule"](cid, r["pattern"], r["target_topic_id"])
        if "router_toggle" in data:
            r = data["router_toggle"]
            db["toggle_topic_router_rule"](cid, r["rule_id"], r.get("enabled", True))
        if "router_delete" in data:
            r = data["router_delete"]
            db["delete_topic_router_rule"](cid, r["rule_id"])
    except Exception as e:
        errors.append(f"TopicRouter: {e}")

    # --- Pro kaufen/verlängern ---
    try:
        months = int(data.get("pro_months") or 0)
        if months>0:
            from datetime import datetime, timedelta
            from zoneinfo import ZoneInfo
            until = datetime.now(ZoneInfo("UTC")) + timedelta(days=30*months)
            db["set_pro_until"](cid, until, tier="pro")
    except Exception as e:
        errors.append(f"Pro-Abo: {e}")

    # --- PRO Payment Config ---
    try:
        pro = data.get("pro") or {}
        if pro:
            # Speichere Zahlungskonfiguration als globale PRO-Settings
            cfg = db.get("get_global_config", lambda *_: {})(f"pro_{cid}") or {}
            # Update mit neuen Werten
            if "payment_ton" in pro:
                cfg["payment_ton"] = bool(pro["payment_ton"])
            if "payment_ton_wallet" in pro:
                cfg["payment_ton_wallet"] = (pro.get("payment_ton_wallet") or "").strip()
            if "payment_near" in pro:
                cfg["payment_near"] = bool(pro["payment_near"])
            if "payment_near_wallet" in pro:
                cfg["payment_near_wallet"] = (pro.get("payment_near_wallet") or "emeraldcontent.near").strip()
            if "payment_coinbase" in pro:
                cfg["payment_coinbase"] = bool(pro["payment_coinbase"])
            if "payment_coinbase_key" in pro:
                cfg["payment_coinbase_key"] = (pro.get("payment_coinbase_key") or "").strip()
            if "payment_paypal" in pro:
                cfg["payment_paypal"] = bool(pro["payment_paypal"])
            if "payment_paypal_email" in pro:
                cfg["payment_paypal_email"] = (pro.get("payment_paypal_email") or "emerald@mail.de").strip()
            if "price_1m" in pro:
                cfg["price_1m"] = float(pro.get("price_1m") or 9.99)
            if "price_3m" in pro:
                cfg["price_3m"] = float(pro.get("price_3m") or 24.99)
            if "price_12m" in pro:
                cfg["price_12m"] = float(pro.get("price_12m") or 79.99)
            if "description" in pro:
                cfg["description"] = (pro.get("description") or "").strip()
            db.get("set_global_config", lambda *_: None)(f"pro_{cid}", cfg)
            logger.info(f"[miniapp] PRO config saved for cid={cid}")
    except Exception as e:
        errors.append(f"PRO Payment: {e}")

        # --- Clean Deleted Accounts (Einmal-Aktion) ---
    try:
        if data.get("clean_delete_now"):
            from .utils import clean_delete_accounts_for_chat
            
            async def _clean_delete_task_route():
                try:
                    result = await clean_delete_accounts_for_chat(cid, app.bot)
                    logger.info(f"[miniapp] Clean delete completed for cid={cid}, removed={result}")
                except Exception as e:
                    logger.error(f"[miniapp] Clean delete failed for cid={cid}: {e}", exc_info=True)
            
            asyncio.create_task(_clean_delete_task_route())
    except Exception as e:
        errors.append(f"CleanDelete: {e}")

    # --- Story-Sharing speichern (pro Gruppe) ---
    try:
        if isinstance(data.get("sharing"), dict):
            sh = data["sharing"]
            # Normalisiere Template-Schalter
            if isinstance(sh.get("templates"), dict):
                sh["templates"] = {k: bool(v) for k, v in sh["templates"].items()}
            if "set_story_settings" in db:
                db["set_story_settings"](cid, sh)
    except Exception as e:
        errors.append(f"StorySharing: {e}")

    return errors




# ---------- HTTP-Fallback: /miniapp/apply ----------
async def route_apply(request):
    app: Application = request.app["ptb_app"]
    if request.method == "OPTIONS":
        return _cors_json({})
    try:
        data = await request.json()
    except Exception:
        data = {}

    logger.info(
        "[miniapp] APPLY cid=%s uid=%s keys=%s",
        request.query.get("cid"),
        _resolve_uid(request),
        list(data.keys()),
    )

    cid = int(request.query.get("cid", "0") or 0)
    uid = _resolve_uid(request)
    if uid <= 0:
        return _cors_json({"error": "auth_required"}, 403)

    if not await _is_admin(app, cid, uid):
        return _cors_json({"error": "forbidden"}, 403)

    if not cid:
        return web.Response(status=400, text="cid fehlt")

    # ✅ Rewards global speichern – jetzt über _db(), ohne 500er
    if isinstance(data.get("rewards"), dict):
        try:
            db = _db()
            if "set_global_config" in db:
                db["set_global_config"]("rewards", data["rewards"])
        except Exception as e:
            logger.warning("[miniapp] set_global_config('rewards') failed: %s", e)

    errors = await _save_from_payload(cid, uid, data, request.app["ptb_app"])
    if errors:
        return web.Response(
            status=207,
            text="Teilweise gespeichert:\n- " + "\n- ".join(errors),
        )
    return web.Response(text="✅ Einstellungen gespeichert.")

@routes.post('/miniapp/pay')
async def route_pay(request: web.Request):
    app: Application = request.app["ptb_app"]

    if request.method == "OPTIONS":
        return _cors_json({})

    # User & Chat aus Request holen
    cid = int(request.query.get("cid", "0") or 0)
    uid = _resolve_uid(request)
    if uid <= 0:
        return _cors_json({"error": "auth_required"}, 403)

    # Optional: nur Admins erlauben, oder später lockern
    if not await _is_admin(app, cid, uid):
        return _cors_json({"error": "forbidden"}, 403)

    try:
        data = await request.json()
    except Exception:
        data = {}

    plan = data.get("plan") or "pro_monthly"
    provider = data.get("provider") or "walletconnect_ton"

    from shared import payments  # falls nötig
    try:
        order = payments.create_payment_order(provider, plan, uid, cid)
    except Exception as e:
        logging.exception("payment error")
        return _cors_json({"error": str(e)}, 500)

    pay_url = order.get("pay_url")
    if not pay_url:
        return _cors_json({"error": "no_url"}, 500)

    return _cors_json({"ok": True, "url": pay_url, "order_id": order.get("order_id")})

async def route_file(request: web.Request):
    try:
        file_id = request.query.get("file_id") or request.query.get("id")
        if not file_id:
            return web.Response(
                status=400,
                text="file_id required",
                headers={"Access-Control-Allow-Origin": ALLOWED_ORIGIN},
            )

        # Erste PTB-App nehmen
        appweb = request.app
        botapp = appweb["ptb_app"]

        f = await botapp.bot.get_file(file_id)
        blob = await botapp.bot.request.retrieve(f.file_path)

        lower = (f.file_path or "").lower()
        if lower.endswith(".png"):
            ctype = "image/png"
        elif lower.endswith(".gif"):
            ctype = "image/gif"
        else:
            ctype = "image/jpeg"

        return web.Response(
            body=blob,
            content_type=ctype,
            headers={
                "Cache-Control": "public, max-age=86400",
                "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            },
        )
    except Exception as e:
        logger.warning(f"[miniapp] file proxy failed: {e}")
        return web.Response(
            status=404,
            text="not found",
            headers={"Access-Control-Allow-Origin": ALLOWED_ORIGIN},
        )

# Erlaubter Origin für CORS (aus MINIAPP_URL abgeleitet)
def _origin(url: str) -> str:
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"
    except Exception:
        return "*"

ALLOWED_ORIGIN = _origin(MINIAPP_URL)

# === DB-Brücke: Funktionen dynamisch laden (shared.database ODER lokale database) ===
def _db():
    # Nur noch lokale DB – kein shared.database mehr
    try:
        from .database import (
            get_registered_groups,get_clean_deleted_settings, set_clean_deleted_settings,
            set_welcome, delete_welcome, get_welcome,
            set_rules, delete_rules, get_rules, get_group_stats,
            set_farewell, delete_farewell, get_farewell, get_agg_summary, get_heatmap,
            get_link_settings, set_link_settings, get_spam_policy, set_spam_policy, get_spam_policy_topic, set_spam_policy_topic,
            list_spam_policy_topics, delete_spam_policy_topic,
            list_forum_topics, sync_user_topic_names, list_user_topics, effective_spam_policy,
            assign_topic, remove_topic, 
            get_rss_topic, set_rss_topic_for_group_feeds, add_rss_feed, remove_rss_feed, set_rss_feed_options, list_rss_feeds,
            get_ai_settings, set_ai_settings, upsert_faq, delete_faq,
            set_daily_stats, is_daily_stats_enabled, get_top_responders, get_agg_rows,
            set_mood_question, get_mood_question, set_mood_topic, get_mood_topic,
            set_group_language, set_night_mode, add_topic_router_rule, get_effective_link_policy, 
            get_rss_feeds_full, get_subscription_info, effective_ai_mod_policy, get_ai_mod_settings, 
            set_ai_mod_settings, list_faqs, list_topic_router_rules, get_night_mode, set_pro_until,
            get_captcha_settings, set_captcha_settings, get_global_config, set_global_config,
            count_forum_topics, upsert_forum_topic, get_story_settings, set_story_settings
        )
        # explizit ein dict bauen, damit die Funktionen korrekt referenziert werden
        return {
            "get_story_settings": get_story_settings,
            "set_story_settings": set_story_settings,
            "get_clean_deleted_settings": get_clean_deleted_settings,
            "set_clean_deleted_settings": set_clean_deleted_settings,
            "get_captcha_settings": get_captcha_settings,
            "set_captcha_settings": set_captcha_settings,
            "get_agg_summary": get_agg_summary,
            "get_heatmap": get_heatmap,
            "get_registered_groups": get_registered_groups,
            "set_welcome": set_welcome,
            "delete_welcome": delete_welcome,
            "get_welcome": get_welcome,
            "set_rules": set_rules,
            "delete_rules": delete_rules,
            "get_rules": get_rules,
            "set_farewell": set_farewell,
            "delete_farewell": delete_farewell,
            "get_farewell": get_farewell,
            "get_link_settings": get_link_settings,
            "set_link_settings": set_link_settings,
            "get_spam_policy": get_spam_policy,
            "set_spam_policy": set_spam_policy,
            "get_spam_policy_topic": get_spam_policy_topic,
            "set_spam_policy_topic": set_spam_policy_topic,
            "list_spam_policy_topics": list_spam_policy_topics,
            "delete_spam_policy_topic": delete_spam_policy_topic,
            "list_forum_topics": list_forum_topics,
            "assign_topic": assign_topic,
            "remove_topic": remove_topic,
            "list_forum_topics": list_forum_topics,
            "sync_user_topic_names": sync_user_topic_names,
            "list_user_topics": list_user_topics,
            "delete_spam_policy_topic": delete_spam_policy_topic,
            "effective_spam_policy": effective_spam_policy,
            "list_user_topics": list_user_topics,
            "set_rss_topic": set_rss_topic_for_group_feeds,
            "get_rss_topic": get_rss_topic,
            "add_rss_feed": add_rss_feed,
            "remove_rss_feed": remove_rss_feed,
            "set_rss_feed_options": set_rss_feed_options,
            "list_rss_feeds": list_rss_feeds,
            "get_ai_settings": get_ai_settings,
            "set_ai_settings": set_ai_settings,
            "upsert_faq": upsert_faq,
            "delete_faq": delete_faq,
            "set_daily_stats": set_daily_stats,
            "is_daily_stats_enabled": is_daily_stats_enabled,
            "get_group_stats": get_group_stats,
            "get_top_responders": get_top_responders,
            "get_agg_rows": get_agg_rows,
            "set_mood_question": set_mood_question,
            "get_mood_question": get_mood_question,
            "set_mood_topic": set_mood_topic,
            "get_mood_topic": get_mood_topic,
            "set_group_language": set_group_language,
            "set_night_mode": set_night_mode,
            "add_topic_router_rule": add_topic_router_rule,
            "get_effective_link_policy": get_effective_link_policy,
            "get_rss_feeds_full": get_rss_feeds_full,
            "get_subscription_info": get_subscription_info,
            "effective_ai_mod_policy": effective_ai_mod_policy,
            "get_ai_mod_settings": get_ai_mod_settings,
            "set_ai_mod_settings": set_ai_mod_settings,
            "list_faqs": list_faqs,
            "list_topic_router_rules": list_topic_router_rules,
            "get_night_mode": get_night_mode,
            "set_pro_until": set_pro_until,
            "get_global_config": get_global_config,
            "set_global_config": set_global_config,
            "list_forum_topics": list_forum_topics,
            "count_forum_topics": count_forum_topics,
            "upsert_forum_topic": upsert_forum_topic,
            "list_user_topics": list_user_topics,
            "sync_user_topic_names": sync_user_topic_names,
        }
    except ImportError as e:
        logger.error(f"Database import failed: {e}")
        # Dummy-Funktionen als Fallback
        def dummy(*args, **kwargs):
            return None
        return {name: dummy for name in [
            'get_registered_groups', 'set_welcome', 'delete_welcome', 'get_welcome']
            # ... alle anderen benötigten Funktionen ...
        }

# === Helpers =================================================================
async def _is_admin_or_owner(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        cm = await context.bot.get_chat_member(chat_id, user_id)
        status = (getattr(cm, "status", "") or "").lower()
        return status in ("administrator", "creator")
    except Exception as e:
        logger.debug(f"[miniapp] get_chat_member({chat_id},{user_id}) failed: {e}")
        return False

async def _is_admin(app_or_webapp, cid: int, uid: int) -> bool:
    """Prüft Adminrechte über *alle* bekannten PTB-Apps, mit Access.is_admin_or_owner()."""
    apps: list[Application] = []
    try:
        if isinstance(app_or_webapp, Application):
            apps = [app_or_webapp]
        else:
            apps = list(app_or_webapp.get("_ptb_apps", []))
            if not apps and "ptb_app" in app_or_webapp:
                apps = [app_or_webapp["ptb_app"]]
    except Exception:
        apps = []

    for a in apps:
        try:
            is_admin, is_owner = await is_admin_or_owner(a.bot, cid, uid)
            if is_admin or is_owner:
                return True
        except Exception:
            continue
    return False

def _webapp_url(cid: int, title: Optional[str]) -> str:
    url = f"{MINIAPP_URL}?cid={cid}&title={urllib.parse.quote(title or str(cid))}"
    if MINIAPP_API_BASE:
        url += f"&api={urllib.parse.quote(MINIAPP_API_BASE)}"
    return url

def _hm_to_min(hhmm: str, default_min: int) -> int:
    try:
        hh, mm = (hhmm or "").split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return default_min

async def _file_proxy(request):
    app = request.app["ptb_app"]
    if request.method == "OPTIONS":
        return _cors_json({})
    try:
        cid = int(request.query.get("cid","0"))
        uid = int(request.query.get("uid","0"))
        fid = (request.query.get("file_id") or "").strip()
    except Exception:
        return _cors_json({"error":"bad_params"}, 400)
    if not fid:
        return _cors_json({"error":"missing_file_id"}, 400)
    if not await _is_admin(app, cid, uid):
        return _cors_json({"error":"forbidden"}, 403)
    try:
        # irgendeine App darf die Datei holen – wir nehmen die erste verfügbare
        apps = request.app.get("_ptb_apps", [])
        if not apps:
            return _cors_json({"error":"no_bot"}, 500)
        f = await apps[0].bot.get_file(fid)
        buf = BytesIO()
        await f.download_to_memory(out=buf)
        buf.seek(0)
        return Response(body=buf.read(), headers={"Access-Control-Allow-Origin": ALLOWED_ORIGIN},
                        content_type="application/octet-stream")
    except Exception:
        return _cors_json({"error":"not_found"}, 404)

# --- kleine Helferblöcke (DB-Aufrufe sauber gekapselt) -----------------------
def _media_block_with_image(cid, kind):
    db = _db()
    ph = db["get_photo_id"](cid, kind)
    tx = db["get_text"](cid, kind)
    image_url = (f"/miniapp/file?file_id={ph}" if ph else None)
    enabled = bool(tx) or bool(ph)
    return {
        "on": enabled,
        "enabled": enabled,
        "text": tx or "",
        "photo": bool(ph),
        "photo_id": ph or "",
        "image_url": image_url,
    }

async def _state_json(cid: int) -> dict:
    db = _db()
    # Links/Spam
    try:
        eff = db["get_effective_link_policy"](cid, None) or {}
    except Exception:
        eff = {}
    try:
        link = db["get_link_settings"](cid) or {}
    except Exception:
        link = {}

    # Spam-Policy (global) + Topic-Overrides + Topic/User-Mapping
    try:
        sp_global = db["get_spam_policy"](cid) or {}
    except Exception:
        sp_global = {"level": "off", "user_whitelist": []}

    try:
        topics_rows = db["list_forum_topics"](cid, limit=200, offset=0) or []
        topics = [{"id": int(tid), "name": str(name or f"Topic {tid}")} for (tid, name, _ls) in topics_rows]
    except Exception:
        topics = []

    try:
        topic_policies = db["list_spam_policy_topics"](cid) or []
    except Exception:
        topic_policies = []

    try:
        topic_users = db["list_user_topics"](cid) or []
    except Exception:
        topic_users = []
    
    # RSS voll
    feeds = []
    try:
        for (c,url,topic,etag,lm,post_images,enabled) in db["get_rss_feeds_full"]():
            if c == cid:
                feeds.append({"url":url, "topic":topic, "post_images":bool(post_images), "enabled":bool(enabled)})
    except Exception:
        pass

    # Subscription
    try:
        sub = db["get_subscription_info"](cid) or {}
    except Exception:
        sub = {"tier":"free","active":False,"valid_until":None}

    # AI-Moderation
    try:
        # richtig: topic_id explizit mitgeben
        aimod_eff = db["effective_ai_mod_policy"](cid, None) or {}
        aimod_cfg = db["get_ai_mod_settings"](cid, 0) or {}
        aimod = {**aimod_eff, **aimod_cfg}
    except Exception as e:
        logger.warning("[miniapp] aimod state failed for cid=%s: %s", cid, e)
        aimod = {}

    # FAQs
    try:
        faqs = [{"q": q, "a": a} for (q, a) in (db["list_faqs"](cid) or [])]
    except Exception:
        faqs = []

    # Router
    try:
        rows = db["list_topic_router_rules"](cid) or []
        router_rules = [{
          "rule_id": r[0], "target_topic_id": r[1], "enabled": bool(r[2]),
          "delete_original": bool(r[3]), "warn_user": bool(r[4]),
          "keywords": r[5] or [], "domains": r[6] or []
        } for r in rows]
    except Exception:
        router_rules = []

    # Night mode
    try:
        night_data = db["get_night_mode"](cid)
        enabled, start_m, end_m, del_non_admin, warn_once, tz, hard, override_until, write_lock, lock_message = night_data
        night = {
          "enabled": bool(enabled),
          "on": bool(enabled),
          "start": f"{start_m//60:02d}:{start_m%60:02d}",
          "end":   f"{end_m//60:02d}:{end_m%60:02d}",
          "delete_non_admin_msgs": bool(del_non_admin),
          "warn_once": bool(warn_once),
          "timezone": tz,
          "hard_mode": bool(hard),
          "override_until": override_until.isoformat() if override_until else None,
          "write_lock": bool(write_lock),
          "lock_message": lock_message or "Die Gruppe ist gerade im Nachtmodus. Schreiben ist nicht möglich.",
        }
    except Exception:
        night = {"enabled": False, "start": "22:00", "end":"07:00", "write_lock": False, "lock_message": ""}

    try:
        topic0_policy = db["get_spam_policy_topic"](cid, 0) or {}
    except Exception:
        topic0_policy = {}

    # Sorge dafür, dass die UI immer einen "Topic 0" Datensatz zum Bearbeiten hat
    try:
        has0 = any(int(p.get("topic_id", -1)) == 0 for p in (topic_policies or []))
        if not has0:
            topic_policies = [{
                "topic_id": 0,
                "level": topic0_policy.get("level") or "",
                "link_whitelist": eff.get("whitelist") or [],
                "domain_blacklist": eff.get("blacklist") or [],
                "action_primary": topic0_policy.get("action_primary") or eff.get("action") or "delete",
                "per_user_daily_limit": int(topic0_policy.get("per_user_daily_limit") or 0),
                "quota_notify": topic0_policy.get("quota_notify") or "smart",
            }] + (topic_policies or [])
    except Exception:
        pass

    spam_block = {
        # Globaler Spamfilter (spam_policy)
        "on":           (str(sp_global.get("level") or "off").lower() != "off"),
        "level":        str(sp_global.get("level") or "off").lower(),
        "user_whitelist": sp_global.get("user_whitelist") or [],

        # Linkschutz (group_settings)
        "block_links":  bool(link.get("only_admin_links")),

        # Topic-Override Editor (default: Topic 0)
        "policy_topic": 0,
        "topic_level":  (topic0_policy.get("level") or ""),
        "whitelist":    eff.get("whitelist") or [],
        "blacklist":    eff.get("blacklist") or [],
        "action":       (topic0_policy.get("action_primary") or eff.get("action") or "delete"),
        "per_user_daily_limit": int(topic0_policy.get("per_user_daily_limit") or 0),
        "quota_notify": (topic0_policy.get("quota_notify") or "smart"),

        # Read-only Listen zur Kontrolle
        "topics": topics,
        "topic_policies": topic_policies,
        "topic_users": topic_users,

        # (noch nicht implementiert)
        "block_media":  False,
        "block_invite_links": False
    }

    # Story-Sharing Settings (pro Gruppe)
    try:
        sharing_block = db.get("get_story_settings", lambda *_: None)(cid) or {}
    except Exception:
        sharing_block = {}

    # AI Flags
    try:
        (ai_faq, ai_rss) = db["get_ai_settings"](cid)
    except Exception:
        ai_faq, ai_rss = (False, False)

    # Welcome/Rules/Farewell mit Bild-URL
    def _media_block_with_image(cid, kind):
        """Bildfelder defensiv aufbauen – nie Exceptions werfen."""
        try:
            if kind=="welcome":
                ph, tx = _db()["get_welcome"](cid) or (None, None)
            elif kind=="rules":
                ph, tx = _db()["get_rules"](cid) or (None, None)
            else:
                ph, tx = _db()["get_farewell"](cid) or (None, None)
        except Exception as e:
            logger.warning(f"[miniapp] _media_block DB failed kind={kind}: {e}")
            ph, tx = (None, None)
        try:
            image_url = (f"/miniapp/file?id={ph}" if ph else None)
        except Exception:
            image_url = None
        # Nur ID zurückgeben – URL wird in route_state absolut gesetzt
        return {
            "on": bool(tx) or bool(ph),
            "text": tx or "",
            "photo": bool(ph),
            "photo_id": ph or "",
            "image_url": None
        }

    # Captcha-Block aus DB lesen
    try:
        en, ctype, behavior = db["get_captcha_settings"](cid)
        captcha = {"enabled": bool(en), "type": ctype, "behavior": behavior}
    except Exception:
        captcha = {"enabled": False, "type": "button", "behavior": "kick"}

    # Fix: call the function from db dict, not a string
    stats = db["get_group_stats"](cid, date.today()) if "get_group_stats" in db else {}
    daily_stats = db["is_daily_stats_enabled"](cid)
    
    # Clean-Deleted aus DB lesen
    try:
        cds = db["get_clean_deleted_settings"](cid)
    except Exception:
        cds = {"enabled": False, "hh":3, "mm":0, "weekday": None, "demote": False, "notify": True}
    clean_deleted = {
        "enabled": bool(cds.get("enabled")),
        "time": f"{int(cds.get('hh',3)):02d}:{int(cds.get('mm',0)):02d}",
        "weekday": cds.get("weekday"),
        "demote": bool(cds.get("demote")),
        "notify": bool(cds.get("notify")),
    }
    return {
        "welcome": _media_block_with_image(cid, "welcome"),
        "rules":   _media_block_with_image(cid, "rules"),
        "farewell":_media_block_with_image(cid, "farewell"),
        "captcha": captcha,
        "links":   {"only_admin_links": bool(link.get("only_admin_links"))},
        "spam":    spam_block,
        "sharing": sharing_block,
        "ai":      {"on": bool(ai_faq or ai_rss), "faq": ""},
        "aimod":   aimod,
        "faqs":    faqs,
        "router_rules": router_rules,
        "mood":    {"topic": (db["get_mood_topic"](cid) or 0), "question": db["get_mood_question"](cid)},
        "rss":     {"feeds": feeds},
        "report": {
            "enabled": bool(daily_stats),
            "stats": stats,   # falls du die Statistiken dort anzeigen willst
        },
        "daily_stats": bool(daily_stats),
        "subscription": sub,
        "night":   night,
        "language": db.get("get_group_language", lambda *_: None)(cid),
        "rewards": (
            db.get("get_global_config", lambda *_: None)("rewards")
            or {
                    "enabled": False,
                    "mode": "claim",          # claim = User holt sich die Tokens
                    "token": "EMRD",
                    "network": "TON",
                    "decimals": 9,
                    "rate_answer": 0.02,      # EMRD pro Antwort (Vorschlag)
                    "rate_helpful": 0.10,     # EMRD pro „Helpful“-Vote (Vorschlag)
                    "cap_user": 10,           # max. 10 EMRD/Tag pro User
                    "cap_chat": 150,
                    "min_claim": 20.0,       # ab 20 EMRD claimbar
                    "cooldown_days": 7       # max. 1 Claim pro Woche
                }
        ),
        "clean_deleted": clean_deleted,
        "misc": {
            "patchnotes": {
                "version": __version__,
                "content": PATCH_NOTES
            },
            "manual": {
                "available": True,
                "command": "/help",
                "description": "Öffne das vollständige Benutzerhandbuch"
            }
        }
    }


# === HTTP-Routen (nur lesend, Admin-Gate per Bot) ============================
async def route_state(request: web.Request):
    # Zugriff auf die AIOHTTP-App (enthält _ptb_apps)
    webapp = request.app
    if request.method == "OPTIONS":
        return _cors_json({})
    
    # Debug-Logging hinzufügen
    logger.info(f"Headers: {dict(request.headers)}")
    logger.info(f"Query params: {dict(request.query)}")
    
    try:
        cid = int(request.query.get("cid", "0") or 0)
        uid = _resolve_uid(request)
        logger.info(f"Resolved UID: {uid}, CID: {cid}")

        if uid <= 0:
            logger.warning("Authentication failed: UID <= 0")
            return _cors_json({"error": "auth_required"}, 403)

        if not await _is_admin(webapp, cid, uid):
            logger.warning(f"User {uid} is not admin in {cid}")
            return _cors_json({"error": "forbidden"}, 403)

        data = await _state_json(cid)
        # Bild-URLs absolut machen (GitHub Pages lädt sonst relativ vom falschen Host)
        base = (MINIAPP_API_BASE or f"{request.scheme}://{request.host}").rstrip("/")
        for key in ("welcome", "rules", "farewell"):
            blk = data.get(key) or {}
            fid = blk.get("photo_id")
            blk["image_url"] = f"{base}/miniapp/file?cid={cid}&file_id={fid}" if fid else None
            data[key] = blk
        return _cors_json(data)
    
    except Exception as e:
        logger.exception(f"[miniapp] state failed for cid={locals().get('cid','?')}: {e}")
        return _cors_json({"error":"state_failed", "welcome":{}, "rules":{}, "farewell":{}}, 200)

async def route_stats(request: web.Request):
    webapp = request.app
    if request.method == "OPTIONS":
        return _cors_json({})
    try:
        cid = int(request.query.get("cid", "0") or 0)
        uid = _resolve_uid(request)
        if uid <= 0:
            return _cors_json({"error": "auth_required"}, 403)
        if not await _is_admin(webapp, cid, uid):
            return _cors_json({"error": "forbidden"}, 403)

    except Exception:
        return _cors_json({"error": "bad_params"}, 400)
    if not await _is_admin(webapp, cid, uid):
        return _cors_json({"error": "forbidden"}, 403)

    db = _db()
    days = int(request.query.get("days", "14"))
    d_end = date.today()
    d_start = d_end - timedelta(days=days - 1)

    top_rows = db["get_top_responders"](cid, d_start, d_end, 10) or []
    top = [{"user_id": u, "answers": n, "avg_ms": a} for (u, n, a) in top_rows]

    agg_raw = db["get_agg_rows"](cid, d_start, d_end) or []
    agg = [{"date": str(d), "messages": m, "active": au, "joins": j, "leaves": l, "kicks": k,
            "reply_p90_ms": p90, "spam_actions": spam}
           for (d, m, au, j, l, k, _p50, p90, _arh, _arhp, spam, _night) in agg_raw]

    return _cors_json({
        "daily_stats_enabled": db["is_daily_stats_enabled"](cid),
        "top_responders": top,
        "agg": agg,
    })

async def route_spam_effective(request: web.Request):
    """Test-Endpoint: liefert effektive Link-Policy & Spam-Policy für ein Topic."""
    webapp = request.app
    if request.method == "OPTIONS":
        return _cors_json({})

    try:
        cid = int(request.query.get("cid", "0") or 0)
        topic_raw = request.query.get("topic_id", None)
        topic_id = None
        if topic_raw is not None and str(topic_raw).strip() != "":
            try:
                topic_id = int(str(topic_raw).strip())
            except Exception:
                topic_id = None

        uid = _resolve_uid(request)
        if uid <= 0:
            return _cors_json({"error": "auth_failed"}, 403)
        if cid <= 0:
            return _cors_json({"error": "bad_params"}, 400)
        if not await _is_admin(webapp, cid, uid):
            return _cors_json({"error": "forbidden"}, 403)

        db = _db()
        link_settings = None
        try:
            link_settings = db["get_link_settings"](cid)
        except Exception:
            link_settings = None

        try:
            link_policy = db["get_effective_link_policy"](cid, topic_id)
        except Exception:
            link_policy = {}

        try:
            spam_policy = db["effective_spam_policy"](cid, topic_id, link_settings)
        except Exception:
            spam_policy = {}

        return _cors_json({
            "ok": True,
            "cid": cid,
            "topic_id": topic_id,
            "link_policy": link_policy,
            "spam_policy": spam_policy
        })

    except Exception as e:
        logger.exception("route_spam_effective failed")
        return _cors_json({"error": "server_error", "detail": str(e)}, 500)


async def route_topics_sync(request: web.Request):
    """Forum-Topics aus DB liefern (für Dropdowns) und optional aus Client-Payload upserten.

    Wichtig: Telegram Chat IDs für Gruppen/Supergroups sind negativ → cid != 0 ist gültig.
    """
    webapp = request.app
    if request.method == "OPTIONS":
        return _cors_json({})

    payload = {}
    try:
        if request.can_read_body:
            payload = await request.json()
    except Exception:
        payload = {}

    # cid: bevorzugt aus Query, fallback aus Body
    try:
        cid = int(request.query.get("cid") or payload.get("cid") or payload.get("chat_id") or 0)
    except Exception:
        cid = 0

    if cid == 0:
        return _cors_json({"error": "bad_params"}, 400)

    uid = _resolve_uid(request)
    if uid <= 0:
        return _cors_json({"error": "auth_required"}, 403)
    if not await _is_admin(webapp, cid, uid):
        return _cors_json({"error": "forbidden"}, 403)

    db = _db()

    # Optional: topics aus dem Payload übernehmen (falls Frontend sie liefern kann)
    topics_in = payload.get("topics") if isinstance(payload, dict) else None
    if isinstance(topics_in, list) and db.get("upsert_forum_topic"):
        for t in topics_in:
            if not isinstance(t, dict):
                continue
            try:
                tid = int(t.get("id"))
                name = (t.get("name") or "").strip() or None
                if tid >= 0:
                    db["upsert_forum_topic"](cid, tid, name)
            except Exception:
                continue

    # Namen im Mapping syncen (nice-to-have)
    try:
        if db.get("sync_user_topic_names"):
            db["sync_user_topic_names"](cid)
    except Exception:
        pass

    # Aktuelle Topics aus DB liefern
    try:
        rows = db["list_forum_topics"](cid, limit=200, offset=0) if db.get("list_forum_topics") else []
        topics = [{"id": int(tid), "name": (name or f"Topic {tid}")} for (tid, name, _ls) in (rows or [])]
    except Exception:
        topics = []

    return _cors_json({"ok": True, "cid": cid, "count": len(topics), "topics": topics})
   
async def route_send_mood(request: web.Request):
    webapp = request.app
    if request.method == "OPTIONS":
        return _cors_json({})
    try:
        cid = int(request.query.get("cid", "0") or 0)
        uid = _resolve_uid(request)
        if uid <= 0:
            return _cors_json({"error": "auth_required"}, 403)
        if not await _is_admin(webapp, cid, uid):
            return _cors_json({"error": "forbidden"}, 403)

    except Exception:
        return _cors_json({"error": "bad_params"}, 400)
    if not await _is_admin(webapp, cid, uid):
        return _cors_json({"error": "forbidden"}, 403)

    db = _db()
    question = db["get_mood_question"](cid) or "Wie ist deine Stimmung?"
    topic_id = db["get_mood_topic"](cid) or None

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("👍", callback_data="mood_like"),
        InlineKeyboardButton("👎", callback_data="mood_dislike"),
        InlineKeyboardButton("🤔", callback_data="mood_think"),
    ]])

    try:
        apps = webapp.get("_ptb_apps", []) or [webapp["ptb_app"]]
        await apps[0].bot.send_message(chat_id=cid, text=question, reply_markup=kb,
                                       message_thread_id=topic_id)
        return _cors_json({"ok": True})
    except Exception as e:
        logger.error(f"[miniapp] send_mood failed: {e}")
        return _cors_json({"error":"send_failed"}, 500)

# === Bot-Befehle & WebAppData speichern ======================================
async def miniapp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # nur im Privatchat
    if update.effective_chat and update.effective_chat.type != "private":
        return
    if not update.effective_user or not update.effective_message:
        return

    user = update.effective_user
    msg = update.effective_message
    db = _db()
    
    # Optional: Suchparameter aus Command extrahieren
    search_term = " ".join(context.args).lower() if context.args else ""

    try:
        all_groups: List[Tuple[int, str]] = db["get_registered_groups"]() or []
    except Exception as e:
        logger.warning(f"[miniapp] get_registered_groups failed: {e}")
        all_groups = []

    # Gruppen nach Namen sortieren
    all_groups.sort(key=lambda x: str(x[1] or x[0]).lower())
    
    # Suchfilter anwenden wenn vorhanden
    if search_term:
        all_groups = [g for g in all_groups if search_term in str(g[1] or g[0]).lower()]

    rows: List[List[InlineKeyboardButton]] = []
    admin_groups = []
    
    # Erst Admin-Status für alle Gruppen prüfen
    for cid, title in all_groups:
        try:
            cid = int(cid)
            if await _is_admin_or_owner(context, cid, user.id):
                admin_groups.append((cid, title))
        except Exception:
            continue
    
    # Paginierung implementieren (10 Gruppen pro Seite)
    page = getattr(context.user_data, 'miniapp_page', 0)
    items_per_page = 10
    total_pages = (len(admin_groups) + items_per_page - 1) // items_per_page
    
    if total_pages > 0:
        # Gruppen für aktuelle Seite
        start_idx = page * items_per_page
        end_idx = min(start_idx + items_per_page, len(admin_groups))
        current_groups = admin_groups[start_idx:end_idx]
        
        # Gruppen-Buttons erstellen
        for cid, title in current_groups:
            rows.append([InlineKeyboardButton(
                f"{title or cid} – Mini-App öffnen",
                web_app=WebAppInfo(url=_webapp_url(cid, title))
            )])
        
        # Navigations-Buttons hinzufügen wenn nötig
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Zurück", callback_data="miniapp_prev"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Weiter ➡️", callback_data="miniapp_next"))
        if nav_buttons:
            rows.append(nav_buttons)
            
        # Suchbutton hinzufügen
        rows.append([InlineKeyboardButton("🔍 Gruppe suchen", switch_inline_query_current_chat="")])

    if not rows:
        if search_term:
            return await msg.reply_text(f"❌ Keine passenden Gruppen gefunden für: {search_term}")
        return await msg.reply_text("❌ Du bist in keiner registrierten Gruppe Admin/Owner.")

    await msg.reply_text("Wähle eine Gruppe:", reply_markup=InlineKeyboardMarkup(rows))

async def webapp_data_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not getattr(msg, "web_app_data", None):
        return

    logger.info("[miniapp] web_app_data received: uid=%s, len=%s",
                msg.from_user.id if msg.from_user else None,
                len(msg.web_app_data.data or ""))

    try:
        data = json.loads(update.effective_message.web_app_data.data)
    except Exception:
        return await msg.reply_text("❌ Ungültige Daten von der Mini-App.")

    # cid aus Payload ziehen (wie bei dir bisher)
    cid = None
    try:
        if "cid" in data: cid = int(data.get("cid"))
        elif "context" in data and "cid" in data["context"]: cid = int(data["context"]["cid"])
    except Exception:
        pass
    if not cid:
        return await msg.reply_text("❌ Gruppen-ID fehlt oder ist ungültig.")

    # Admincheck
    if not await _is_admin_or_owner(context, cid, update.effective_user.id):
        return await msg.reply_text("❌ Du bist in dieser Gruppe kein Admin.")

    # Optional: Rewards global speichern (MiniApp-Submit)
    db = _db()
    if isinstance(data.get("rewards"), dict) and "set_global_config" in db:
        db["set_global_config"]("rewards", data["rewards"])
        
    errors = await _save_from_payload(cid, update.effective_user.id, data, context.application)

    if errors:
        return await msg.reply_text("⚠️ Teilweise gespeichert:\n• " + "\n• ".join(errors))
    
    db = _db()

    # Links/Spam
    try:
        sp = data.get("spam") or {}
        admins_only = bool(sp.get("on") or sp.get("block_links") or data.get("admins_only"))
        db["set_link_settings"](cid, admins_only=admins_only)

        t_raw = str(sp.get("policy_topic") or '').strip()
        topic_id = int(t_raw) if t_raw.isdigit() else None

        def _to_list(v):
            if isinstance(v, list): return [str(x).strip() for x in v if str(x).strip()]
            if isinstance(v, str):  return [s.strip() for line in v.splitlines() for s in line.split(',') if s.strip()]
            return []

        fields = {}
        wl=_to_list(sp.get("whitelist","")); bl=_to_list(sp.get("blacklist",""))
        if wl: fields["link_whitelist"]   = wl
        if bl: fields["domain_blacklist"] = bl
        act=(sp.get("action") or '').strip().lower();
        if act in ("delete","warn","mute"): fields["action_primary"] = act
        lim=str(sp.get("per_user_daily_limit") or '').strip();
        if lim.isdigit(): fields["per_user_daily_limit"] = int(lim)
        qn=(sp.get("quota_notify") or '').strip().lower();
        if qn in ("off","smart","always"): fields["quota_notify"]=qn
        if topic_id is not None and fields:
            db["set_spam_policy_topic"](cid, topic_id, **fields)
            
        # --- Topic→User Zuordnung (ohne Überschreiben) ---
        tus = data.get("topic_user_set") or sp.get("topic_user_set")
        if isinstance(tus, dict):
            u = tus.get("user_id")
            t = tus.get("topic_id")
            if str(u).lstrip("-").isdigit() and str(t).lstrip("-").isdigit():
                topic_name = None
                try:
                    rows = db.get("list_forum_topics") and db["list_forum_topics"](cid, limit=200, offset=0) or []
                    for (tid, name, _ls) in rows:
                        if int(tid) == int(t):
                            topic_name = name
                            break
                except Exception:
                    pass

                inserted = False
                try:
                    if "add_user_topic" in db:
                        inserted = bool(db["add_user_topic"](cid, int(u), int(t), topic_name))
                    elif "assign_topic" in db:
                        # Fallback: altes Verhalten (überschreibt)
                        db["assign_topic"](cid, int(u), int(t), topic_name)
                        inserted = True
                except Exception as e:
                    errors.append(f"Topic-User: {e}")

                if inserted is False:
                    errors.append("Topic-User: User hat bereits eine Zuordnung (bitte erst entfernen).")

        tud = data.get("topic_user_del") or sp.get("topic_user_del")
        if isinstance(tud, dict):
            u = tud.get("user_id")
            if str(u).lstrip("-").isdigit():
                try:
                    if "remove_topic" in db:
                        db["remove_topic"](cid, int(u))
                except Exception as e:
                    errors.append(f"Topic-User-Del: {e}")
        
    except Exception as e: errors.append(f"Spam/Links: {e}")

    # RSS: add/del + update (enabled/post_images)
    try:
        r = data.get("rss") or {}
        if (r.get("url") or '').strip():
            url = r.get("url").strip(); topic = int(r.get("topic") or 0)
            try: db["set_rss_topic"](cid, topic)
            except Exception: pass
            db["add_rss_feed"](cid, url, topic)
            db["set_rss_feed_options"](cid, url, post_images=bool(r.get("post_images")), enabled=bool(r.get("enabled", True)))
        upd = data.get("rss_update") or None
        if upd and (upd.get("url") or '').strip():
            url=upd.get("url").strip()
            db["set_rss_feed_options"](cid, url, post_images=upd.get("post_images"), enabled=upd.get("enabled"))
        if data.get("rss_del"): db["remove_rss_feed"](cid, data.get("rss_del"))
    except Exception as e: errors.append(f"RSS: {e}")

    # KI (Assistent/FAQ)
    try:
        ai = data.get("ai") or {}
        faq_on = bool(ai.get("on") or (ai.get("faq") or '').strip())
        db["set_ai_settings"](cid, faq=faq_on, rss=None)
    except Exception as e: errors.append(f"KI: {e}")

    # FAQ
    try:
        faq_add = data.get("faq_add") or None
        if faq_add and (faq_add.get("q") or '').strip():
            db["upsert_faq"](cid, faq_add["q"].strip(), (faq_add.get("a") or '').strip())
        faq_del = data.get("faq_del") or None
        if faq_del and (faq_del.get("q") or '').strip():
            db["delete_faq"](cid, faq_del["q"].strip())
    except Exception as e: errors.append(f"FAQ: {e}")

    # KI‑Moderation (viele Felder erlaubt)
    try:
        aimod = data.get("ai_mod") or {}
        if aimod:
            # Leere Strings zu None konvertieren!
            aimod_clean = _clean_dict_empty_to_none(aimod)
            allowed = {
              "enabled","shadow_mode","action_primary","mute_minutes","warn_text","appeal_url",
              "max_per_min","cooldown_s","exempt_admins","exempt_topic_owner",
              "toxicity","hate","sexual","harassment","selfharm","violence",
              # Aliase → DB‑Spalten
              "tox_thresh","hate_thresh","sex_thresh","harass_thresh","selfharm_thresh","violence_thresh"
            }
            payload={}
            for k in allowed:
                if k in aimod_clean and aimod_clean[k] is not None:
                    payload[k]=aimod_clean[k]
            # Aliase umbenennen
            alias = {
              "toxicity":"tox_thresh","hate":"hate_thresh","sexual":"sex_thresh",
              "harassment":"harass_thresh","selfharm":"selfharm_thresh","violence":"violence_thresh"
            }
            for k,v in list(payload.items()):
              if k in alias: payload[alias[k]]=v; del payload[k]
            db["set_ai_mod_settings"](cid, 0, **payload)
    except Exception as e: errors.append(f"AI‑Mod: {e}")

    # Daily Report
    try:
        if "daily_stats" in data: db["set_daily_stats"](cid, bool(data.get("daily_stats")))
    except Exception as e: errors.append(f"Daily‑Report: {e}")

    # Mood
    try:
        if (data.get("mood") or {}).get("question", "").strip(): db["set_mood_question"](cid, data["mood"]["question"].strip())
        if str((data.get("mood") or {}).get("topic", "")).strip().isdigit(): db["set_mood_topic"](cid, int(str(data["mood"]["topic"]).strip()))
    except Exception as e: errors.append(f"Mood: {e}")

    # Sprache
    try:
        lang=(data.get("language") or '').strip()
        if lang: db["set_group_language"](cid, lang[:5])
    except Exception as e: errors.append(f"Sprache: {e}")

    # Router
    try:
        if "router_add" in data:
            ra = data["router_add"] or {}
            target = int(ra.get("target_topic_id") or 0)
            kw = ra.get("keywords") or []
            dom = ra.get("domains") or []
            del_orig = bool(ra.get("delete_original", True))
            warn_user = bool(ra.get("warn_user", True))
            if target:
                db["add_topic_router_rule"](cid, target, keywords=kw, domains=dom, delete_original=del_orig, warn_user=warn_user)
    except Exception as e: errors.append(f"Router: {e}")

    # Pro kaufen/verlängern
    try:
        months = int(data.get("pro_months") or 0)
        if months>0:
            from datetime import datetime, timedelta
            from zoneinfo import ZoneInfo
            until = datetime.now(ZoneInfo("UTC")) + timedelta(days=30*months)
            db["set_pro_until"](cid, until, tier="pro")
    except Exception as e: errors.append(f"Pro‑Abo: {e}")

    if errors:
        return await msg.reply_text("⚠️ Teilweise gespeichert:\n• " + "\n• ".join(errors))
    return await msg.reply_text("✅ Einstellungen gespeichert.")

async def _cors_ok(request):
    # Einheitliche Antwort für Preflight
    return web.json_response(
        {}, status=204,
        headers={
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-Telegram-Init-Data",
        }
    )

def _attach_http_routes(app: Application) -> bool:
    """Versucht, die HTTP-Routen am PTB-aiohttp-Webserver zu registrieren.
    Funktioniert nur, wenn der Bot in Webhook-Mode läuft (app.webhook_application()).
    """
    try:
        webapp = app.webhook_application()
    except Exception:
        webapp = None

    if not webapp:
        logger.info("[miniapp] webhook_application() noch nicht verfügbar (oder Polling-Mode) – retry folgt")
        return False

    # Doppelte Registrierung vermeiden
    if webapp.get("_miniapp_routes_attached"):
        return True
    try:
        register_miniapp_routes(webapp, app)
    except Exception as e:
        logger.warning(f"[miniapp] register_miniapp_routes fehlgeschlagen: {e}", exc_info=True)
        return False

    webapp["_miniapp_routes_attached"] = True
    logger.info("[miniapp] HTTP-Routen registriert (Miniapp + Story API)")
    return True

def register_miniapp_routes(webapp, app):
    global ALLOWED_ORIGIN
    if not ALLOWED_ORIGIN:
        ALLOWED_ORIGIN = "https://greeny187.github.io"
    webapp["allowed_origin"] = ALLOWED_ORIGIN
    webapp.setdefault("_ptb_apps", []).append(app)
    webapp.setdefault("ptb_app", app)
    webapp.router.add_route("GET", "/miniapp/groups",    route_groups)
    webapp.router.add_route("GET", "/miniapp/state",     route_state)
    webapp.router.add_route("GET", "/miniapp/stats",     route_stats)
    webapp.router.add_route("GET", "/miniapp/file",      route_file)
    webapp.router.add_route("GET", "/miniapp/send_mood", route_send_mood)
    webapp.router.add_route("POST", "/miniapp/apply",    route_apply)
    webapp.router.add_route("GET",  "/miniapp/spam/effective", route_spam_effective)
    webapp.router.add_route("POST", "/miniapp/topics/sync",   route_topics_sync)
    for p in ("/miniapp/groups","/miniapp/state","/miniapp/stats",
              "/miniapp/file","/miniapp/send_mood","/miniapp/topics/sync","/miniapp/apply"):
        webapp.router.add_route("OPTIONS", p, _cors_ok)
    for p in ("/miniapp/spam/effective",):
        webapp.router.add_route("OPTIONS", p, _cors_ok)
        
    # --- Story-Sharing API (Emerald) ---
    try:
        from .story_api import register_story_api
        register_story_api(webapp)
    except Exception as e:  # ✅ Fehlender except Block
        logger.warning(f"[miniapp] Story API registration failed: {e}")
    webapp["_miniapp_routes_attached"] = True
    logger.info("[miniapp] HTTP-Routen registriert")
    return True

async def miniapp_pagination_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler für die Paginierungs-Callbacks der Gruppen-Liste"""
    query = update.callback_query
    if not query or not query.data:
        return
        
    # Seite aktualisieren
    if query.data == "miniapp_next":
        context.user_data['miniapp_page'] = context.user_data.get('miniapp_page', 0) + 1
    elif query.data == "miniapp_prev":
        context.user_data['miniapp_page'] = max(0, context.user_data.get('miniapp_page', 0) - 1)
    
    # Neue Liste anzeigen
    await miniapp_cmd(update, context)
    await query.answer()

def register_miniapp(app: Application):
    # Bot-Token dynamisch sammeln (für die Init-Data-Verifikation)
    try:
        tok = getattr(app.bot, "token", None)
        if tok:
            _ALL_TOKENS.add(tok)
    except Exception:
        pass
    
    # 1) Handler wie gehabt
    app.add_handler(CommandHandler("miniapp", miniapp_cmd, filters=filters.ChatType.PRIVATE))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE, webapp_data_handler, block=False), group=-4)
    
    # 2) Neuer Handler für Paginierung
    app.add_handler(CallbackQueryHandler(miniapp_pagination_handler, pattern="^miniapp_"))

    # 3) HTTP-Routen (aiohttp) anhängen – wichtig für Miniapp + Story API
    # Hinweis: klappt nur im Webhook-Mode. Im Polling-Mode sind die Routes nicht verfügbar.
    try:
        ok = _attach_http_routes(app)
        if not ok and getattr(app, "job_queue", None):
            async def _retry_attach(ctx):
                try:
                    if _attach_http_routes(app):
                        ctx.job.schedule_removal()
                except Exception:
                    pass
            app.job_queue.run_repeating(_retry_attach, interval=3, first=1, name="miniapp_attach_routes")
    except Exception as e:
        logger.warning(f"[miniapp] Route-Attach Setup fehlgeschlagen: {e}")