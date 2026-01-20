import json, hashlib, hmac, time, numpy as np
from typing import Any, Dict
from aiohttp import web
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes

from .config import BOT_TOKEN, APP_BASE_URL, ALLOWED_PROVIDERS, SECRET_KEY, TELEGRAM_LOGIN_TTL_SECONDS, PRO_DEFAULT, PRO_USERS
from .database import execute, fetch, fetchrow, init_all_schemas
from .crypto_utils import encrypt_blob, decrypt_blob
from .providers.base import ProviderCredentials
from .providers.kraken import KrakenProvider
from .providers.coinbase import CoinbaseProvider
from .providers.mexc import MexcProvider
from .providers.base import ProviderBase
from .ml.xgb_signals import score_signal
from .risk.atr import atr, position_size
from .sentiment.finbert import analyze as finbert_analyze
from .portfolio.optimizer import optimize as portfolio_opt
from .proof.onchain import ensure_table as ensure_proof_table, record_proof, list_proofs

PROVIDER_MAP = {
    "kraken": KrakenProvider,
    "coinbase": CoinbaseProvider,
    "mexc": MexcProvider,
}

# Optional: wenn du hier weitere Tabellen brauchst, definiere sie sauber:
INIT_SQL = ""

def init_schema():
    # Zentral: alles über database.py, damit einheitliche Schemas existieren
    init_all_schemas()
    # Zusätzliche Schema-Statements (falls du später welche brauchst)
    for stmt in [s.strip() for s in INIT_SQL.split(";") if s.strip()]:
        execute(stmt + ";")
    # Proof-Table aus proof/onchain.py
    ensure_proof_table()

# ----------------- "Best Management" helpers -----------------
_RL = {}  # telegram_id -> (ts_window_start, count)
def _rate_limit(tid: int, key: str, limit: int = 30, window_s: int = 60) -> bool:
    # simple in-memory limiter (Heroku dyno-local, reicht für Beta)
    now = int(time.time())
    k = f"{tid}:{key}"
    ts, cnt = _RL.get(k, (now, 0))
    if now - ts >= window_s:
        _RL[k] = (now, 1)
        return True
    if cnt >= limit:
        return False
    _RL[k] = (ts, cnt + 1)
    return True

_CACHE = {}  # key -> (exp_ts, payload)
def _cache_get(k: str):
    v = _CACHE.get(k)
    if not v: return None
    exp, payload = v
    if time.time() > exp:
        _CACHE.pop(k, None)
        return None
    return payload

def _cache_set(k: str, payload, ttl: int = 10):
    _CACHE[k] = (time.time() + ttl, payload)

def verify_webapp_initdata(init_data: Dict[str, Any]) -> Dict[str, Any]:
    if not BOT_TOKEN:
        raise ValueError("TRADE_API_BOT_TOKEN env fehlt")
    if "hash" not in init_data:
        raise ValueError("missing hash")
    pairs = []
    for k in sorted(init_data.keys()):
        if k == "hash": continue
        v = init_data[k]
        if isinstance(v, dict):
            v = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
        pairs.append(f"{k}={v}")
    data_check = "\n".join(pairs)
    secret = hashlib.sha256(BOT_TOKEN.encode()).digest()
    calc = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
    if calc != init_data["hash"]:
        raise ValueError("bad hash")
    auth_date = int(init_data.get("auth_date", "0"))
    if auth_date and (time.time() - auth_date) > TELEGRAM_LOGIN_TTL_SECONDS:
        raise ValueError("login expired")
    user = init_data.get("user")
    if not user or not user.get("id"):
        raise ValueError("no user in initData")
    return {"telegram_id": int(user["id"]), "username": user.get("username")}

def user_is_pro(telegram_id: int) -> bool:
    if telegram_id in PRO_USERS: return True
    return PRO_DEFAULT

async def _json(data: Any, status: int = 200):
    return web.json_response(data, status=status)

# ---------- Basic API ----------
async def tradeapi_auth(request: web.Request):
    payload = await request.json()
    try:
        u = verify_webapp_initdata(payload.get("initData") or payload)
    except Exception as e:
        return await _json({"error": str(e)}, 400)
    return await _json({"ok": True, "telegram_id": u["telegram_id"], "username": u.get("username")})

async def providers(request: web.Request):
    cached = _cache_get("providers")
    if cached: return await _json(cached)
    payload = {"providers": ALLOWED_PROVIDERS}
    _cache_set("providers", payload, ttl=60)
    return await _json(payload)

# ---------- Dashboard / Portfolio / Alerts / Settings (MiniApp-first) ----------
async def dashboard(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error":"telegram_id required"}, 400)
    if not _rate_limit(tid, "dashboard", limit=60, window_s=60):
        return await _json({"error":"rate_limited"}, 429)

    # Portfolio
    p = fetchrow("select id, coalesce(name,'') as name, coalesce(total_value,0) as total_value, coalesce(cash,0) as cash from tradeapi_portfolios where user_id=%s", (tid,))
    portfolio_count = 1 if p else 0
    total_value = float(p["total_value"]) if p else 0.0

    # Alerts
    a = fetch("select id from tradeapi_alerts where user_id=%s and triggered_at is null", (tid,))
    active_alerts = len(a)

    # Recent signals
    s = fetch("select symbol, signal_type, confidence, created_at from tradeapi_signals where user_id=%s order by id desc limit 10", (tid,))
    recent = [{
        "symbol": r["symbol"],
        "signal_type": r["signal_type"],
        "confidence": float(r["confidence"]) if r["confidence"] is not None else 0.0,
        "created_at": str(r["created_at"]),
    } for r in s]

    return await _json({"dashboard": {
        "portfolio_count": portfolio_count,
        "active_alerts": active_alerts,
        "total_portfolio_value": total_value,
        "recent_signals": recent
    }})

async def portfolios_get(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error":"telegram_id required"}, 400)
    rows = fetch("select id, coalesce(name,'') as name, coalesce(total_value,0) as total_value, coalesce(cash,0) as cash, created_at, updated_at from tradeapi_portfolios where user_id=%s", (tid,))
    return await _json({"portfolios": rows})

async def portfolios_post(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    if not _rate_limit(tid, "portfolios_post", limit=10, window_s=60):
        return await _json({"error":"rate_limited"}, 429)
    name = (body.get("name") or "").strip()
    if not name: return await _json({"error":"name required"}, 400)
    desc = (body.get("description") or "").strip()
    risk = (body.get("risk_level") or "medium").strip()
    cash = float(body.get("initial_cash") or 0.0)

    # user_id UNIQUE -> upsert (1 portfolio in Beta)
    execute(
        "insert into tradeapi_portfolios(user_id, name, total_value, cash) values (%s,%s,%s,%s) "
        "on conflict (user_id) do update set name=excluded.name, cash=excluded.cash, updated_at=current_timestamp",
        (tid, name, cash, cash)
    )
    return await _json({"ok": True})

async def alerts_get(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error":"telegram_id required"}, 400)
    active_only = (request.query.get("active_only") or "").lower() in ("1","true","yes","y")
    if active_only:
        rows = fetch("select id, symbol, alert_type, target_price, created_at from tradeapi_alerts where user_id=%s and triggered_at is null order by id desc", (tid,))
    else:
        rows = fetch("select id, symbol, alert_type, target_price, created_at, triggered_at from tradeapi_alerts where user_id=%s order by id desc", (tid,))
    return await _json({"alerts": rows})

async def alerts_post(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    if not _rate_limit(tid, "alerts_post", limit=20, window_s=60):
        return await _json({"error":"rate_limited"}, 429)
    symbol = (body.get("symbol") or "").strip().upper()
    alert_type = (body.get("alert_type") or "price").strip().lower()
    comparison = (body.get("comparison") or "above").strip().lower()
    target_price = float(body.get("target_price") or 0.0)
    if not symbol or target_price <= 0:
        return await _json({"error":"symbol and target_price required"}, 400)
    # comparison currently stored implicitly by alert_type string, keep minimal:
    execute(
        "insert into tradeapi_alerts(user_id, symbol, alert_type, target_price) values (%s,%s,%s,%s)",
        (tid, symbol, f"{alert_type}:{comparison}", target_price)
    )
    return await _json({"ok": True})

async def alerts_delete(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    alert_id = int(body.get("alert_id") or 0)
    if not alert_id:
        return await _json({"error":"alert_id required"}, 400)
    execute("delete from tradeapi_alerts where id=%s and user_id=%s", (alert_id, tid))
    return await _json({"ok": True})

async def settings_post(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    theme = (body.get("theme") or "dark").strip()
    language = (body.get("language") or "de").strip()
    notif = bool(body.get("notifications_enabled", True))
    thr = float(body.get("alert_threshold_usd") or 100.0)
    cur = (body.get("preferred_currency") or "USD").strip()
    execute(
        "insert into tradeapi_settings(user_id, theme, language, notifications_enabled, alert_threshold_usd, preferred_currency, updated_at) "
        "values (%s,%s,%s,%s,%s,%s,now()) "
        "on conflict (user_id) do update set theme=excluded.theme, language=excluded.language, "
        "notifications_enabled=excluded.notifications_enabled, alert_threshold_usd=excluded.alert_threshold_usd, "
        "preferred_currency=excluded.preferred_currency, updated_at=now()",
        (tid, theme, language, notif, thr, cur)
    )
    return await _json({"ok": True})

async def keys_list(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error":"telegram_id required"}, 400)
    rows = fetch("select id, provider, coalesce(label,'') as label, created_at, updated_at from tradeapi_keys where telegram_id=%s order by provider, label", (tid,))
    return await _json({"items": rows})

async def keys_upsert(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    provider = (body.get("provider") or "").strip()
    if not provider: return await _json({"error":"provider required"}, 400)
    if provider not in [p["id"] for p in ALLOWED_PROVIDERS]:
        return await _json({"error":"provider not allowed"}, 400)
    label = (body.get("label") or "").strip() or None
    fields = {
        "api_key":     (body.get("api_key") or "").strip(),
        "api_secret":  (body.get("api_secret") or "").strip(),
        "passphrase":  (body.get("passphrase") or "").strip(),
        "extras":      body.get("extras") or {},
    }
    if not fields["api_key"] or not fields["api_secret"]:
        return await _json({"error":"api_key and api_secret required"}, 400)
    # Free vs Pro: wenn user nicht Pro und bereits >0 Keys vorhanden, verweigern
    rows = fetch("select count(*) as n from tradeapi_keys where telegram_id=%s", (tid,))
    count = int(rows[0]["n"]) if rows else 0
    if count >= 1 and not user_is_pro(tid):
        return await _json({"error":"Mehrere APIs sind nur in der Pro-Version erlaubt."}, 402)
    blob = encrypt_blob(SECRET_KEY, fields)
    execute(            "insert into tradeapi_keys(telegram_id, provider, label, api_fields_enc) values (%s,%s,%s,%s) "
        "on conflict (telegram_id, provider, coalesce(label,'')) do update set api_fields_enc=excluded.api_fields_enc, updated_at=now()",
        (tid, provider, label, blob)
    )
    return await _json({"ok": True})

async def keys_delete(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    kid = int(body.get("id") or 0)
    if not kid: return await _json({"error":"id required"}, 400)
    execute("delete from tradeapi_keys where id=%s and telegram_id=%s", (kid, tid))
    return await _json({"ok": True})


async def keys_verify(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    provider = (body.get("provider") or "").strip()
    if not provider: return await _json({"error":"provider required"}, 400)
    row = fetchrow("select api_fields_enc from tradeapi_keys where telegram_id=%s and provider=%s order by updated_at desc limit 1", (tid, provider))
    if not row: return await _json({"error":"keine Credentials gefunden"}, 404)
    fields = decrypt_blob(SECRET_KEY, row["api_fields_enc"])
    creds = ProviderCredentials(fields.get("api_key"), fields.get("api_secret"), fields.get("passphrase") or None, fields.get("extras") or {})
    Prov = PROVIDER_MAP.get(provider)
    if not Prov: return await _json({"error":"provider not implemented"}, 400)
    p = Prov(creds)
    try:
        bals = await p.balances()
        return await _json({"ok": True, "balances": bals})
    except Exception as e:
        # Wichtig: keine Secrets leaken, Fehler nur kurz
        msg = str(e)
        if "Incorrect padding" in msg or "binascii.Error" in msg:
            msg = "Secret-Format ungültig (Base64). Bitte Secret exakt aus der Börse kopieren."
        return await _json({"ok": False, "error": msg})

async def keys_ping(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    provider = (body.get("provider") or "").strip()
    if not provider: return await _json({"error":"provider required"}, 400)
    row = fetchrow("select api_fields_enc from tradeapi_keys where telegram_id=%s and provider=%s order by updated_at desc limit 1", (tid, provider))
    if not row: return await _json({"error":"keine Credentials gefunden"}, 404)
    fields = decrypt_blob(SECRET_KEY, row["api_fields_enc"])
    creds = ProviderCredentials(fields.get("api_key"), fields.get("api_secret"), fields.get("passphrase") or None, fields.get("extras") or {})
    Prov = PROVIDER_MAP.get(provider)
    if not Prov: return await _json({"error":"provider not implemented"}, 400)
    p = Prov(creds)
    ok = await p.ping()
    return await _json({"ok": bool(ok)})

# ---------- Signals + Risk + Proof ----------
async def signal_generate(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    tid = u["telegram_id"]
    if not _rate_limit(tid, "signal_generate", limit=20, window_s=60):
        return await _json({"error":"rate_limited"}, 429)
    provider = (body.get("provider") or "").strip()
    symbol = (body.get("symbol") or "BTCUSDT").upper()
    ohlcv = np.array(body.get("ohlcv") or [], dtype=float)
    if ohlcv.shape[1] if ohlcv.size else 0 != 5:
        return await _json({"error":"ohlcv must be Nx5 [O,H,L,C,V]"}, 400)
    sig = score_signal(ohlcv)
    high, low, close = ohlcv[:,1], ohlcv[:,2], ohlcv[:,3]
    atr_val = float(atr(high, low, close))
    # simplistic balance assumption 1000 USD if no API or balance endpoint wired
    bal = float(body.get("balance_usd") or 1000.0)
    entry = float(close[-1])
    size = position_size(bal, entry, atr_val)
    payload = {"signal": sig, "atr": atr_val, "pos_size": size, "entry": entry, "symbol": symbol, "provider": provider}
    proof = record_proof(tid, provider or "na", symbol, payload)
    
    # Persist minimal signal into tradeapi_signals for dashboard
    try:
        execute(
            "insert into tradeapi_signals(user_id, symbol, signal_type, confidence) values (%s,%s,%s,%s)",
            (tid, symbol, sig.get("signal"), float(sig.get("score", 0.0)))
        )
    except Exception:
        pass
    
    return await _json({"ok": True, "payload": payload, "proof": proof})

async def proof_list(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error":"telegram_id required"}, 400)
    rows = list_proofs(tid, limit=50)
    return await _json({"items": rows})

# ---------- Sentiment + Portfolio ----------
async def sentiment_analyze(request: web.Request):
    body = await request.json()
    try:
        _ = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    texts = body.get("texts") or []
    res = finbert_analyze(texts)
    return await _json({"sentiment": res})

async def portfolio_optimize(request: web.Request):
    body = await request.json()
    try:
        _ = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    weights_hint = body.get("weights") or {}
    sentiment = body.get("sentiment") or None
    res = portfolio_opt(weights_hint, sentiment)
    return await _json({"weights": res})

# ---------- Telegram Commands ----------
async def _start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    url = f"{APP_BASE_URL}/static/apptradeapi.html" if APP_BASE_URL else "https://example.com/static/apptradeapi.html"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(text="🔐 Trading-API MiniApp öffnen", web_app=WebAppInfo(url=url))]])
    await update.message.reply_text("Emerald Trade API Bot — Beta v0.1\n\n• Verknüpfe deine Börsen-APIs\n• Generiere XGB-Signale + ATR\n• Prüfe Proof-Hashes on-chain (DB-Stub)\n• Sentiment & Portfolio-Optimizer", reply_markup=kb)

async def _me(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    rows = fetch("select provider, coalesce(label,'') as label, updated_at from tradeapi_keys where telegram_id=%s order by provider, label",
              (update.effective_user.id,))
    if not rows:
        await update.message.reply_text("Keine API-Keys hinterlegt. Tippe /start und öffne die MiniApp.")
        return
    text = "Deine Verknüpfungen:\n" + "\n".join([f"• {r['provider']}{' – '+r['label'] if r['label'] else ''} (upd {r['updated_at']})" for r in rows])
    await update.message.reply_text(text)

def register(application: Application):
    application.add_handler(CommandHandler("start", _start))
    application.add_handler(CommandHandler("me", _me))

def register_jobs(application: Application):
    pass

def register_miniapp_routes(webapp: web.Application, application: Application):
    init_schema()
    webapp.router.add_post( "/tradeapi/auth",               tradeapi_auth)
    webapp.router.add_get(  "/tradeapi/providers",          providers)
    webapp.router.add_get(  "/tradeapi/keys",               keys_list)
    webapp.router.add_get(  "/tradeapi/dashboard",          dashboard)
    webapp.router.add_get(  "/tradeapi/portfolios",         portfolios_get)
    webapp.router.add_post( "/tradeapi/portfolios",         portfolios_post)
    webapp.router.add_get(  "/tradeapi/alerts",             alerts_get)
    webapp.router.add_post( "/tradeapi/alerts",             alerts_post)
    webapp.router.add_post( "/tradeapi/alerts/delete",      alerts_delete)
    webapp.router.add_post( "/tradeapi/settings",           settings_post)
    webapp.router.add_post( "/tradeapi/keys",               keys_upsert)
    webapp.router.add_post( "/tradeapi/keys/delete",        keys_delete)
    webapp.router.add_post( "/tradeapi/keys/ping",          keys_ping)
    webapp.router.add_post( "/tradeapi/keys/verify",        keys_verify)
    webapp.router.add_post( "/tradeapi/signal/generate",    signal_generate)
    webapp.router.add_post( "/tradeapi/sentiment/analyze",  sentiment_analyze)
    webapp.router.add_post( "/tradeapi/portfolio/optimize", portfolio_optimize)
    webapp.router.add_get(  "/tradeapi/proof/list",         proof_list)
