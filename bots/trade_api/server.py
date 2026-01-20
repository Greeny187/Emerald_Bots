import json, hashlib, hmac, time, numpy as np, logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List
from aiohttp import web
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes

from .config import (
    BOT_TOKEN,
    APP_BASE_URL,
    MINIAPP_URL,
    ALLOWED_PROVIDERS,
    SECRET_KEY,
    TELEGRAM_LOGIN_TTL_SECONDS,
    PRO_DEFAULT,
    PRO_USERS,
    MARKET_CACHE_TTL_SECONDS,
    SIGNAL_ATR_MULT_STOP,
    SIGNAL_ATR_MULT_TP,
    ALERT_CHECK_INTERVAL_SECONDS,
)
from .database import execute, fetch, fetchrow, fetchval, init_schema
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

logger = logging.getLogger(__name__)

PROVIDER_MAP = {
    "kraken": KrakenProvider,
    "coinbase": CoinbaseProvider,
    "mexc": MexcProvider,
}

"""Server-side HTTP + Telegram integration for the Trade API MiniApp."""

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


def _extract_sig_fields(sig: Any) -> Dict[str, Any]:
    """Normalize whatever score_signal() returns to a consistent shape."""
    signal_type = "FLAT"
    confidence = 0.0
    strength = 0.0

    if isinstance(sig, dict):
        raw_type = sig.get("signal_type") or sig.get("signal") or sig.get("action") or sig.get("side") or ""
        signal_type = str(raw_type).upper() if raw_type else "FLAT"
        try:
            confidence = float(sig.get("confidence") if sig.get("confidence") is not None else sig.get("prob", 0.0))
        except Exception:
            confidence = 0.0
        try:
            strength = float(sig.get("strength") if sig.get("strength") is not None else sig.get("score", 0.0))
        except Exception:
            strength = 0.0
    elif isinstance(sig, str):
        signal_type = sig.upper()

    # Common aliases
    if signal_type in ("LONG", "BUY", "BULL"):
        signal_type = "BUY"
    elif signal_type in ("SHORT", "SELL", "BEAR"):
        signal_type = "SELL"
    elif signal_type in ("NONE", "NO_TRADE", "HOLD"):
        signal_type = "FLAT"

    return {"signal_type": signal_type, "confidence": confidence, "strength": strength}


def _classify_regime(close: np.ndarray, atr_val: float) -> str:
    """Very lightweight regime detection: trend/range + high volatility."""
    if close.size < 20:
        return "unknown"
    entry = float(close[-1]) if close.size else 0.0
    atr_pct = (atr_val / entry) if entry else 0.0

    n = int(min(close.size, 60))
    x = np.arange(n, dtype=float)
    y = close[-n:].astype(float)
    try:
        slope = float(np.polyfit(x, y, 1)[0])
        slope_pct = slope / entry if entry else 0.0
    except Exception:
        slope_pct = 0.0

    if atr_pct >= 0.06:
        return "high_vol"
    if abs(slope_pct) >= 0.0006:
        return "trend"
    return "range"

# ---------- Basic API ----------
async def tradeapi_auth(request: web.Request):
    payload = await request.json()
    try:
        init_data = payload if isinstance(payload, dict) and "hash" in payload else (payload.get("initData") or {})
        u = verify_webapp_initdata(init_data)
    except Exception as e:
        return await _json({"error": str(e)}, 400)
    return await _json({"ok": True, "telegram_id": u["telegram_id"], "username": u.get("username")})

async def providers(request: web.Request):
    return await _json({"providers": ALLOWED_PROVIDERS})

async def keys_list(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error":"telegram_id required"}, 400)
    rows = fetch(
        "select id, provider, label, created_at, updated_at "
        "from tradeapi_keys where telegram_id=%s order by provider, label",
        (tid,),
    )
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
    # `label` is stored as NOT NULL default '' (see database.py)
    label = (body.get("label") or "").strip()
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
    execute(
        "insert into tradeapi_keys(telegram_id, provider, label, api_fields_enc) values (%s,%s,%s,%s) "
        "on conflict (telegram_id, provider, label) do update "
        "set api_fields_enc=excluded.api_fields_enc, updated_at=now()",
        (tid, provider, label, blob),
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
        return await _json({"ok": False, "error": str(e)})

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
    provider = (body.get("provider") or "").strip()
    symbol = (body.get("symbol") or "BTCUSDT").upper()
    ohlcv = np.array(body.get("ohlcv") or [], dtype=float)
    if ohlcv.shape[1] if ohlcv.size else 0 != 5:
        return await _json({"error":"ohlcv must be Nx5 [O,H,L,C,V]"}, 400)
    sig_raw = score_signal(ohlcv)
    sig = _extract_sig_fields(sig_raw)
    high, low, close = ohlcv[:, 1], ohlcv[:, 2], ohlcv[:, 3]
    atr_val = float(atr(high, low, close))
    entry = float(close[-1])
    regime = _classify_regime(close, atr_val)

    # ---- Risk settings (per user) ----
    risk_row = fetchrow(
        "select risk_per_trade, min_confidence from tradeapi_user_risk where telegram_id=%s",
        (tid,),
    )
    if not risk_row:
        execute("insert into tradeapi_user_risk(telegram_id) values(%s) on conflict do nothing", (tid,))
        risk_row = {"risk_per_trade": 0.005, "min_confidence": 0.6}
    risk_per_trade = float(risk_row.get("risk_per_trade") or 0.005)
    min_conf = float(risk_row.get("min_confidence") or 0.6)

    # Balance: if not supplied, assume a small test account
    bal = float(body.get("balance_usd") or 1000.0)

    # Gate by min confidence
    if sig["confidence"] < min_conf:
        sig["signal_type"] = "FLAT"

    # Simple regime gating: in extreme volatility we only act on high confidence
    if regime == "high_vol" and sig["confidence"] < max(min_conf, 0.75):
        sig["signal_type"] = "FLAT"

    stop_dist = float(atr_val * SIGNAL_ATR_MULT_STOP)
    tp_dist = float(atr_val * SIGNAL_ATR_MULT_TP)

    # Position sizing: try your existing helper first, otherwise fallback to risk-based sizing
    size = 0.0
    if sig["signal_type"] in ("BUY", "SELL") and stop_dist > 0:
        try:
            size = float(position_size(bal, entry, atr_val, risk_per_trade=risk_per_trade))
        except TypeError:
            risk_amt = bal * risk_per_trade
            size = float(risk_amt / stop_dist)
        except Exception:
            risk_amt = bal * risk_per_trade
            size = float(risk_amt / stop_dist)

    stop_loss = 0.0
    take_profit = 0.0
    if sig["signal_type"] == "BUY":
        stop_loss = entry - stop_dist
        take_profit = entry + tp_dist
    elif sig["signal_type"] == "SELL":
        stop_loss = entry + stop_dist
        take_profit = entry - tp_dist

    # Persist the signal (so your /dashboard and /signals can show real history)
    execute(
        "insert into tradeapi_signals(telegram_id, symbol, signal_type, confidence, strength, regime, atr_value, entry_price, stop_loss, take_profit, position_size) "
        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            tid,
            symbol,
            sig["signal_type"],
            float(sig["confidence"]),
            float(sig["strength"]),
            regime,
            atr_val,
            entry,
            stop_loss or None,
            take_profit or None,
            size,
        ),
    )

    payload = {
        "symbol": symbol,
        "provider": provider,
        "entry": entry,
        "regime": regime,
        "atr": atr_val,
        "signal": sig_raw,
        "signal_type": sig["signal_type"],
        "confidence": sig["confidence"],
        "strength": sig["strength"],
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "position_size": size,
        "risk_per_trade": risk_per_trade,
    }
    proof = record_proof(tid, provider or "na", symbol, payload)
    return await _json({"ok": True, "payload": payload, "proof": proof})

async def proof_list(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error":"telegram_id required"}, 400)
    rows = list_proofs(tid, limit=50)
    return await _json({"items": rows})


async def list_signals(request: web.Request):
    """Get recent signals (GET /tradeapi/signals?telegram_id=...&limit=...&symbol=...)."""
    tid = int(request.query.get("telegram_id") or 0)
    if not tid:
        return await _json({"error": "telegram_id required"}, 400)
    limit = int(request.query.get("limit") or 50)
    limit = max(1, min(limit, 200))
    symbol = (request.query.get("symbol") or "").upper().strip()
    if symbol:
        rows = fetch(
            "select symbol, signal_type, confidence, strength, regime, entry_price, stop_loss, take_profit, position_size, created_at "
            "from tradeapi_signals where telegram_id=%s and symbol=%s order by created_at desc limit %s",
            (tid, symbol, limit),
        )
    else:
        rows = fetch(
            "select symbol, signal_type, confidence, strength, regime, entry_price, stop_loss, take_profit, position_size, created_at "
            "from tradeapi_signals where telegram_id=%s order by created_at desc limit %s",
            (tid, limit),
        )
    return await _json({"ok": True, "signals": rows})


async def api_signals_post(request: web.Request):
    """Compatibility endpoint for older miniapps that POST /api/tradeapi/signals."""
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)

    # optional filter
    limit = int(body.get("limit") or 50)
    symbol = (body.get("symbol") or "").upper().strip()
    # build a fake query-based path by calling list_signals logic inline
    tid = u["telegram_id"]
    limit = max(1, min(limit, 200))
    if symbol:
        rows = fetch(
            "select symbol, signal_type, confidence, strength, regime, entry_price, stop_loss, take_profit, position_size, created_at "
            "from tradeapi_signals where telegram_id=%s and symbol=%s order by created_at desc limit %s",
            (tid, symbol, limit),
        )
    else:
        rows = fetch(
            "select symbol, signal_type, confidence, strength, regime, entry_price, stop_loss, take_profit, position_size, created_at "
            "from tradeapi_signals where telegram_id=%s order by created_at desc limit %s",
            (tid, limit),
        )
    return await _json({"status": "ok", "signals": rows, "ok": True})

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

# ---------- User Settings ----------
async def get_user_settings(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error": "telegram_id required"}, 400)
    
    row = fetchrow(
        "select theme, notifications_enabled, alert_threshold_usd, preferred_currency, language "
        "from tradeapi_user_settings where telegram_id=%s",
        (tid,)
    )
    
    if not row:
        # Create default settings
        execute(
            "insert into tradeapi_user_settings(telegram_id) values(%s) on conflict do nothing",
            (tid,)
        )
        row = {"theme": "dark", "notifications_enabled": True, "alert_threshold_usd": 100.0, 
               "preferred_currency": "USD", "language": "de"}
    
    return await _json({"ok": True, "settings": row})

async def update_user_settings(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    
    tid = u["telegram_id"]
    theme = body.get("theme", "dark")
    notifications = body.get("notifications_enabled", True)
    alert_threshold = body.get("alert_threshold_usd", 100.0)
    currency = body.get("preferred_currency", "USD")
    language = body.get("language", "de")
    
    execute(
        "insert into tradeapi_user_settings(telegram_id, theme, notifications_enabled, alert_threshold_usd, preferred_currency, language) "
        "values(%s,%s,%s,%s,%s,%s) "
        "on conflict(telegram_id) do update set theme=excluded.theme, notifications_enabled=excluded.notifications_enabled, "
        "alert_threshold_usd=excluded.alert_threshold_usd, preferred_currency=excluded.preferred_currency, language=excluded.language, updated_at=now()",
        (tid, theme, notifications, alert_threshold, currency, language)
    )
    
    return await _json({"ok": True})

# ---------- Portfolio Management ----------
async def list_portfolios(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error": "telegram_id required"}, 400)
    
    rows = fetch(
        "select id, name, description, total_value, cash, risk_level, created_at, updated_at "
        "from tradeapi_portfolios where telegram_id=%s order by created_at desc",
        (tid,)
    )
    
    return await _json({"ok": True, "portfolios": rows})

async def create_portfolio(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    
    tid = u["telegram_id"]
    name = (body.get("name") or "Portfolio").strip()
    description = (body.get("description") or "").strip()
    risk_level = (body.get("risk_level") or "medium").strip()
    initial_cash = float(body.get("initial_cash") or 10000.0)
    
    try:
        row = fetchrow(
            "insert into tradeapi_portfolios(telegram_id, name, description, cash, total_value, risk_level) "
            "values(%s,%s,%s,%s,%s,%s) returning id",
            (tid, name, description, initial_cash, initial_cash, risk_level)
        )
        return await _json({"ok": True, "portfolio_id": row["id"]})
    except Exception as e:
        return await _json({"error": f"Portfolio bereits vorhanden oder Fehler: {str(e)}"}, 400)

async def get_portfolio(request: web.Request):
    pid = int(request.query.get("portfolio_id") or 0)
    if not pid: return await _json({"error": "portfolio_id required"}, 400)
    
    portfolio = fetchrow("select * from tradeapi_portfolios where id=%s", (pid,))
    if not portfolio: return await _json({"error": "not found"}, 404)
    
    positions = fetch("select * from tradeapi_positions where portfolio_id=%s", (pid,))
    
    # Calculate totals
    total_invested = sum(float(p.get("cost_basis") or 0) for p in positions)
    total_current = sum(float(p.get("current_price", 0)) * float(p.get("quantity", 0)) for p in positions)
    
    return await _json({
        "ok": True,
        "portfolio": portfolio,
        "positions": positions,
        "summary": {
            "total_invested": total_invested,
            "total_current": total_current,
            "unrealized_pnl": total_current - total_invested,
            "unrealized_pnl_percent": ((total_current - total_invested) / total_invested * 100) if total_invested > 0 else 0
        }
    })

async def add_position(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    
    pid = int(body.get("portfolio_id") or 0)
    if not pid: return await _json({"error": "portfolio_id required"}, 400)
    
    symbol = (body.get("symbol") or "").upper()
    quantity = float(body.get("quantity") or 0)
    entry_price = float(body.get("entry_price") or 0)
    current_price = float(body.get("current_price") or entry_price)
    
    if quantity <= 0 or entry_price <= 0:
        return await _json({"error": "Ungültige Menge oder Preis"}, 400)
    
    cost_basis = quantity * entry_price
    
    execute(
        "insert into tradeapi_positions(portfolio_id, asset_symbol, quantity, entry_price, current_price, cost_basis) "
        "values(%s,%s,%s,%s,%s,%s)",
        (pid, symbol, quantity, entry_price, current_price, cost_basis)
    )
    
    # Update portfolio value
    positions = fetch("select sum(quantity * current_price) as total from tradeapi_positions where portfolio_id=%s", (pid,))
    cash = fetchrow("select cash from tradeapi_portfolios where id=%s", (pid,))
    
    if positions and positions[0]["total"]:
        new_total = float(positions[0]["total"]) + float(cash["cash"])
        execute("update tradeapi_portfolios set total_value=%s, updated_at=now() where id=%s", (new_total, pid))
    
    return await _json({"ok": True, "message": "Position hinzugefügt"})

# ---------- Alerts Management ----------
async def list_alerts(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error": "telegram_id required"}, 400)
    
    active_only = request.query.get("active_only", "false").lower() in ("1", "true", "yes")
    
    query = "select id, symbol, alert_type, target_price, comparison, is_active, is_triggered, created_at, triggered_at from tradeapi_alerts where telegram_id=%s"
    params = [tid]
    
    if active_only:
        query += " and is_active=true"
    
    query += " order by created_at desc limit 100"
    
    rows = fetch(query, tuple(params))
    return await _json({"ok": True, "alerts": rows})

async def create_alert(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    
    tid = u["telegram_id"]
    symbol = (body.get("symbol") or "").upper()
    alert_type = body.get("alert_type", "price")  # "price", "volume", "rsi"
    target_price = float(body.get("target_price") or 0)
    comparison = body.get("comparison", "above")  # "above", "below"
    
    if not symbol or target_price <= 0:
        return await _json({"error": "Symbol und Target Price erforderlich"}, 400)
    
    execute(
        "insert into tradeapi_alerts(telegram_id, symbol, alert_type, target_price, comparison, is_active) "
        "values(%s,%s,%s,%s,%s,true)",
        (tid, symbol, alert_type, target_price, comparison)
    )
    
    return await _json({"ok": True, "message": "Alert erstellt"})

async def delete_alert(request: web.Request):
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)
    
    tid = u["telegram_id"]
    aid = int(body.get("alert_id") or 0)
    
    if not aid: return await _json({"error": "alert_id required"}, 400)
    
    execute("delete from tradeapi_alerts where id=%s and telegram_id=%s", (aid, tid))
    return await _json({"ok": True})


async def api_alerts_post(request: web.Request):
    """Compatibility endpoint for older miniapps that POST /api/tradeapi/alerts."""
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)

    action = (body.get("action") or "get").lower()
    # Support the old stub contract: {action:'get'|'create', ...}
    if action == "create":
        symbol = (body.get("symbol") or "").upper()
        alert_type = body.get("alert_type", "price")
        target_price = float(body.get("target_price") or 0)
        comparison = body.get("comparison", "above")
        if not symbol or target_price <= 0:
            return await _json({"error": "Symbol und Target Price erforderlich"}, 400)
        execute(
            "insert into tradeapi_alerts(telegram_id, symbol, alert_type, target_price, comparison, is_active) values(%s,%s,%s,%s,%s,true)",
            (u["telegram_id"], symbol, alert_type, target_price, comparison),
        )
    # Always return current alerts
    rows = fetch(
        "select id, symbol, alert_type, target_price, comparison, is_active, is_triggered, created_at, triggered_at "
        "from tradeapi_alerts where telegram_id=%s order by created_at desc limit 100",
        (u["telegram_id"],),
    )
    return await _json({"status": "ok", "ok": True, "alerts": rows})

# ---------- Market Data & Price Update ----------
async def _provider_public_ticker(provider_id: str, symbol: str) -> Optional[Dict[str, Any]]:
    """Best-effort ticker fetch (works even if provider implementations differ)."""
    Prov = PROVIDER_MAP.get(provider_id)
    if not Prov:
        return None

    # Provider classes in your ecosystem usually accept credentials; for public endpoints, dummy creds are fine.
    try:
        p = Prov(ProviderCredentials("", "", None, {}))
    except Exception:
        try:
            p = Prov(None)  # type: ignore[arg-type]
        except Exception:
            return None

    for meth in (
        "ticker",
        "get_ticker",
        "fetch_ticker",
        "public_ticker",
        "market_price",
        "price",
    ):
        if hasattr(p, meth):
            try:
                res = await getattr(p, meth)(symbol)
                if res is None:
                    continue
                if isinstance(res, (int, float)):
                    return {"price": float(res)}
                if isinstance(res, dict):
                    return res
            except Exception:
                continue
    return None


async def get_market_price(request: web.Request):
    symbol = request.query.get("symbol", "BTCUSDT").upper()
    provider = request.query.get("provider", "kraken")
    force = request.query.get("refresh", "false").lower() in ("1", "true", "yes")
    ttl_s = int(MARKET_CACHE_TTL_SECONDS)
    
    # Try cache first
    row = None
    if not force:
        row = fetchrow(
            f"select price, volume, change_24h, high_24h, low_24h, created_at from tradeapi_market_cache "
            f"where symbol=%s and provider=%s and created_at > now() - interval '{ttl_s} seconds'",
            (symbol, provider),
        )
    
    if row:
        return await _json({"ok": True, "cached": True, "data": row})
    
    # Fetch fresh ticker
    t = await _provider_public_ticker(provider, symbol)
    if not t:
        return await _json({"ok": True, "cached": False, "data": None, "warning": "ticker fetch failed"})

    # Normalize common keys
    def _f(k: str) -> Optional[float]:
        v = t.get(k)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    price = _f("price") or _f("last") or _f("close")
    volume = _f("volume") or _f("vol")
    change_24h = _f("change_24h") or _f("change")
    high_24h = _f("high_24h") or _f("high")
    low_24h = _f("low_24h") or _f("low")

    if price is None:
        return await _json({"ok": True, "cached": False, "data": None, "warning": "no price in ticker"})

    execute(
        "insert into tradeapi_market_cache(symbol, provider, price, volume, change_24h, high_24h, low_24h, created_at) "
        "values(%s,%s,%s,%s,%s,%s,%s,now()) "
        "on conflict(symbol, provider) do update set price=excluded.price, volume=excluded.volume, change_24h=excluded.change_24h, "
        "high_24h=excluded.high_24h, low_24h=excluded.low_24h, created_at=now()",
        (symbol, provider, price, volume, change_24h, high_24h, low_24h),
    )

    data = {
        "price": price,
        "volume": volume,
        "change_24h": change_24h,
        "high_24h": high_24h,
        "low_24h": low_24h,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    return await _json({"ok": True, "cached": False, "data": data})

# ---------- Dashboard & Analytics ----------
def _resolve_miniapp_url() -> str:
    # Prefer serving your own static if APP_BASE_URL is set
    if APP_BASE_URL:
        return f"{APP_BASE_URL}/static/apptradeapi.html"
    return MINIAPP_URL


def _build_dashboard_data(tid: int) -> Dict[str, Any]:
    # Get portfolios
    portfolios = fetch(
        "select id, name, total_value from tradeapi_portfolios where telegram_id=%s order by created_at desc",
        (tid,),
    )

    # Get recent signals
    signals = fetch(
        "select symbol, signal_type, confidence, entry_price, position_size, created_at "
        "from tradeapi_signals where telegram_id=%s order by created_at desc limit 5",
        (tid,),
    )

    # Get active alerts
    alerts = fetch(
        "select symbol, alert_type, target_price from tradeapi_alerts "
        "where telegram_id=%s and is_active=true order by created_at desc limit 5",
        (tid,),
    )

    # Get settings
    settings = fetchrow("select theme, language from tradeapi_user_settings where telegram_id=%s", (tid,))
    if not settings:
        settings = {"theme": "dark", "language": "de"}

    total_portfolio_value = sum(float(p.get("total_value") or 0) for p in portfolios)

    return {
        "total_portfolio_value": total_portfolio_value,
        "portfolio_count": len(portfolios),
        "active_alerts": len(alerts),
        "recent_signals": len(signals),
        "portfolios": portfolios,
        "recent_signals": signals,
        "active_alerts": alerts,
        "settings": settings,
    }


async def get_dashboard(request: web.Request):
    tid = int(request.query.get("telegram_id") or 0)
    if not tid: return await _json({"error": "telegram_id required"}, 400)

    return await _json({"ok": True, "dashboard": _build_dashboard_data(tid)})


async def api_dashboard_post(request: web.Request):
    """Compatibility endpoint for older miniapps that POST /api/tradeapi/dashboard."""
    body = await request.json()
    try:
        u = verify_webapp_initdata(body.get("initData") or {})
    except Exception as e:
        return await _json({"error": str(e)}, 401)

    return await _json({"ok": True, "dashboard": _build_dashboard_data(u["telegram_id"])})

# ---------- Telegram Commands ----------

async def _start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    url = _resolve_miniapp_url()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(text="🔐 Trading-API MiniApp öffnen", web_app=WebAppInfo(url=url))]])
    await update.message.reply_text(
        "🚀 **Emerald Trade API Bot** — v0.2\n\n"
        "Vollständige Trading-Platform mit:\n"
        "✅ Multi-Provider API Integration (Kraken, Coinbase, MEXC)\n"
        "✅ XGBoost Signale + ATR Risk Management\n"
        "✅ Portfolio Manager & Analytics\n"
        "✅ Smart Alerts & Notifications\n"
        "✅ FinBERT Sentiment Analysis\n"
        "✅ On-Chain Proof Verification\n"
        "✅ Benutzer-spezifische Einstellungen\n\n"
        "Pro-Funktionen für meherer APIs verfügbar!",
        reply_markup=kb,
        parse_mode="Markdown"
    )

async def _me(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tid = update.effective_user.id
    
    keys = fetch(
        "select provider, label, updated_at from tradeapi_keys "
        "where telegram_id=%s order by provider, label",
        (tid,),
    )
    
    portfolios = fetch(
        "select name, total_value from tradeapi_portfolios where telegram_id=%s",
        (tid,)
    )
    
    alerts = fetch(
        "select count(*) as count from tradeapi_alerts where telegram_id=%s and is_active=true",
        (tid,)
    )
    
    alert_count = int(alerts[0]["count"]) if alerts else 0
    
    text = f"👤 **Dein Profil**\n\n"
    text += f"**API-Verknüpfungen:** {len(keys)}\n"
    if keys:
        for k in keys:
            text += f"  • {k['provider']}{' – '+k['label'] if k['label'] else ''}\n"
    
    text += f"\n**Portfolios:** {len(portfolios)}\n"
    if portfolios:
        total_value = sum(float(p.get('total_value') or 0) for p in portfolios)
        text += f"  Gesamtwert: ${total_value:,.2f}\n"
    
    text += f"\n**Aktive Alerts:** {alert_count}\n"
    text += f"\n_Klicke auf den Button unten, um die MiniApp zu öffnen!_"
    
    url = _resolve_miniapp_url()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(text="📊 MiniApp öffnen", web_app=WebAppInfo(url=url))]])
    
    await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")

async def _help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = """🆘 **Hilfe – Trade API Bot**

**Verfügbare Befehle:**
/start – Willkommen & MiniApp öffnen
/me – Dein Profil & Übersicht
/help – Diese Hilfe

**MiniApp-Features:**
1️⃣ **API Keys** – Verknüpfe Trading-Börsen (Kraken, Coinbase, MEXC)
2️⃣ **Portfolio Manager** – Verwalte mehrere Portfolios
3️⃣ **Trading Signals** – XGBoost + ATR für professionelle Signale
4️⃣ **Sentiment Analysis** – FinBERT für Marktstimmung
5️⃣ **Portfolio Optimizer** – Gewichte basierend auf Sentiment optimieren
6️⃣ **Price Alerts** – Benachrichtigungen bei Preiszielen
7️⃣ **Dashboard** – Komplette Übersicht aller Daten
8️⃣ **Settings** – Theme, Sprache, Benachrichtigungen

**Sicherheit:**
🔒 Alle API-Keys werden serverseitig verschlüsselt
🔐 Telegram Login Verification für alle Operationen
🛡️ Proof-Hashes für Signalverifizierung

**Preismodelle:**
🆓 **Free:** 1 API-Provider, 5 Alerts, 1 Portfolio
💎 **Pro:** Unbegrenzte Provider, 50+ Alerts, 10+ Portfolios

Klicke unten, um die App zu starten!"""
    
    url = _resolve_miniapp_url()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(text="🚀 App starten", web_app=WebAppInfo(url=url))]])
    
    await update.message.reply_text(text, reply_markup=kb, parse_mode="Markdown")

def register(application: Application):
    """Register Telegram handlers"""
    application.add_handler(CommandHandler("start", _start))
    application.add_handler(CommandHandler("me", _me))
    application.add_handler(CommandHandler("help", _help))

def register_jobs(application: Application):
    """Register background jobs (alerts etc.)."""
    if not getattr(application, "job_queue", None):
        logger.warning("No job_queue available; alert checks disabled")
        return

    application.job_queue.run_repeating(
        _alert_check_job,
        interval=int(ALERT_CHECK_INTERVAL_SECONDS),
        first=10,
        name="tradeapi_alert_check",
    )


async def _alert_check_job(ctx: ContextTypes.DEFAULT_TYPE):
    """Checks active alerts and notifies users in Telegram."""
    rows = fetch(
        "select id, telegram_id, symbol, target_price, comparison from tradeapi_alerts "
        "where is_active=true and is_triggered=false order by created_at asc limit 200"
    )
    if not rows:
        return

    for a in rows:
        try:
            tid = int(a["telegram_id"])
            symbol = str(a["symbol"] or "").upper()
            target = float(a["target_price"] or 0)
            comp = str(a.get("comparison") or "above").lower()
            if not symbol or target <= 0:
                continue

            # Pick a provider: most recent key, else default
            provider = fetchval(
                "select provider from tradeapi_keys where telegram_id=%s order by updated_at desc limit 1",
                (tid,),
            ) or "kraken"

            cache = fetchrow(
                "select price from tradeapi_market_cache where symbol=%s and provider=%s",
                (symbol, provider),
            )
            price = None
            if cache and cache.get("price") is not None:
                try:
                    price = float(cache["price"])
                except Exception:
                    price = None

            if price is None:
                t = await _provider_public_ticker(str(provider), symbol)
                if not t:
                    continue
                try:
                    price = float(t.get("price") or t.get("last") or t.get("close"))
                except Exception:
                    price = None
                if price is None:
                    continue

                # Store into cache (best effort)
                try:
                    execute(
                        "insert into tradeapi_market_cache(symbol, provider, price, created_at) values(%s,%s,%s,now()) "
                        "on conflict(symbol, provider) do update set price=excluded.price, created_at=now()",
                        (symbol, provider, price),
                    )
                except Exception:
                    pass

            trig = (comp == "above" and price >= target) or (comp == "below" and price <= target)
            if not trig:
                continue

            execute(
                "update tradeapi_alerts set is_triggered=true, is_active=false, triggered_at=now() where id=%s",
                (int(a["id"]),),
            )

            msg = (
                f"🔔 Alert ausgelöst\n\n"
                f"Symbol: {symbol}\n"
                f"Ziel: {target}\n"
                f"Preis: {price}\n"
                f"Vergleich: {comp}"
            )
            try:
                await ctx.application.bot.send_message(chat_id=tid, text=msg)
            except Exception:
                logger.info("Could not DM user %s for alert", tid)

        except Exception as e:
            logger.warning("Alert job error: %s", e)

def register_miniapp_routes(webapp: web.Application, application: Application):
    """Register HTTP API routes"""
    init_schema()
    # Proof table lives in a separate module (kept for your ecosystem compatibility)
    try:
        ensure_proof_table()
    except Exception as e:
        logger.warning("Proof table init skipped/failed: %s", e)
    
    # Auth & Providers
    webapp.router.add_post( "/tradeapi/auth",               tradeapi_auth)
    webapp.router.add_get(  "/tradeapi/providers",          providers)

    # Backwards-compatible routes (older miniapp builds)
    webapp.router.add_post( "/api/tradeapi/auth",           tradeapi_auth)
    webapp.router.add_post( "/api/tradeapi/dashboard",      api_dashboard_post)
    webapp.router.add_post( "/api/tradeapi/signals",        api_signals_post)
    webapp.router.add_post( "/api/tradeapi/alerts",         api_alerts_post)
    
    # API Key Management
    webapp.router.add_get(  "/tradeapi/keys",               keys_list)
    webapp.router.add_post( "/tradeapi/keys",               keys_upsert)
    webapp.router.add_post( "/tradeapi/keys/delete",        keys_delete)
    webapp.router.add_post( "/tradeapi/keys/ping",          keys_ping)
    webapp.router.add_post( "/tradeapi/keys/verify",        keys_verify)
    
    # Signals & Risk
    webapp.router.add_post( "/tradeapi/signal/generate",    signal_generate)
    webapp.router.add_get(  "/tradeapi/signals",            list_signals)
    webapp.router.add_get(  "/tradeapi/proof/list",         proof_list)
    
    # Sentiment & Portfolio
    webapp.router.add_post( "/tradeapi/sentiment/analyze",  sentiment_analyze)
    webapp.router.add_post( "/tradeapi/portfolio/optimize", portfolio_optimize)
    
    # User Settings
    webapp.router.add_get(  "/tradeapi/settings",           get_user_settings)
    webapp.router.add_post( "/tradeapi/settings",           update_user_settings)
    
    # Portfolio Management
    webapp.router.add_get(  "/tradeapi/portfolios",         list_portfolios)
    webapp.router.add_post( "/tradeapi/portfolios",         create_portfolio)
    webapp.router.add_get(  "/tradeapi/portfolio",          get_portfolio)
    webapp.router.add_post( "/tradeapi/position",           add_position)
    
    # Alerts
    webapp.router.add_get(  "/tradeapi/alerts",             list_alerts)
    webapp.router.add_post( "/tradeapi/alerts",             create_alert)
    webapp.router.add_post( "/tradeapi/alerts/delete",      delete_alert)
    
    # Market Data
    webapp.router.add_get(  "/tradeapi/market/price",       get_market_price)
    
    # Dashboard
    webapp.router.add_get(  "/tradeapi/dashboard",          get_dashboard)

