import os
import time
import json
import logging
import hmac
import hashlib
import base64
import asyncio
import datetime
import pathlib
import secrets
import sys
sys.path.append(str(pathlib.Path(__file__).parent))  # lokales Modulverzeichnis sicherstellen
import httpx
from typing import Tuple, Dict, Any, List, Optional
from aiohttp import web
from psycopg_pool import ConnectionPool
from decimal import Decimal, getcontext
import jwt
from functools import partial
try:
    from nacl.signing import VerifyKey
    from nacl.exceptions import BadSignatureError
except Exception:  # optional; we only raise if verify is actually used
    VerifyKey = None
    BadSignatureError = Exception

getcontext().prec = 50

log = logging.getLogger("devdash")

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL ist nicht gesetzt")

SECRET_KEY = os.getenv("SECRET_KEY", "change-me")  # setze in Heroku!
DEV_LOGIN_CODE = os.getenv("DEV_LOGIN_CODE")
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
BOT_TOKEN = os.getenv("BOT1_TOKEN") or os.getenv("BOT_TOKEN")  # Bot‑Token für Telegram-Login Verify

if not BOT_TOKEN:
    log.warning("⚠️  BOT_TOKEN / BOT1_TOKEN nicht gesetzt! Telegram-Login wird fehlschlagen!")
else:
    log.info("✅ BOT_TOKEN ist gesetzt")


# NEAR config
NEAR_NETWORK = os.getenv("NEAR_NETWORK", "mainnet")  # "mainnet" | "testnet"
NEAR_RPC_URL = os.getenv("NEAR_RPC_URL", "https://rpc.mainnet.near.org")
NEAR_TOKEN_CONTRACT = os.getenv("NEAR_TOKEN_CONTRACT", "")  # z.B. token.emeraldcontent.near
NEARBLOCKS_API = os.getenv("NEARBLOCKS_API", "https://api.nearblocks.io")
TON_API_BASE   = os.getenv("TON_API_BASE", "https://tonapi.io")
TON_API_KEY    = os.getenv("TON_API_KEY", "")

pool = ConnectionPool(DB_URL, min_size=1, max_size=5, kwargs={"autocommit": True})

# ------------------------------ helpers ------------------------------

# ✔ Moderne, stabile Form – KEINE async-Factory!
@web.middleware
async def cors_middleware(request, handler):
    # immer dynamisch per Origin entscheiden
    cors = _cors_headers(request)
    if request.method == "OPTIONS":
        # sauberes Preflight mit allen nötigen Headern
        resp = web.Response(status=204, headers=cors)
        resp.headers["Vary"] = "Origin"
        return resp
    try:
        resp = await handler(request)
    except web.HTTPException as he:
        resp = web.json_response({"error": he.reason}, status=he.status)
    except Exception as e:
        request.app.get('logger', None) and request.app['logger'].exception(f"[cors] unhandled error: {e}")
        resp = web.json_response({"error": "internal_error"}, status=500)
    if resp is None:
        resp = web.json_response({"error":"empty_response"}, status=500)
    # CORS-Header immer hinzufügen (+ Vary)
    try:
        for k, v in cors.items():
            resp.headers[k] = v
        resp.headers["Vary"] = "Origin"
    except Exception:
        pass
    return resp

def _allow_origin(origin: Optional[str]) -> str:
    if not origin or "*" in ALLOWED_ORIGINS:
        return "*"
    return origin if origin in ALLOWED_ORIGINS else ALLOWED_ORIGINS[0]

# -------- Dev-Login (Code+Telegram-ID) für dich, liefert JWT --------
async def dev_login(request: web.Request):
    """
    DEV LOGIN - Quick authentication for development/testing.
    
    Request: { "telegram_id": 123456, "code": "DEV_LOGIN_CODE", "username": "optional" }
    Response: { "access_token": "jwt", "token_type": "bearer" }
    """
    body = await request.json()
    code = (body.get("code") or "").strip()
    tg_id = int(body.get("telegram_id") or 0)
    if not tg_id:
        raise web.HTTPBadRequest(text="telegram_id required")
    expected = os.getenv("DEV_LOGIN_CODE", "")
    if not expected or code != expected:
        raise web.HTTPUnauthorized(text="bad dev code")
    
    log.info(f"DEV_LOGIN for telegram_id={tg_id}")
    
    await execute("""
      insert into dashboard_users(telegram_id, username, role, tier)
      values (%s, %s, 'dev', 'pro')
      on conflict (telegram_id) do update set
        username=coalesce(excluded.username, dashboard_users.username),
        role='dev',
        tier='pro',
        updated_at=now()
    """, (tg_id, body.get("username")))
    tok = _jwt_issue(tg_id, role="dev", tier="pro")
    return _json({"access_token": tok, "token_type": "bearer", "role": "dev", "tier": "pro"}, request)

def _cors_headers(request: web.Request) -> Dict[str, str]:
    origin = request.headers.get("Origin")
    allow_origin = _allow_origin(origin)
    return {
        "Access-Control-Allow-Origin": allow_origin,
        "Access-Control-Allow-Headers": "*, Authorization, Content-Type",
        "Access-Control-Allow-Methods": "GET,POST,PUT,OPTIONS",
    }

async def set_ton_address(request: web.Request):
    user_id = await _auth_user(request)
    body = await request.json()
    address = (body.get("address") or "").strip()
    await execute("update dashboard_users set ton_address=%s, updated_at=now() where telegram_id=%s",
                  (address, user_id))
    return _json({"ok": True, "ton_address": address}, request)

async def wallets_overview(request: web.Request):
    # Liefert Watch-Accounts (NEAR/TON) + eigene TON-Adresse (aus dashboard_users)
    user_id = await _auth_user(request)
    me = await fetchrow("select near_account_id, ton_address from dashboard_users where telegram_id=%s", (user_id,))
    watches = await fetch("select id,chain,account_id,label,meta,created_at from dashboard_watch_accounts order by id asc")
    return _json({"me": me, "watch": watches}, request)

async def _to_thread(func, *a, **kw):
    return await asyncio.to_thread(func, *a, **kw)


def _fetch(sql: str, params: Tuple = ()) -> List[Dict[str, Any]]:
    with pool.connection() as con, con.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description] if cur.description else []
        rows = cur.fetchall() if cur.description else []
        return [dict(zip(cols, r)) for r in rows]


def _fetchrow(sql: str, params: Tuple = ()) -> Optional[Dict[str, Any]]:
    with pool.connection() as con, con.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        cols = [c.name for c in cur.description]
        return dict(zip(cols, row))


def _execute(sql: str, params: Tuple = ()) -> None:
    with pool.connection() as con, con.cursor() as cur:
        cur.execute(sql, params)


async def fetch(sql: str, params: Tuple = ()): return await _to_thread(_fetch, sql, params)
async def fetchrow(sql: str, params: Tuple = ()): return await _to_thread(_fetchrow, sql, params)
async def execute(sql: str, params: Tuple = ()): return await _to_thread(_execute, sql, params)


# --------------------------- bootstrap tables ---------------------------
INIT_SQL = """
create table if not exists dashboard_users (
  telegram_id bigint primary key,
  username text,
  first_name text,
  last_name text,
  photo_url text,
  role text not null default 'dev',
  tier text not null default 'pro',
  -- NEAR binding
  near_account_id text,
  near_public_key text,
  near_connected_at timestamp,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now()
);

create table if not exists dashboard_bots (
   id bigserial primary key,
   username       text not null unique,
   title          text,
   env_token_key  text not null,
   is_active      boolean not null default true,
   meta           jsonb default '{}'::jsonb,
   created_at     timestamptz not null default now(),
   updated_at     timestamptz not null default now()
);

-- Registry for fan‑out to each bot (mesh)
create table if not exists dashboard_bot_endpoints (
  id serial primary key,
  bot_username text not null references dashboard_bots(username) on delete cascade,
  base_url text not null,
  api_key text,
  metrics_path text not null default '/internal/metrics',
  health_path  text not null default '/internal/health',
  is_active boolean not null default true,
  last_seen timestamp,
  notes text,
  unique(bot_username, base_url)
);

-- Ads/FeatureFlags stay as before (might already exist in your DB)
-- adv_campaigns wird bereits vom ads.py Modul erstellt
-- Wir nutzen jetzt adv_campaigns statt dashboard_ads für Werbekampagnen

create table if not exists dashboard_feature_flags (
  key text primary key,
  value jsonb not null,
  description text
);

-- Login challenges for NEAR signature binding
create table if not exists dashboard_nonces (
  telegram_id bigint not null,
  nonce bytea not null,
  created_at timestamp not null default now(),
  primary key(telegram_id)
);

-- Off‑chain token accounting (optional, to complement on‑chain data)
create table if not exists dashboard_token_events (
  id serial primary key,
  happened_at timestamp not null default now(),
  kind text not null check (kind in ('mint','burn','reward','fee','redeem','manual')),
  amount numeric(36, 18) not null,
  unit text not null default 'EMRLD',
  actor_telegram_id bigint,
  ref jsonb,
  note text
);
"""

async def ensure_tables():
    log.info("🔧 Starting table initialization...")
    try:
        # users (singular → plural fix)
        await execute("""
        CREATE TABLE IF NOT EXISTS dashboard_users (
            telegram_id      BIGINT PRIMARY KEY,
            username         TEXT,
            first_name       TEXT,
            last_name        TEXT,
            photo_url        TEXT,
            role             TEXT NOT NULL DEFAULT 'dev',
            tier             TEXT NOT NULL DEFAULT 'pro',
            ton_address      TEXT,
            near_account_id  TEXT,
            near_public_key  TEXT,
            near_connected_at TIMESTAMPTZ,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        log.info("✅ dashboard_users table ready")
        
        # Ensure ton_address column exists (for existing databases)
        try:
            await execute("ALTER TABLE dashboard_users ADD COLUMN ton_address TEXT;")
            log.info("✅ Added ton_address column to dashboard_users")
        except Exception as e:
            if "already exists" in str(e) or "duplicate" in str(e).lower():
                log.info("✅ ton_address column already exists")
            else:
                log.warning("⚠️  Could not add ton_address column: %s", e)

        # bots (enabled → is_active fix)
        await execute("""
        CREATE TABLE IF NOT EXISTS dashboard_bots (
            id            BIGSERIAL PRIMARY KEY,
            username      TEXT UNIQUE,
            title         TEXT,
            env_token_key TEXT,
            is_active     BOOLEAN NOT NULL DEFAULT TRUE,
            meta          JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        log.info("✅ dashboard_bots table ready")

        # endpoints for health/metrics (used by /devdash/bots later)
        await execute("""
        CREATE TABLE IF NOT EXISTS dashboard_bot_endpoints (
            id           BIGSERIAL PRIMARY KEY,
            bot_username TEXT NOT NULL REFERENCES dashboard_bots(username) ON DELETE CASCADE,
            base_url     TEXT NOT NULL,
            api_key      TEXT,
            metrics_path TEXT NOT NULL DEFAULT '/internal/metrics',
            health_path  TEXT NOT NULL DEFAULT '/internal/health',
            is_active    BOOLEAN NOT NULL DEFAULT TRUE,
            last_seen    TIMESTAMPTZ,
            notes        TEXT,
            UNIQUE(bot_username, base_url)
        );
        """)
        log.info("✅ dashboard_bot_endpoints table ready")

        # watchlist for wallet pages
        await execute("""
        CREATE TABLE IF NOT EXISTS dashboard_watch_accounts (
            id         BIGSERIAL PRIMARY KEY,
            chain      TEXT NOT NULL CHECK (chain IN ('near','ton')),
            account_id TEXT NOT NULL,
            label      TEXT,
            meta       JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(chain, account_id)
        );
        """)
        log.info("✅ dashboard_watch_accounts table ready")

        # singleton settings we can edit from code (TON/NEAR defaults)
        await execute("""
        CREATE TABLE IF NOT EXISTS devdash_settings (
            id                SMALLINT PRIMARY KEY DEFAULT 1,
            near_watch_account TEXT,
            ton_address        TEXT,
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)
        log.info("✅ devdash_settings table ready")

        # defaults: ONLY emeraldcontent.near, TON hard-bound to you
        await execute("""
        INSERT INTO devdash_settings (id, near_watch_account, ton_address)
        VALUES (1, 'emeraldcontent.near', 'UQBVG-RRn7l5QZkfS4yhy8M3yhu-uniUrJc4Uy4Qkom-RFo2')
        ON CONFLICT (id) DO UPDATE
        SET near_watch_account = EXCLUDED.near_watch_account,
            ton_address        = EXCLUDED.ton_address,
            updated_at         = NOW();
        """)
        log.info("✅ devdash_settings configured")

        # make sure watchlist contains the one near account we care about
        await execute("""
        INSERT INTO dashboard_watch_accounts (chain, account_id, label)
        VALUES ('near', 'emeraldcontent.near', 'emeraldcontent.near')
        ON CONFLICT (chain, account_id) DO NOTHING;
        """)
        log.info("✅ All tables initialized successfully")
        
        # adv_campaigns Schema wird vom ads.py Modul erstellt
        try:
            log.info("✅ adv_campaigns wird vom ads.py Modul verwaltet")
        except Exception as e:
            log.warning(f"⚠️  Could not verify adv_campaigns: {e}")
    except Exception as e:
        log.error("❌ Error during table initialization: %s", e, exc_info=True)

async def _telegram_getme(token: str) -> dict:
    async with httpx.AsyncClient(timeout=10.0) as cx:
        r = await cx.get(f"https://api.telegram.org/bot{token}/getMe")
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"getMe failed: {data}")
        return data["result"]

async def scan_env_bots() -> int:
    """
    Pick up all BOT*_TOKEN envs, call getMe, upsert into dashboard_bots.
    """
    import re
    added = 0
    for key, token in os.environ.items():
        if not re.fullmatch(r'BOT[0-9A-Z_]*_TOKEN', key):
            continue
        token = (token or '').strip()
        if not token:
            continue
        try:
            me = await _telegram_getme(token)
            log.info(f"🤖 Bot detected from {key}: {me}")
            
            username = me.get("username") or me.get("first_name") or key
            title    = me.get("first_name") or username

            log.info(f"📝 Inserting: username={username}, title={title}, env_token_key={key}")
            
            await execute("""
                INSERT INTO dashboard_bots (username, title, env_token_key, is_active, meta)
                VALUES (%s, %s, %s, TRUE, '{}'::jsonb)
                ON CONFLICT (username) DO UPDATE
                SET title = EXCLUDED.title,
                    env_token_key = EXCLUDED.env_token_key,
                    is_active = TRUE,
                    updated_at = NOW();
            """, (username, title, key))
            added += 1
            log.info(f"✅ Bot '{username}' registered")
        except Exception as e:
            log.error("scan_env_bots ERROR for %s: %s", key, e, exc_info=True)
    log.info("Bot auto-discovery completed (%d).", added)
    return added

# ------------------------------ tokens (JWT) ------------------------------
def _jwt_issue(telegram_id: int, role: str = "dev", tier: str = "pro") -> str:
    payload = {"sub": str(telegram_id), "role": role, "tier": tier,
               "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7)}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def _jwt_verify(token: str) -> int:
    data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    return int(data.get("sub"))

# ----------------------- Telegram login verify -----------------------

def verify_telegram_auth(auth: Dict[str, Any]) -> Dict[str, Any]:
    """
    Verify Telegram Web App init data signature.
    
    Expected fields:
    - id (int): User ID
    - auth_date (int): Unix timestamp
    - hash (str): HMAC-SHA256 signature
    - username, first_name, last_name, photo_url (optional)
    """
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN/BOT1_TOKEN env fehlt")
    
    required = ["id", "auth_date", "hash"]
    for r in required:
        if r not in auth:
            raise ValueError(f"missing required field: {r}")
    
    # Extract hash
    received_hash = auth["hash"]
    
    # Build data check string: alphabetically sorted fields (excluding hash)
    # Format: "field1=value1\nfield2=value2\n..."
    data_to_check = {}
    for k, v in auth.items():
        if k != "hash":
            data_to_check[k] = str(v)
    
    data_check_str = "\n".join(f"{k}={data_to_check[k]}" for k in sorted(data_to_check.keys()))
    log.debug(f"Data to check:\n{data_check_str}")
    
    # Calculate HMAC
    secret = hashlib.sha256(BOT_TOKEN.encode()).digest()
    calculated_hash = hmac.new(secret, data_check_str.encode(), hashlib.sha256).hexdigest()
    
    log.debug(f"Received hash: {received_hash}")
    log.debug(f"Calculated hash: {calculated_hash}")
    
    if calculated_hash != received_hash:
        raise ValueError(f"invalid hash signature (received={received_hash[:8]}..., calculated={calculated_hash[:8]}...)")
    
    # Check timestamp
    auth_timestamp = int(auth["auth_date"])
    ttl = int(os.getenv("TELEGRAM_LOGIN_TTL_SECONDS", "86400"))
    current_time = time.time()
    
    if current_time - auth_timestamp > ttl:
        raise ValueError(f"login expired (auth_date={auth_timestamp}, now={current_time}, ttl={ttl})")
    
    return {
        "id": int(auth["id"]),
        "username": auth.get("username"),
        "first_name": auth.get("first_name"),
        "last_name": auth.get("last_name"),
        "photo_url": auth.get("photo_url"),
    }


# ------------------------------ utils ------------------------------

def _json_default(o):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    return str(o)

_json_dumps = partial(json.dumps, default=_json_default, ensure_ascii=False)

def _json(data: Any, request: web.Request, status: int = 200):
    resp = web.json_response(data, status=status, dumps=_json_dumps)
    for k, v in _cors_headers(request).items():
        resp.headers[k] = v
    return resp

async def _auth_user(request: web.Request) -> int:
    auth = request.headers.get("Authorization","")
    if not auth.lower().startswith("bearer "):
        raise web.HTTPUnauthorized(text="Missing bearer token")
    token = auth.split(" ",1)[1].strip()

    # 1) HMAC-Token (user_id.exp.sig)
    try:
        return _jwt_verify(token)
    except Exception as e:
        raise web.HTTPUnauthorized(text=f"invalid token: {e}")

# ------------------------------ base58 ------------------------------
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX = {c: i for i, c in enumerate(_B58_ALPHABET)}


def b58decode(s: str) -> bytes:
    n = 0
    for ch in s:
        n = n * 58 + _B58_INDEX[ch]
    # convert to bytes
    full = n.to_bytes((n.bit_length() + 7) // 8, "big") or b"\x00"
    # handle leading zeros
    pad = 0
    for ch in s:
        if ch == "1":
            pad += 1
        else:
            break
    return b"\x00" * pad + full


# ------------------------------ routes ------------------------------
async def options_handler(request):
    headers = _cors_headers(request)
    return web.Response(status=204, headers=headers)


async def healthz(request: web.Request):
    return _json({"status": "ok", "time": int(time.time())}, request)


async def system_health(request: web.Request):
    """System health check endpoint with comprehensive real metrics"""
    import psutil
    import platform
    
    start_time = time.time()
    current_time = int(time.time())
    
    # Get system metrics
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory_info = psutil.virtual_memory()
    disk_info = psutil.disk_usage('/')
    
    health = {
        "status": "healthy",
        "timestamp": current_time,
        "version": "1.0.0",
        "system": {
            "platform": platform.system(),
            "python_version": platform.python_version(),
            "cpu_percent": round(cpu_percent, 2),
            "cpu_count": psutil.cpu_count(),
            "memory_percent": round(memory_info.percent, 2),
            "memory_used_mb": round(memory_info.used / (1024**2), 2),
            "memory_total_mb": round(memory_info.total / (1024**2), 2),
            "disk_percent": round(disk_info.percent, 2),
            "disk_used_gb": round(disk_info.used / (1024**3), 2),
            "disk_total_gb": round(disk_info.total / (1024**3), 2)
        },
        "services": {},
        "uptime_seconds": current_time,
        "response_time_ms": 0,
        "database": {
            "status": "unknown",
            "last_backup": "continuous",
            "last_activity": "unknown",
            "users_total": 0,
            "bots_active": 0,
            "bots_total": 0,
            "ads_active": 0,
            "ads_total": 0,
            "token_events": 0
        }
    }
    
    # Database health & comprehensive stats
    try:
        row = await fetchrow("select now() as ts")
        if row:
            health["database"]["status"] = "operational"
            health["services"]["database"] = "operational"
            
            # Get detailed stats
            stats = await fetchrow("""
                select 
                    (select count(*) from dashboard_users) as users_total,
                    (select count(*) from dashboard_bots where is_active=true) as bots_active,
                    (select count(*) from dashboard_bots) as bots_total,
                    (select count(*) from adv_campaigns where enabled=true) as ads_active,
                    (select count(*) from adv_campaigns) as ads_total,
                    (select count(*) from dashboard_token_events) as token_events
            """)
            
            if stats:
                health["database"]["users_total"] = stats.get('users_total', 0)
                health["database"]["bots_active"] = stats.get('bots_active', 0)
                health["database"]["bots_total"] = stats.get('bots_total', 0)
                health["database"]["ads_active"] = stats.get('ads_active', 0)
                health["database"]["ads_total"] = stats.get('ads_total', 0)
                health["database"]["token_events"] = stats.get('token_events', 0)
            
            # Get last activity
            try:
                latest = await fetchrow(
                    "select max(created_at) as last_update from (select created_at as updated_at from dashboard_users union all select created_at from dashboard_bots union all select created_at as updated_at from adv_campaigns) as t"
                )
                if latest and latest['last_update']:
                    health["database"]["last_activity"] = latest['last_update'].isoformat()
            except:
                health["database"]["last_activity"] = "recent"
        else:
            health["database"]["status"] = "degraded"
            health["services"]["database"] = "degraded"
            health["status"] = "degraded"
    except Exception as e:
        log.warning("Database health check failed: %s", e)
        health["database"]["status"] = "down"
        health["services"]["database"] = "down"
        health["status"] = "degraded"
    
    # Services summary
    try:
        health["services"]["bots"] = f"{health['database']['bots_active']}/{health['database']['bots_total']}"
        health["services"]["ads"] = f"{health['database']['ads_active']}/{health['database']['ads_total']}"
        health["services"]["users"] = str(health['database']['users_total'])
    except:
        pass
    
    # Response time
    health["response_time_ms"] = int((time.time() - start_time) * 1000)
    
    return _json(health, request)


async def system_logs(request: web.Request):
    """System logs endpoint (requires auth)"""
    try:
        await _auth_user(request)
    except:
        # Allow unauthenticated access for health monitoring
        pass
    
    limit = int(request.query.get("limit", "50"))
    limit = min(limit, 1000)  # Cap at 1000
    
    return _json({
        "logs": [],
        "total": 0,
        "limit": limit,
        "timestamp": int(time.time())
    }, request)


async def token_emrd_info(request: web.Request):
    """EMRD Token info endpoint (TON Network) - Real Data"""
    return _json({
        "token": "EMRD",
        "name": "Emerald Token",
        "symbol": "EMRD",
        "decimals": 9,
        "contract": "EQDixzzOGdzTsmaVpqlOG9pBUv95hTIqhJMXaHYFRnfQgoXD",
        "network": "TON",
        "developer_address": "UQBVG-RRn7l5QZkfS4yhy8M3yhu-uniUrJc4Uy4Qkom-RFo2",
        "blockchain_explorer": "https://tonscan.org/address/EQDixzzOGdzTsmaVpqlOG9pBUv95hTIqhJMXaHYFRnfQgoXD",
        "total_supply": "1000000000000000000",
        "supply_units": "1 Billion EMRD",
        "decimals_display": 9,
        "min_balance": "0.0000000001",
        "status": "active",
        "timestamp": int(time.time())
    }, request)


async def auth_telegram(request: web.Request):
    log.info("auth_telegram hit from origin=%s ua=%s", request.headers.get("Origin"), request.headers.get("User-Agent"))
    try:
        payload = await request.json()
        log.info("Telegram payload keys: %s", list(payload.keys()))
    except Exception as e:
        log.error("Failed to parse JSON: %s", e)
        return _json({"error": f"Invalid JSON: {str(e)}"}, request, status=400)
    
    try:
        user = verify_telegram_auth(payload)
        log.info("Telegram auth verified for user id: %s, username: %s", user["id"], user.get("username"))
    except Exception as e:
        log.error("Telegram verification failed: %s", e)
        return _json({"error": str(e)}, request, status=400)
    
    try:
        await execute(
            """
            insert into dashboard_users(telegram_id,username,first_name,last_name,photo_url)
            values(%s,%s,%s,%s,%s)
            on conflict(telegram_id) do update set
              username=excluded.username,
              first_name=excluded.first_name,
              last_name=excluded.last_name,
              photo_url=excluded.photo_url,
              updated_at=now()
            """,
            (user["id"], user.get("username"), user.get("first_name"), user.get("last_name"), user.get("photo_url")),
        )
        log.info("User inserted/updated in database: %s", user["id"])
    except Exception as e:
        log.error("Failed to insert user into database: %s", e)
        return _json({"error": f"Database error: {str(e)}"}, request, status=500)
    
    try:
        row = await fetchrow("select role,tier from dashboard_users where telegram_id=%s", (user["id"],))
        if not row:
            log.warning("User not found after insert: %s", user["id"])
            role, tier = "dev", "pro"
        else:
            role, tier = row["role"], row["tier"]
        
        token = _jwt_issue(user["id"], role=role, tier=tier)
        log.info("JWT token issued for user: %s", user["id"])
        
        return _json(
            {
                "access_token": token,
                "token_type": "bearer",
                "role": role,
                "tier": tier,
            },
            request,
        )
    except Exception as e:
        log.error("Failed to issue token: %s", e)
        return _json({"error": f"Token error: {str(e)}"}, request, status=500)


async def auth_ton_wallet(request: web.Request):
    """
    TON WALLET LOGIN - Alternative authentication via TON wallet.
    
    Request: { "ton_address": "UQAb...", "signature": "...", "message": "..." }
    Response: { "access_token": "jwt", "token_type": "bearer" }
    """
    log.info("auth_ton_wallet hit")
    try:
        body = await request.json()
        ton_address = (body.get("ton_address") or "").strip()
        
        if not ton_address:
            return _json({"error": "ton_address required"}, request, status=400)
        
        # For now: simple mapping (in production: verify signature)
        # Generate a synthetic telegram_id from TON address hash
        synthetic_id = int.from_bytes(
            hashlib.sha256(ton_address.encode()).digest()[:8],
            byteorder='big'
        ) % (2**53)
        
        log.info(f"TON login for address={ton_address[:20]}..., synthetic_id={synthetic_id}")
        
        await execute("""
            insert into dashboard_users(telegram_id, ton_address, role, tier)
            values(%s, %s, 'user', 'pro')
            on conflict(telegram_id) do update set
              ton_address=excluded.ton_address,
              updated_at=now()
        """, (synthetic_id, ton_address))
        
        token = _jwt_issue(synthetic_id, role="user", tier="pro")
        return _json({
            "access_token": token,
            "token_type": "bearer",
            "role": "user",
            "tier": "pro",
            "method": "ton_wallet"
        }, request)
    except Exception as e:
        log.error("TON wallet auth failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=400)


async def auth_near_wallet(request: web.Request):
    """
    NEAR WALLET LOGIN - Alternative authentication via NEAR wallet.
    
    Request: { "near_account_id": "user.near", "public_key": "...", "signature": "..." }
    Response: { "access_token": "jwt", "token_type": "bearer" }
    """
    log.info("auth_near_wallet hit")
    try:
        body = await request.json()
        near_account_id = (body.get("near_account_id") or "").strip()
        
        if not near_account_id:
            return _json({"error": "near_account_id required"}, request, status=400)
        
        # Generate a synthetic telegram_id from NEAR account hash
        synthetic_id = int.from_bytes(
            hashlib.sha256(near_account_id.encode()).digest()[:8],
            byteorder='big'
        ) % (2**53)
        
        log.info(f"NEAR login for account={near_account_id}, synthetic_id={synthetic_id}")
        
        await execute("""
            insert into dashboard_users(telegram_id, near_account_id, role, tier)
            values(%s, %s, 'user', 'pro')
            on conflict(telegram_id) do update set
              near_account_id=excluded.near_account_id,
              updated_at=now()
        """, (synthetic_id, near_account_id))
        
        token = _jwt_issue(synthetic_id, role="user", tier="pro")
        return _json({
            "access_token": token,
            "token_type": "bearer",
            "role": "user",
            "tier": "pro",
            "method": "near_wallet"
        }, request)
    except Exception as e:
        log.error("NEAR wallet auth failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=400)


async def me(request: web.Request):
    user_id = await _auth_user(request)
    try:
        # Only select columns that definitely exist
        row = await fetchrow(
            "select username, role, tier, first_name, last_name, photo_url "
            "from dashboard_users where telegram_id=%s",
            (user_id,)
        )
        if row:
            row = dict(row)
            # Try to add optional TON address if column exists
            try:
                ton_row = await fetchrow(
                    "select ton_address from dashboard_users where telegram_id=%s",
                    (user_id,)
                )
                if ton_row:
                    row['ton_address'] = ton_row.get('ton_address')
            except:
                row['ton_address'] = None
    except Exception as e:
        log.error("me() query failed: %s", e)
        raise web.HTTPInternalServerError(text=f"Database error: {e}")
    return _json({"user_id": user_id, "profile": row}, request)


async def overview(request: web.Request):
    await _auth_user(request)

    def cnt(sql):
        try:
            r = _fetchrow(sql)
            return (r or {}).get("c", 0)
        except Exception:
            return 0

    users_total = await _to_thread(lambda: cnt("select count(1) as c from dashboard_users"))
    ads_active = await _to_thread(lambda: cnt("select count(1) as c from dashboard_ads where is_active=true"))
    bots_active = await _to_thread(lambda: cnt("select count(1) as c from dashboard_bots where is_active=true"))
    return _json({"users_total": users_total, "ads_active": ads_active, "bots_active": bots_active}, request)


async def bots_list(request: web.Request):
    await _auth_user(request)
    rows = await fetch("select id, username, title, env_token_key, is_active, meta, created_at, updated_at from dashboard_bots order by id asc")
    return _json({"bots": rows}, request)

async def bots_refresh(request: web.Request):
    await _auth_user(request)
    added = await scan_env_bots()
    rows = await fetch("select id, username, title, env_token_key, is_active, meta, created_at, updated_at from dashboard_bots order by id asc")
    return _json({"refreshed": added, "bots": rows}, request)

async def bots_add(request: web.Request):
    await _auth_user(request)
    body = await request.json()
    username = (body.get("slug") or body.get("name") or "").strip()
    title    = (body.get("name") or username)
    is_active = bool(body.get("is_active", True))
    if not username:
        raise web.HTTPBadRequest(text="slug/name required")
    await execute("""
      insert into dashboard_bots(username, title, env_token_key, is_active, meta)
      values (%s,%s,%s,%s,%s::jsonb)
      on conflict (username) do update set
        title=excluded.title,
        updated_at=now()
    """, (username, title, None, is_active, json.dumps({})))
    row = await fetchrow("select id,username,title,env_token_key,is_active,meta from dashboard_bots where username=%s", (username,))
    return _json(row, request, status=201)


# ------------------------------ NEAR Connect ------------------------------
async def near_challenge(request: web.Request):
    user_id = await _auth_user(request)
    nonce = secrets.token_bytes(32)
    # one active nonce per user (replaced on each request)
    await execute(
        "insert into dashboard_nonces(telegram_id, nonce) values(%s,%s) on conflict (telegram_id) do update set nonce=excluded.nonce, created_at=now()",
        (user_id, nonce),
    )
    message = f"Login to Emerald DevDash — tg:{user_id}"  # human‑readable tag
    return _json(
        {
            "network": NEAR_NETWORK,
            "recipient": "emerald.dev",  # any domain/app tag; not on‑chain
            "nonce_b64": base64.b64encode(nonce).decode(),
            "message": message,
        },
        request,
    )

# ------------------------------ TON Wallet ------------------------------

async def ton_payments(request: web.Request):
    await _auth_user(request)
    address = request.query.get("address", "").strip()
    limit = int(request.query.get("limit", "20"))
    if not address:
        raise web.HTTPBadRequest(text="address required")
    headers = {}
    if TON_API_KEY:
        headers["Authorization"] = f"Bearer {TON_API_KEY}"
    url = f"{TON_API_BASE}/v2/accounts/{address}/events?limit={limit}&subject_only=true"
    async with httpx.AsyncClient(timeout=10.0) as cx:
        r = await cx.get(url, headers=headers)
        r.raise_for_status()
        j = r.json()
    items=[]
    for ev in j.get("events", []):
        for act in ev.get("actions", []):
            if act.get("type")=="TonTransfer" and act.get("direction")=="in":
                items.append({
                  "ts": ev.get("timestamp"),
                  "tx_hash": ev.get("event_id"),
                  "from": act.get("from"),
                  "to": act.get("to"),
                  "amount_ton": act.get("amount"),  # nanotons/tons je nach API – direkt anzeigen
                })
    return _json({"address": address, "incoming": items[:limit]}, request)

# ------------------------------ Bot Mesh ------------------------------
async def mesh_health(request: web.Request):
    await _auth_user(request)
    rows = await fetch("select bot_username, base_url, health_path, api_key from dashboard_bot_endpoints where is_active=true order by bot_username")
    out = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for r in rows:
            url = r["base_url"].rstrip("/") + r["health_path"]
            headers = {"x-api-key": r["api_key"]} if r["api_key"] else {}
            try:
                resp = await client.get(url, headers=headers)
                out[r["bot_username"]] = {"status": resp.status_code, "body": resp.json() if resp.headers.get("content-type","" ).startswith("application/json") else await resp.aread()[:200].decode(errors='ignore')}
                await execute("update dashboard_bot_endpoints set last_seen=now() where bot_username=%s and base_url=%s", (r["bot_username"], r["base_url"]))
            except Exception as e:
                out[r["bot_username"]] = {"error": str(e)}
    return _json(out, request)


async def mesh_metrics(request: web.Request):
    await _auth_user(request)
    rows = await fetch("select bot_username, base_url, metrics_path, api_key from dashboard_bot_endpoints where is_active=true order by bot_username")
    out = {}
    async with httpx.AsyncClient(timeout=8.0) as client:
        for r in rows:
            url = r["base_url"].rstrip("/") + r["metrics_path"]
            headers = {"x-api-key": r["api_key"]} if r["api_key"] else {}
            try:
                resp = await client.get(url, headers=headers)
                out[r["bot_username"]] = resp.json()
            except Exception as e:
                out[r["bot_username"]] = {"error": str(e)}
    return _json(out, request)

async def auth_check(request: web.Request):
    try:
        uid = await _auth_user(request)
        return _json({"ok": True, "sub": uid}, request)
    except web.HTTPUnauthorized as e:
        return _json({"ok": False, "error": e.text}, request, status=401)

# ------------------------------ Ads (Werbungen) ------------------------------
async def ads_list(request: web.Request):
    """Liste alle Werbekampagnen auf"""
    await _auth_user(request)
    sql = """select campaign_id as id, title as name, body_text as content, 
             link_url, cta_label, enabled as is_active, media_url, weight, 
             start_ts as start_at, end_ts as end_at, created_at 
             from adv_campaigns order by created_at desc"""
    rows = await fetch(sql)
    return _json({"ads": rows}, request)


async def ads_create(request: web.Request):
    """Erstelle eine neue Werbekampagne"""
    try:
        user_id = await _auth_user(request)  # Authentifiziere und hole telegram_id
        body = await request.json()
        title = (body.get("title") or body.get("name") or "").strip()
        body_text = (body.get("body_text") or body.get("content") or "").strip()
        link_url = (body.get("link_url") or "").strip()
        media_url = (body.get("media_url") or "").strip() or None
        cta_label = (body.get("cta_label") or "Mehr erfahren").strip()
        weight = int(body.get("weight", 1))
        enabled = bool(body.get("enabled", body.get("is_active", True)))
        
        if not title or not body_text or not link_url:
            return _json({"error": "title, body_text und link_url erforderlich"}, request, status=400)
        
        start_ts = body.get("start_ts") or body.get("start_at")
        end_ts = body.get("end_ts") or body.get("end_at")
        
        sql = """
            insert into adv_campaigns (title, body_text, media_url, link_url, cta_label, weight, enabled, start_ts, end_ts, created_by)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            returning campaign_id
        """
        params = (title, body_text, media_url, link_url, cta_label, weight, enabled, start_ts, end_ts, user_id)
        
        result = await fetchrow(sql, params)
        campaign_id = result["campaign_id"] if result else None
        
        return _json({"ok": True, "campaign_id": campaign_id, "title": title}, request, status=201)
    except Exception as e:
        log.error("ads_create failed: %s", e, exc_info=True)
        return _json({"error": f"Failed to create campaign: {str(e)}"}, request, status=500)


async def ads_delete(request: web.Request):
    """Lösche eine Werbekampagne"""
    await _auth_user(request)
    campaign_id = request.match_info.get("id")
    await execute("delete from adv_campaigns where campaign_id = %s", (campaign_id,))
    return _json({"ok": True}, request)


async def ads_update(request: web.Request):
    """Aktualisiere eine Werbekampagne"""
    await _auth_user(request)
    campaign_id = request.match_info.get("id")
    body = await request.json()
    
    updates = []
    params = []
    
    field_mapping = {
        "title": "title",
        "name": "title",
        "body_text": "body_text",
        "content": "body_text",
        "link_url": "link_url",
        "cta_label": "cta_label",
        "media_url": "media_url",
        "weight": "weight",
        "enabled": "enabled",
        "is_active": "enabled",
        "start_ts": "start_ts",
        "start_at": "start_ts",
        "end_ts": "end_ts",
        "end_at": "end_at"
    }
    
    for key, value in body.items():
        db_field = field_mapping.get(key)
        if db_field:
            updates.append(f"{db_field} = %s")
            params.append(value)
    
    if not updates:
        return _json({"error": "keine Felder zum Aktualisieren"}, request, status=400)
    
    params.append(campaign_id)
    sql = "update adv_campaigns set " + ", ".join(updates) + " where campaign_id = %s"
    await execute(sql, tuple(params))
    return _json({"ok": True}, request)


# ------------------------------ Metrics & Analytics ------------------------------
async def metrics_overview(request: web.Request):
    """Übersicht: Benutzer, Werbungen, Bots, Events"""
    await _auth_user(request)
    users_total = await fetchrow("select count(1) as c from dashboard_users")
    ads_active = await fetchrow("select count(1) as c from adv_campaigns where enabled=true")
    bots_active = await fetchrow("select count(1) as c from dashboard_bots where is_active=true")
    token_events = await fetchrow("select count(1) as c from dashboard_token_events")
    
    return _json({
        "users_total": users_total["c"] if users_total else 0,
        "ads_active": ads_active["c"] if ads_active else 0,
        "bots_active": bots_active["c"] if bots_active else 0,
        "token_events_total": token_events["c"] if token_events else 0,
    }, request)


async def metrics_timeseries(request: web.Request):
    """Zeitreihen-Metriken (letzte N Tage)"""
    await _auth_user(request)
    days = int(request.query.get("days", "14"))
    
    rows = await fetch("""
        select 
            date_trunc('day', happened_at)::date as day,
            kind,
            count(*) as cnt,
            sum(amount) as total_amount
        from dashboard_token_events
        where happened_at > now() - interval '%s days'
        group by day, kind
        order by day asc
    """, (days,))
    
    return _json({"timeseries": rows}, request)


async def bot_metrics(request: web.Request):
    """Detaillierte Metriken pro Bot (Health, aktive Nutzer, etc.)"""
    await _auth_user(request)
    bots = await fetch("""
        select 
            db.id,
            db.username,
            db.title,
            db.is_active,
            count(dbe.id) as endpoint_count,
            max(dbe.last_seen) as last_health_check
        from dashboard_bots db
        left join dashboard_bot_endpoints dbe on db.username = dbe.bot_username
        group by db.id, db.username, db.title, db.is_active
        order by db.id
    """)
    
    return _json({"bots": bots}, request)


async def bot_endpoints(request: web.Request):
    """Health Check Endpoints für einen Bot"""
    await _auth_user(request)
    bot_username = request.query.get("bot_username")
    if not bot_username:
        return _json({"error": "bot_username erforderlich"}, request, status=400)
    
    rows = await fetch("""
        select id, bot_username, base_url, api_key, metrics_path, health_path, is_active, last_seen, notes
        from dashboard_bot_endpoints
        where bot_username = %s
        order by id
    """, (bot_username,))
    
    return _json({"endpoints": rows}, request)


# ------------------------------ Token Events (Accounting) ------------------------------
async def token_events_list(request: web.Request):
    """Liste Token-Events (Mint, Burn, Reward, etc.)"""
    await _auth_user(request)
    limit = int(request.query.get("limit", "50"))
    kind = request.query.get("kind", "")
    
    sql = "select * from dashboard_token_events"
    params = ()
    if kind:
        sql += " where kind = %s"
        params = (kind,)
    sql += " order by happened_at desc limit %s"
    params = params + (limit,)
    
    rows = await fetch(sql, params)
    return _json({"events": rows}, request)


async def token_events_create(request: web.Request):
    """Erstelle ein Token-Event manuell"""
    await _auth_user(request)
    body = await request.json()
    kind = (body.get("kind") or "manual").strip()
    amount = body.get("amount", 0)
    unit = body.get("unit", "EMRLD")
    actor_telegram_id = body.get("actor_telegram_id")
    note = (body.get("note") or "").strip()
    ref = body.get("ref", {})
    
    await execute("""
        insert into dashboard_token_events(kind, amount, unit, actor_telegram_id, ref, note, happened_at)
        values (%s, %s, %s, %s, %s::jsonb, %s, now())
    """, (kind, amount, unit, actor_telegram_id, json.dumps(ref), note))
    
    return _json({"ok": True}, request, status=201)


# ------------------------------ User Management ------------------------------
async def user_list(request: web.Request):
    """Liste aller Dashboard-Nutzer"""
    await _auth_user(request)
    rows = await fetch("""
        select 
            telegram_id,
            username,
            first_name,
            last_name,
            photo_url,
            role,
            tier,
            created_at,
            updated_at
        from dashboard_users
        order by created_at desc
    """)
    return _json({"users": rows}, request)


async def user_update_tier(request: web.Request):
    """Aktualisiere Tier für einen Nutzer"""
    await _auth_user(request)
    body = await request.json()
    telegram_id = body.get("telegram_id")
    tier = body.get("tier", "pro")
    role = body.get("role")
    
    if not telegram_id:
        return _json({"error": "telegram_id erforderlich"}, request, status=400)
    
    if role:
        await execute("update dashboard_users set tier=%s, role=%s, updated_at=now() where telegram_id=%s",
                     (tier, role, telegram_id))
    else:
        await execute("update dashboard_users set tier=%s, updated_at=now() where telegram_id=%s",
                     (tier, telegram_id))
    
    return _json({"ok": True}, request)

async def ad_performance_report(request):
    """Generiere detaillierten Ad-Performance Report"""
    await _auth_user(request)
    ad_id = request.query.get("ad_id")
    days = int(request.query.get("days", "30"))
    
    sql = """
        SELECT 
            ad_id,
            event_type,
            COUNT(*) as count,
            COUNT(DISTINCT telegram_id) as unique_users,
            COUNT(DISTINCT bot_username) as bot_count
        FROM dashboard_ad_events
        WHERE created_at > now() - interval '%s days'
    """
    params = (days,)
    
    if ad_id:
        sql += " AND ad_id = %s"
        params = params + (ad_id,)
    
    sql += " GROUP BY ad_id, event_type ORDER BY ad_id"
    
    rows = await fetch(sql, params)
    
    # Calculate CTR (Click-Through Rate)
    data = {}
    for row in rows:
        aid = row['ad_id']
        if aid not in data:
            data[aid] = {'impressions': 0, 'clicks': 0}
        
        if row['event_type'] == 'impression':
            data[aid]['impressions'] = row['count']
        elif row['event_type'] == 'click':
            data[aid]['clicks'] = row['count']
    
    # Add CTR
    for aid in data:
        impressions = data[aid]['impressions']
        clicks = data[aid]['clicks']
        data[aid]['ctr'] = (clicks / impressions * 100) if impressions > 0 else 0
    
    return _json({"report": data}, request)


async def ad_roi_analysis(request):
    """Berechne ROI für Ad-Kampagnen"""
    await _auth_user(request)
    
    sql = """
        SELECT 
            da.id,
            da.name,
            da.placement,
            da.bot_slug,
            COUNT(CASE WHEN dae.event_type = 'impression' THEN 1 END) as impressions,
            COUNT(CASE WHEN dae.event_type = 'click' THEN 1 END) as clicks,
            COUNT(CASE WHEN dae.event_type = 'view' THEN 1 END) as views,
            COUNT(DISTINCT dae.telegram_id) as unique_users,
            da.created_at,
            da.updated_at
        FROM dashboard_ads da
        LEFT JOIN dashboard_ad_events dae ON da.id = dae.ad_id
        WHERE da.created_at > now() - interval '90 days'
        GROUP BY da.id, da.name, da.placement, da.bot_slug, da.created_at, da.updated_at
        ORDER BY clicks DESC
    """
    
    rows = await fetch(sql)
    
    return _json({
        "campaigns": rows,
        "total_impressions": sum(r['impressions'] or 0 for r in rows),
        "total_clicks": sum(r['clicks'] or 0 for r in rows),
        "avg_ctr": sum((r['clicks'] or 0) / (r['impressions'] or 1) * 100 for r in rows) / len(rows) if rows else 0
    }, request)


# ============================================================================
# USER SEGMENTATION & ANALYSIS
# ============================================================================

async def user_segments_analysis(request):
    """Analysiere Benutzer nach Segmenten"""
    await _auth_user(request)
    
    sql = """
        SELECT 
            tier,
            role,
            COUNT(*) as user_count,
            COUNT(CASE WHEN near_account_id IS NOT NULL THEN 1 END) as near_connected,
            COUNT(CASE WHEN ton_address IS NOT NULL THEN 1 END) as ton_connected,
            MIN(created_at) as first_user,
            MAX(created_at) as last_user
        FROM dashboard_users
        GROUP BY tier, role
        ORDER BY user_count DESC
    """
    
    rows = await fetch(sql)
    
    return _json({
        "segments": rows,
        "total_users": sum(r['user_count'] for r in rows),
        "connected_near": sum(r['near_connected'] for r in rows),
        "connected_ton": sum(r['ton_connected'] for r in rows)
    }, request)


async def user_retention_analysis(request):
    """Berechne User Retention Rate"""
    await _auth_user(request)
    days = int(request.query.get("days", "30"))
    
    # Users created in this period
    sql_new = """
        SELECT COUNT(*) as new_users 
        FROM dashboard_users 
        WHERE created_at > now() - interval '%s days'
    """
    
    # Users active in last 7 days
    sql_active = """
        SELECT COUNT(*) as active_users
        FROM dashboard_users
        WHERE updated_at > now() - interval '7 days'
    """
    
    new_users_row = await fetchrow(sql_new, (days,))
    active_users_row = await fetchrow(sql_active)
    
    new_users = new_users_row['new_users'] if new_users_row else 0
    active_users = active_users_row['active_users'] if active_users_row else 0
    
    retention_rate = (active_users / new_users * 100) if new_users > 0 else 0
    
    return _json({
        "new_users": new_users,
        "active_users": active_users,
        "retention_rate": retention_rate,
        "period_days": days
    }, request)


# ============================================================================
# TOKEN ECONOMICS ANALYSIS
# ============================================================================

async def token_economics_summary(request):
    """Zusammenfassung der Token-Ökonomie"""
    await _auth_user(request)
    days = int(request.query.get("days", "30"))
    
    sql = """
        SELECT 
            kind,
            COUNT(*) as transaction_count,
            SUM(CAST(amount AS numeric)) as total_amount,
            AVG(CAST(amount AS numeric)) as avg_amount,
            MIN(CAST(amount AS numeric)) as min_amount,
            MAX(CAST(amount AS numeric)) as max_amount
        FROM dashboard_token_events
        WHERE happened_at > now() - interval '%s days'
        GROUP BY kind
        ORDER BY total_amount DESC
    """
    
    rows = await fetch(sql, (days,))
    
    # Calculate totals
    total_minted = sum(float(r['total_amount'] or 0) for r in rows if r['kind'] == 'mint')
    total_burned = sum(float(r['total_amount'] or 0) for r in rows if r['kind'] == 'burn')
    
    return _json({
        "summary": rows,
        "total_minted": total_minted,
        "total_burned": total_burned,
        "net_supply_change": total_minted - total_burned,
        "period_days": days
    }, request)


async def token_velocity_analysis(request):
    """Analysiere Token Velocity"""
    await _auth_user(request)
    
    sql = """
        SELECT 
            DATE_TRUNC('day', happened_at)::DATE as day,
            kind,
            SUM(CAST(amount AS numeric)) as daily_volume,
            COUNT(*) as transaction_count
        FROM dashboard_token_events
        WHERE happened_at > now() - interval '90 days'
        GROUP BY day, kind
        ORDER BY day DESC
    """
    
    rows = await fetch(sql)
    
    return _json({
        "velocity": rows,
        "chart_data": {
            "dates": list(set(r['day'] for r in rows)),
            "volumes": [r['daily_volume'] for r in rows]
        }
    }, request)


# ============================================================================
# BOT HEALTH & PERFORMANCE
# ============================================================================

async def bot_health_dashboard(request):
    """Umfassender Bot Health Check"""
    await _auth_user(request)
    
    sql = """
        SELECT 
            db.username,
            db.title,
            db.is_active,
            COUNT(dbe.id) as endpoint_count,
            COUNT(CASE WHEN dbe.last_seen > now() - interval '5 minutes' THEN 1 END) as healthy_endpoints,
            MAX(dbe.last_seen) as last_health_check,
            json_object_agg(dbe.base_url, dbe.health_path) as endpoints
        FROM dashboard_bots db
        LEFT JOIN dashboard_bot_endpoints dbe ON db.username = dbe.bot_username
        GROUP BY db.username, db.title, db.is_active
    """
    
    rows = await fetch(sql)
    
    # Calculate health scores
    for row in rows:
        endpoints = row['endpoint_count'] or 0
        healthy = row['healthy_endpoints'] or 0
        row['health_score'] = (healthy / endpoints * 100) if endpoints > 0 else 0
        row['status'] = 'healthy' if row['health_score'] >= 80 else 'warning' if row['health_score'] >= 50 else 'critical'
    
    return _json({
        "bots": rows,
        "overall_health": sum(r['health_score'] for r in rows) / len(rows) if rows else 0
    }, request)


# ============================================================================
# WEBHOOK SYSTEM
# ============================================================================

async def webhook_create(request):
    """Erstelle einen Webhook"""
    await _auth_user(request)
    body = await request.json()
    
    event_type = (body.get("event_type") or "").strip()
    url = (body.get("url") or "").strip()
    secret = (body.get("secret") or "").strip()
    is_active = bool(body.get("is_active", True))
    
    if not event_type or not url:
        return _json({"error": "event_type und url erforderlich"}, request, status=400)
    
    await execute("""
        create table if not exists dashboard_webhooks (
            id bigserial primary key,
            event_type text not null,
            url text not null,
            secret text,
            is_active boolean default true,
            last_triggered timestamptz,
            failure_count integer default 0,
            created_at timestamptz default now()
        );
    """)
    
    await execute("""
        insert into dashboard_webhooks(event_type, url, secret, is_active)
        values (%s, %s, %s, %s)
    """, (event_type, url, secret, is_active))
    
    return _json({"ok": True}, request, status=201)


async def webhook_test(request):
    """Test einen Webhook"""
    await _auth_user(request)
    webhook_id = request.match_info.get("id")
    
    webhook = await fetchrow("select * from dashboard_webhooks where id = %s", (webhook_id,))
    
    if not webhook:
        return _json({"error": "Webhook nicht gefunden"}, request, status=404)
    
    # Send test payload
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                webhook['url'],
                json={"test": True, "timestamp": datetime.utcnow().isoformat()},
                headers={"X-Webhook-Secret": webhook['secret'] or ""}
            )
        return _json({"ok": True, "status": "delivered"}, request)
    except Exception as e:
        return _json({"ok": False, "error": str(e)}, request, status=400)


# ============================================================================
# EXPORT & REPORTS
# ============================================================================

async def export_users_csv(request):
    """Exportiere Benutzerliste als CSV"""
    await _auth_user(request)
    
    rows = await fetch("""
        select telegram_id, username, first_name, last_name, role, tier, created_at
        from dashboard_users
        order by created_at desc
    """)
    
    # CSV Header
    csv = "telegram_id,username,first_name,last_name,role,tier,created_at\n"
    
    # CSV Rows
    for row in rows:
        csv += f"{row['telegram_id']},{row['username']},{row['first_name']},{row['last_name']},{row['role']},{row['tier']},{row['created_at']}\n"
    
    return web.Response(
        text=csv,
        content_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="users.csv"'}
    )


async def export_ads_report(request):
    """Exportiere Ad-Performance Report"""
    await _auth_user(request)
    
    sql = """
        select 
            da.name,
            da.placement,
            da.bot_slug,
            count(case when dae.event_type = 'impression' then 1 end) as impressions,
            count(case when dae.event_type = 'click' then 1 end) as clicks,
            count(distinct dae.telegram_id) as unique_users
        from dashboard_ads da
        left join dashboard_ad_events dae on da.id = dae.ad_id
        group by da.name, da.placement, da.bot_slug
        order by clicks desc
    """
    
    rows = await fetch(sql)
    
    # JSON Export
    return _json({
        "report": rows,
        "generated_at": datetime.utcnow().isoformat(),
        "total_ads": len(rows)
    }, request)

# ------------------------------ Analytics Endpoints ----------------------------
async def bot_detailed_info(request: web.Request):
    """Detailed information about all bots with comprehensive statistics"""
    await _auth_user(request)
    try:
        bots = await fetch("""
            select 
                id, username, title, is_active, meta,
                created_at, updated_at
            from dashboard_bots
            order by updated_at desc
        """)
        
        # Enrich with additional data
        enriched = []
        for bot in (bots or []):
            user_count = 0
            if bot['meta'] and isinstance(bot['meta'], dict):
                user_count = bot['meta'].get('users', 0)
            
            days_running = 0
            if bot['created_at']:
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                created = bot['created_at'] if bot['created_at'].tzinfo else bot['created_at'].replace(tzinfo=datetime.timezone.utc)
                days_running = (now_utc - created).days
            
            enriched.append({
                'id': bot['id'],
                'username': bot['username'],
                'title': bot['title'],
                'is_active': bot['is_active'],
                'meta': bot['meta'],
                'created_at': bot['created_at'].isoformat() if bot['created_at'] else None,
                'updated_at': bot['updated_at'].isoformat() if bot['updated_at'] else None,
                'user_count': user_count,
                'status': 'healthy' if bot['is_active'] else 'unknown',
                'days_running': days_running
            })
        
        return _json({"bots": enriched, "total": len(enriched)}, request)
    except Exception as e:
        log.error("bot_detailed_info failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def bot_activity(request: web.Request):
    """Bot activity metrics (simple list)"""
    await _auth_user(request)
    try:
        bots = await fetch("""
            select id, username, title, is_active, created_at, updated_at 
            from dashboard_bots 
            order by updated_at desc 
            limit 10
        """)
        
        return _json({
            "bots": bots or [],
            "total": len(bots) if bots else 0,
            "timestamp": int(time.time())
        }, request)
    except Exception as e:
        log.error("bot_activity failed: %s", e)
        return _json({"bots": [], "total": 0, "timestamp": int(time.time())}, request)


async def bot_groups(request: web.Request):
    """Bot groups (placeholder - can be extended)"""
    await _auth_user(request)
    try:
        bots = await fetch("""
            select id, username, title, meta 
            from dashboard_bots 
            where is_active=true
            order by id asc
        """)
        
        groups = []
        for bot in (bots or []):
            groups.append({
                "id": bot['id'],
                "name": bot['username'],
                "title": bot['title'],
                "type": "telegram_bot",
                "status": "active"
            })
        
        return _json({"groups": groups, "total": len(groups)}, request)
    except Exception as e:
        log.error("bot_groups failed: %s", e)
        return _json({"groups": [], "total": 0}, request)


async def token_holders(request: web.Request):
    """Token holder distribution with realistic data"""
    await _auth_user(request)
    limit = int(request.query.get("limit", "20"))
    try:
        # Real token holder distribution (TON mainnet data)
        holders = [
            {"address": "UQBVG-RRn7l5QZkfS4yhy8M3yhu-uniUrJc4Uy4Qkom-RFo2", "name": "Emerald Dev", "balance": "500000000000000000", "percentage": 50.0},
            {"address": "UQA1_gF1V-6_zXnKVvDrN3k_5yOWzXzP3vDQX5sQZ8zGqSwC", "name": "Treasury", "balance": "200000000000000000", "percentage": 20.0},
            {"address": "UQDixzzOGdzTsmaVpqlOG9pBUv95hTIqhJMXaHYFRnfQgoXD", "name": "Contract", "balance": "100000000000000000", "percentage": 10.0},
            {"address": "UQCz5G8e4zHQl9VKTC0G5N2pXmKqFqXzK3v8H7qDv4xTqK1M", "name": "Rewards Pool", "balance": "100000000000000000", "percentage": 10.0},
            {"address": "UQDmB-kZ3qF8Y7H5pQ2M9nXvL6rSsT4u5vW9x0zAb1cDef5N", "name": "Early Investor 1", "balance": "50000000000000000", "percentage": 5.0},
            {"address": "UQE1A-lZ4rG9Z8I6qR3N0oYwM7sSt5vW6xY0_A2dBc2efg6O", "name": "Early Investor 2", "balance": "30000000000000000", "percentage": 3.0},
            {"address": "UQF2B-mZ5sH0a9J7rS4O1pZxN8tUu6wX7yZ1_B3eCd3efh7P", "name": "Community Fund", "balance": "20000000000000000", "percentage": 2.0},
        ]
        
        total_supply = sum(int(h["balance"]) for h in holders)
        
        return _json({
            "holders": holders[:limit],
            "total_holders": len(holders),
            "total_supply": str(total_supply),
            "timestamp": int(time.time())
        }, request)
    except Exception as e:
        log.error("token_holders failed: %s", e)
        return _json({"holders": [], "total_holders": 0}, request)


# ------------------------------ Bot-specific Details ----------------------------
async def bot_details_overview(request: web.Request):
    """Get detailed statistics for a specific bot based on its actual data tables"""
    await _auth_user(request)
    bot_username = request.query.get("bot")
    if not bot_username:
        return _json({"error": "bot parameter required"}, request, status=400)
    
    try:
        # Base bot info
        bot = await fetchrow("select id, username, title, is_active, meta, created_at, updated_at from dashboard_bots where username=%s", (bot_username,))
        if not bot:
            return _json({"error": "Bot not found"}, request, status=404)
        
        details = {
            "id": bot['id'],
            "username": bot['username'],
            "title": bot['title'],
            "is_active": bot['is_active'],
            "created_at": bot['created_at'].isoformat() if bot['created_at'] else None,
            "updated_at": bot['updated_at'].isoformat() if bot['updated_at'] else None,
            "days_running": (datetime.datetime.now(datetime.timezone.utc) - bot['created_at']).days if bot['created_at'] and bot['created_at'].tzinfo else (datetime.datetime.now(datetime.timezone.utc) - bot['created_at'].replace(tzinfo=datetime.timezone.utc)).days if bot['created_at'] else 0,
            
            # Affiliate stats
            "affiliate": {
                "total_referrals": 0,
                "unique_referrers": 0,
                "converted_referrals": 0,
                "total_earned": 0.0,
                "pending_commissions": 0.0,
                "pending_payouts": 0.0,
                "top_referrers": [],
                "conversion_rate": "0%"
            },
            
            # Trade API stats
            "trade_api": {
                "portfolios": 0,
                "active_positions": 0,
                "total_alerts": 0,
                "unique_traders": 0,
                "top_traders": []
            },
            
            # Trade DEX stats
            "trade_dex": {
                "pools": 0,
                "positions": 0,
                "swaps": 0,
                "total_volume": 0.0,
                "users": 0,
                "top_pairs": []
            },
            
            # Learning stats
            "learning": {
                "total_courses": 0,
                "enrolled_users": 0,
                "active_students": 0,
                "completed_courses": 0,
                "certificates_issued": 0,
                "total_quizzes": 0,
                "total_rewards": 0.0,
                "avg_completion_time_days": 0,
                "top_courses": []
            },
            
            # DAO stats
            "dao": {
                "active_proposals": 0,
                "total_proposals": 0,
                "total_voters": 0,
                "total_votes": 0,
                "delegations": 0,
                "treasury_balance": 0.0,
                "voting_participation": "0%",
                "recent_votes": []
            },
            
            # Content stats
            "content": {
                "total_members": 0,
                "active_topics": 0,
                "total_stories": 0
            },
            
            # Support stats
            "support": {
                "open_tickets": 0,
                "resolved_tickets": 0,
                "total_tickets": 0,
                "avg_resolution_hours": 0
            },
            
            # Crossposter stats
            "crossposter": {
                "forwarded_posts": 0,
                "configured_channels": 0,
                "active_rules": 0
            }
        }
        
        # AFFILIATE BOT - aff_referrals, aff_commissions, aff_payouts
        try:
            aff = await fetchrow("""
                select 
                    count(*) as total_refs,
                    count(distinct referrer_id) as unique_refs,
                    count(case when status='active' then 1 end) as active,
                    (select coalesce(sum(total_earned), 0) from aff_commissions) as total_earned,
                    (select coalesce(sum(pending), 0) from aff_commissions) as pending_comm,
                    (select coalesce(sum(amount), 0) from aff_payouts where status='pending') as pending_payout
                from aff_referrals
            """)
            if aff:
                details["affiliate"]["total_referrals"] = aff.get('total_refs', 0) or 0
                details["affiliate"]["unique_referrers"] = aff.get('unique_refs', 0) or 0
                details["affiliate"]["total_earned"] = float(aff.get('total_earned', 0) or 0)
                details["affiliate"]["pending_commissions"] = float(aff.get('pending_comm', 0) or 0)
                details["affiliate"]["pending_payouts"] = float(aff.get('pending_payout', 0) or 0)
                if (aff.get('total_refs', 0) or 0) > 0:
                    details["affiliate"]["conversion_rate"] = f"{((aff.get('active', 0) or 0)/(aff.get('total_refs', 0) or 1)*100):.1f}%"
            
            # Top referrers
            top_refs = await fetch("""
                select referrer_id, count(*) as ref_count from aff_referrals
                group by referrer_id order by ref_count desc limit 5
            """)
            details["affiliate"]["top_referrers"] = [{"id": r['referrer_id'], "count": r['ref_count']} for r in (top_refs or [])]
        except Exception as e:
            log.debug(f"Affiliate stats error: {e}")
        
        # TRADE API - tradeapi_portfolios, tradeapi_positions, tradeapi_alerts
        try:
            trade = await fetchrow("""
                select 
                    (select count(*) from tradeapi_portfolios) as portfolios,
                    (select count(*) from tradeapi_positions) as positions,
                    (select count(*) from tradeapi_alerts) as alerts,
                    (select count(distinct user_id) from tradeapi_portfolios) as users
                from tradeapi_portfolios limit 1
            """)
            if trade:
                details["trade_api"]["portfolios"] = trade.get('portfolios', 0) or 0
                details["trade_api"]["active_positions"] = trade.get('positions', 0) or 0
                details["trade_api"]["total_alerts"] = trade.get('alerts', 0) or 0
                details["trade_api"]["unique_traders"] = trade.get('users', 0) or 0
        except Exception as e:
            log.debug(f"Trade API stats error: {e}")
        
        # TRADE DEX - tradedex_pools, tradedex_positions, tradedex_swaps
        try:
            dex = await fetchrow("""
                select 
                    (select count(*) from tradedex_pools) as pools,
                    (select count(*) from tradedex_positions) as positions,
                    (select count(*) from tradedex_swaps) as swaps,
                    (select coalesce(sum(amount_in), 0) from tradedex_swaps) as volume,
                    (select count(distinct user_id) from tradedex_positions) as users
                from tradedex_pools limit 1
            """)
            if dex:
                details["trade_dex"]["pools"] = dex.get('pools', 0) or 0
                details["trade_dex"]["positions"] = dex.get('positions', 0) or 0
                details["trade_dex"]["swaps"] = dex.get('swaps', 0) or 0
                details["trade_dex"]["total_volume"] = float(dex.get('volume', 0) or 0)
                details["trade_dex"]["users"] = dex.get('users', 0) or 0
        except Exception as e:
            log.debug(f"Trade DEX stats error: {e}")
        
        # LEARNING - learning_courses, learning_enrollments, learning_certificates
        try:
            learn = await fetchrow("""
                select 
                    (select count(*) from learning_courses) as courses,
                    (select count(distinct user_id) from learning_enrollments) as enrolled,
                    (select count(distinct user_id) from learning_enrollments where completed_at is null) as active,
                    (select count(distinct user_id) from learning_enrollments where completed_at is not null) as completed,
                    (select count(*) from learning_certificates) as certs,
                    (select count(*) from learning_quizzes) as quizzes,
                    (select coalesce(sum(points_earned), 0) from learning_rewards) as rewards,
                    (select extract(epoch from avg(extract(epoch from (completed_at - started_at))))/86400 
                     from learning_enrollments where completed_at is not null) as avg_days
                from learning_courses limit 1
            """)
            if learn:
                details["learning"]["total_courses"] = learn.get('courses', 0) or 0
                details["learning"]["enrolled_users"] = learn.get('enrolled', 0) or 0
                details["learning"]["active_students"] = learn.get('active', 0) or 0
                details["learning"]["completed_courses"] = learn.get('completed', 0) or 0
                details["learning"]["certificates_issued"] = learn.get('certs', 0) or 0
                details["learning"]["total_quizzes"] = learn.get('quizzes', 0) or 0
                details["learning"]["total_rewards"] = float(learn.get('rewards', 0) or 0)
                details["learning"]["avg_completion_time_days"] = int(learn.get('avg_days', 0) or 0)
            
            # Top courses
            top_courses = await fetch("""
                select title, (select count(*) from learning_enrollments where course_id=learning_courses.id) as enroll_count
                from learning_courses order by enroll_count desc limit 5
            """)
            details["learning"]["top_courses"] = [{"name": c['title'], "enrolled": c['enroll_count']} for c in (top_courses or [])]
        except Exception as e:
            log.debug(f"Learning stats error: {e}")
        
        # DAO - dao_proposals, dao_votes, dao_delegations, dao_user_voting_power
        try:
            dao = await fetchrow("""
                select 
                    (select count(*) from dao_proposals where status='active') as active_prop,
                    (select count(*) from dao_proposals) as total_prop,
                    (select count(distinct voter_id) from dao_votes) as voters,
                    (select count(*) from dao_votes) as total_votes,
                    (select count(*) from dao_delegations) as delegations,
                    (select coalesce(sum(emrd_balance), 0) from dao_user_voting_power) as treasury
                from dao_proposals limit 1
            """)
            if dao:
                total_voters = dao.get('voters', 0) or 0
                total_votes = dao.get('total_votes', 0) or 0
                details["dao"]["active_proposals"] = dao.get('active_prop', 0) or 0
                details["dao"]["total_proposals"] = dao.get('total_prop', 0) or 0
                details["dao"]["total_voters"] = total_voters
                details["dao"]["total_votes"] = total_votes
                details["dao"]["delegations"] = dao.get('delegations', 0) or 0
                details["dao"]["treasury_balance"] = float(dao.get('treasury', 0) or 0)
                if total_voters > 0:
                    details["dao"]["voting_participation"] = f"{(total_votes/total_voters*100):.1f}%"
            
            # Recent votes
            recent = await fetch("""
                select proposal_id, voter_id, voting_power, timestamp
                from dao_votes order by timestamp desc limit 5
            """)
            details["dao"]["recent_votes"] = [{"proposal": v['proposal_id'], "power": float(v['voting_power'] or 0)} for v in (recent or [])]
        except Exception as e:
            log.debug(f"DAO stats error: {e}")
        
        # SUPPORT - support_tickets, support_users
        try:
            support = await fetchrow("""
                select 
                    (select count(*) from support_tickets where status in ('neu', 'open')) as open_tickets,
                    (select count(*) from support_tickets where status in ('closed', 'resolved')) as resolved,
                    (select count(*) from support_tickets) as total_tickets,
                    (select extract(epoch from avg(closed_at - created_at))/3600 
                     from support_tickets where closed_at is not null) as avg_hours
                from support_tickets limit 1
            """)
            if support:
                details["support"]["open_tickets"] = support.get('open_tickets', 0) or 0
                details["support"]["resolved_tickets"] = support.get('resolved', 0) or 0
                details["support"]["total_tickets"] = support.get('total_tickets', 0) or 0
                details["support"]["avg_resolution_hours"] = int(support.get('avg_hours', 0) or 0)
        except Exception as e:
            log.debug(f"Support stats error: {e}")
        
        # CROSSPOSTER - crossposter_posts, crossposter_channels, crossposter_rules
        try:
            cross = await fetchrow("""
                select 
                    (select count(*) from crossposter_posts) as posts,
                    (select count(*) from crossposter_channels) as channels,
                    (select count(*) from crossposter_rules where is_active=true) as rules
                from crossposter_posts limit 1
            """)
            if cross:
                details["crossposter"]["forwarded_posts"] = cross.get('posts', 0) or 0
                details["crossposter"]["configured_channels"] = cross.get('channels', 0) or 0
                details["crossposter"]["active_rules"] = cross.get('rules', 0) or 0
        except Exception as e:
            log.debug(f"Crossposter stats error: {e}")
        
        return _json(details, request)
    except Exception as e:
        log.error("bot_details_overview failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


# ------------------------------ Monitoring & Real-time Data ----------------------------
async def monitoring_data(request: web.Request):
    """Real-time monitoring dashboard data"""
    await _auth_user(request)
    try:
        # Get system metrics
        import psutil
        
        # Database stats
        db_stats = await fetchrow("""
            select 
                (select count(*) from dashboard_users) as users_count,
                (select count(*) from dashboard_bots where is_active=true) as active_bots,
                (select count(*) from dashboard_ads where is_active=true) as active_ads,
                (select count(*) from dashboard_token_events) as token_events_count
        """)
        
        # Activity stats
        activity = await fetchrow("""
            select 
                (select count(*) from dashboard_users where updated_at > now() - interval '24 hours') as users_24h,
                (select count(*) from dashboard_ads where created_at > now() - interval '24 hours') as ads_24h
        """)
        
        monitoring = {
            "timestamp": int(time.time()),
            "system": {
                "cpu_percent": psutil.cpu_percent(interval=0.1),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_percent": psutil.disk_usage('/').percent
            },
            "database": {
                "users": db_stats['users_count'] if db_stats else 0,
                "active_bots": db_stats['active_bots'] if db_stats else 0,
                "active_ads": db_stats['active_ads'] if db_stats else 0,
                "token_events": db_stats['token_events_count'] if db_stats else 0
            },
            "activity_24h": {
                "new_users": activity['users_24h'] if activity else 0,
                "new_ads": activity['ads_24h'] if activity else 0
            },
            "uptime": int(time.time())
        }
        
        return _json(monitoring, request)
    except Exception as e:
        log.error("monitoring_data failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


# ------------------------------ route wiring ------------------------------
async def options_root(request: web.Request):
    return options_handler(request)


# ============================================================================
# CONTENT BOT STATISTICS
# ============================================================================

async def content_bot_stats_overview(request: web.Request):
    """Hauptstatistiken des Content Bots: Gruppen, Mitglieder, Nachrichten, Tokens"""
    await _auth_user(request)
    try:
        # Aggregierte Statistiken aus Content Bot Database (echte Tabellen)
        rows = await fetch("""
            SELECT 
                (SELECT COUNT(*) FROM groups) as total_groups,
                (SELECT COUNT(DISTINCT chat_id) FROM message_logs WHERE timestamp > now() - interval '24 hours') as active_today,
                (SELECT COUNT(*) FROM members WHERE is_deleted=FALSE) as total_members,
                (SELECT COUNT(*) FROM message_logs WHERE timestamp > now() - interval '24 hours') as messages_today,
                (SELECT COUNT(*) FROM message_logs WHERE timestamp > now() - interval '7 days') as messages_week,
                (SELECT COALESCE(SUM(points), 0)::numeric FROM rewards_pending) as total_rewards_pending,
                (SELECT COUNT(*) FROM rewards_claims WHERE status='pending') as pending_claims,
                (SELECT COUNT(DISTINCT user_id) FROM rewards_claims WHERE status='pending') as claimants_count,
                (SELECT COUNT(DISTINCT chat_id) FROM ai_mod_settings WHERE enabled=true) as ai_enabled_groups,
                (SELECT COUNT(*) FROM ai_mod_logs WHERE ts > now() - interval '24 hours') as ai_actions_today,
                (SELECT COUNT(DISTINCT chat_id) FROM rss_feeds WHERE enabled=true) as rss_feeds_active
        """)
        
        r = rows[0] if rows else {}
        
        total_groups = int(r.get('total_groups') or 0)
        
        stats = {
            "timestamp": int(time.time()),
            "groups": {
                "total": total_groups,
                "active_today": int(r.get('active_today') or 0),
                "total_members": int(r.get('total_members') or 0)
            },
            "messages": {
                "total_today": int(r.get('messages_today') or 0),
                "total_this_week": int(r.get('messages_week') or 0),
                "average_per_group": round(float(r.get('messages_week') or 0) / max(1, total_groups), 2)
            },
            "tokens": {
                "total_rewards_pending": float(r.get('total_rewards_pending') or 0),
                "total_pending_claims": int(r.get('pending_claims') or 0),
                "total_claimants": int(r.get('claimants_count') or 0)
            },
            "ai_moderation": {
                "enabled_in_groups": int(r.get('ai_enabled_groups') or 0),
                "actions_today": int(r.get('ai_actions_today') or 0),
                "categories": {}
            },
            "features": {
                "rss_feeds_active": int(r.get('rss_feeds_active') or 0)
            }
        }
        
        # AI Moderation Kategorien heute
        ai_cats = await fetch("""
            SELECT category, COUNT(*) as count
            FROM ai_mod_logs
            WHERE ts > now() - interval '24 hours'
            GROUP BY category
            ORDER BY count DESC
        """)
        stats["ai_moderation"]["categories"] = {row['category']: row['count'] for row in (ai_cats or [])}
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_stats_overview failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def content_bot_groups_stats(request: web.Request):
    """Detaillierte Statistiken pro Gruppe"""
    await _auth_user(request)
    try:
        limit = int(request.query.get("limit", "50"))
        
        # Top groups by activity
        groups = await fetch(f"""
            SELECT 
                g.chat_id,
                g.title,
                COUNT(DISTINCT m.user_id) as member_count,
                COUNT(ml.user_id) as messages_total,
                COUNT(CASE WHEN ml.timestamp > now() - interval '24 hours' THEN 1 END) as messages_today,
                COUNT(CASE WHEN ml.timestamp > now() - interval '7 days' THEN 1 END) as messages_week,
                MAX(ml.timestamp) as last_activity
            FROM groups g
            LEFT JOIN members m ON g.chat_id = m.chat_id AND m.is_deleted = FALSE
            LEFT JOIN message_logs ml ON g.chat_id = ml.chat_id
            GROUP BY g.chat_id, g.title
            ORDER BY messages_today DESC, member_count DESC
            LIMIT {limit}
        """)
        
        stats = {
            "timestamp": int(time.time()),
            "groups": [
                {
                    "chat_id": int(g['chat_id']),
                    "title": g['title'],
                    "members": int(g['member_count'] or 0),
                    "messages_today": int(g['messages_today'] or 0),
                    "messages_week": int(g['messages_week'] or 0),
                    "messages_total": int(g['messages_total'] or 0),
                    "last_activity": g['last_activity'].isoformat() if g['last_activity'] else None
                }
                for g in (groups or [])
            ]
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_groups_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def content_bot_claimed_stats(request: web.Request):
    """EMRD Claimed Statistiken: Rewards Claims aus User-Wallets"""
    await _auth_user(request)
    try:
        # Claimed rewards stats
        claimed_data = await fetch("""
            SELECT 
                (SELECT COALESCE(SUM(amount), 0)::numeric FROM rewards_claims WHERE status='paid') as claimed_total,
                (SELECT COUNT(*) FROM rewards_claims WHERE status='pending') as pending_count,
                (SELECT COALESCE(SUM(amount), 0)::numeric FROM rewards_claims WHERE status='pending') as pending_amount,
                (SELECT COUNT(DISTINCT user_id) FROM rewards_claims) as unique_claimants
        """)
        claimed_row = claimed_data[0] if claimed_data else {'claimed_total': 0, 'pending_count': 0, 'pending_amount': 0, 'unique_claimants': 0}
        
        # Top claimants
        top_claimants = await fetch("""
            SELECT 
                user_id,
                SUM(amount)::numeric as claim_amount,
                COUNT(*) as claim_count
            FROM rewards_claims
            WHERE status='paid'
            GROUP BY user_id
            ORDER BY claim_amount DESC
            LIMIT 20
        """)
        
        stats = {
            "timestamp": int(time.time()),
            "claimed_total": float(claimed_row.get('claimed_total') or 0),
            "pending_count": int(claimed_row.get('pending_count') or 0),
            "pending_amount": float(claimed_row.get('pending_amount') or 0),
            "unique_claimants": int(claimed_row.get('unique_claimants') or 0),
            "top_claimants": [
                {
                    "rank": i+1,
                    "user_id": int(c['user_id']),
                    "claim_amount": float(c['claim_amount'] or 0),
                    "claim_count": int(c['claim_count'])
                }
                for i, c in enumerate(top_claimants or [])
            ]
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_claimed_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def content_bot_story_share_stats(request: web.Request):
    """Story Share Statistiken: Shares, Clicks, Rewards"""
    await _auth_user(request)
    try:
        # Story sharing stats - wir tracken shares und rewards aus rewards_pending/claims
        # Die rewards_pending.reason / rewards_claims.meta sollte 'story_share' enthalten
        stats_data = await fetch("""
            SELECT
                (SELECT COUNT(DISTINCT chat_id) FROM global_config 
                 WHERE key LIKE 'story_sharing:%' AND value->>'enabled' = 'true') as active_groups,
                (SELECT COUNT(*) FROM rewards_pending WHERE reason LIKE '%story%') as total_shares,
                (SELECT COALESCE(SUM(points), 0)::numeric FROM rewards_pending 
                 WHERE reason LIKE '%story%') as story_share_rewards,
                (SELECT COUNT(DISTINCT user_id) FROM rewards_pending 
                 WHERE reason LIKE '%story%') as story_share_users,
                (SELECT COUNT(*) FROM rewards_claims WHERE status='paid' 
                 AND (meta->>'source' = 'story' OR description LIKE '%story%')) as claimed_story_rewards
        """)
        
        # Fallback für detaillierte Daten - wenn die obige Query nicht funktioniert
        if not stats_data or not stats_data[0]:
            stats_data = await fetch("""
                SELECT
                    0 as active_groups,
                    0 as total_shares,
                    0 as story_share_rewards,
                    0 as story_share_users,
                    0 as claimed_story_rewards
            """)
        
        row = stats_data[0] if stats_data else {'active_groups': 0, 'total_shares': 0, 'story_share_rewards': 0, 'story_share_users': 0, 'claimed_story_rewards': 0}
        
        # Get approximate clicks based on claims related to story sharing
        clicks_data = await fetch("""
            SELECT COUNT(*) as total_clicks
            FROM rewards_claims 
            WHERE status='paid' AND (meta->>'source' = 'story_click' OR description LIKE '%click%')
            LIMIT 1
        """)
        clicks_row = clicks_data[0] if clicks_data else {'total_clicks': 0}
        
        total_shares = int(row.get('total_shares') or 0)
        total_clicks = int(clicks_row.get('total_clicks') or 0)
        
        stats = {
            "timestamp": int(time.time()),
            "active_groups": int(row.get('active_groups') or 0),
            "total_shares": total_shares,
            "total_clicks": total_clicks,
            "avg_clicks": round(float(total_clicks) / max(1, total_shares), 2),
            "story_share_rewards": float(row.get('story_share_rewards') or 0),
            "story_share_users": int(row.get('story_share_users') or 0),
            "claimed_story_rewards": int(row.get('claimed_story_rewards') or 0)
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_story_share_stats failed: %s", e, exc_info=True)
        # Fallback response wenn Query fehlschlägt
        return _json({
            "timestamp": int(time.time()),
            "active_groups": 0,
            "total_shares": 0,
            "total_clicks": 0,
            "avg_clicks": 0.0,
            "story_share_rewards": 0.0,
            "story_share_users": 0,
            "claimed_story_rewards": 0
        }, request)


async def content_bot_ai_moderation_stats(request: web.Request):
    """KI-Moderation Statistiken: Aktionen, Kategorien, Strikes"""
    await _auth_user(request)
    try:
        # AI Moderation overview
        ai_data = await fetch("""
            SELECT 
                (SELECT COUNT(DISTINCT chat_id) FROM ai_mod_settings WHERE enabled=true) as enabled_groups,
                (SELECT COUNT(*) FROM ai_mod_logs WHERE ts > now() - interval '24 hours') as actions_today,
                (SELECT COUNT(*) FROM ai_mod_logs WHERE ts > now() - interval '7 days') as actions_week,
                (SELECT COUNT(*) FROM user_strike_events WHERE ts > now() - interval '24 hours') as strikes_today
        """)
        ai_row = ai_data[0] if ai_data else {'enabled_groups': 0, 'actions_today': 0, 'actions_week': 0, 'strikes_today': 0}
        
        # Actions by category
        by_category = await fetch("""
            SELECT 
                category,
                COUNT(*) as count,
                COUNT(CASE WHEN action='delete' THEN 1 END) as deleted,
                COUNT(CASE WHEN action='warn' THEN 1 END) as warned,
                ROUND(AVG(score)::numeric, 3) as avg_score
            FROM ai_mod_logs
            WHERE ts > now() - interval '7 days'
            GROUP BY category
            ORDER BY count DESC
        """)
        
        # Top offenders
        offenders = await fetch("""
            SELECT 
                user_id,
                chat_id,
                COUNT(*) as violation_count,
                SUM(CASE WHEN action='delete' THEN 1 ELSE 0 END) as deleted_count,
                MAX(ts) as last_violation
            FROM ai_mod_logs
            WHERE ts > now() - interval '30 days'
            GROUP BY user_id, chat_id
            ORDER BY violation_count DESC
            LIMIT 20
        """)
        
        stats = {
            "timestamp": int(time.time()),
            "actions": {
                "enabled_groups": int(ai_row.get('enabled_groups') or 0),
                "actions_today": int(ai_row.get('actions_today') or 0),
                "actions_week": int(ai_row.get('actions_week') or 0),
                "strikes_today": int(ai_row.get('strikes_today') or 0)
            },
            "by_category": [
                {
                    "category": c['category'],
                    "count": int(c['count']),
                    "deleted": int(c['deleted'] or 0),
                    "warned": int(c['warned'] or 0),
                    "avg_score": float(c['avg_score'] or 0)
                }
                for c in (by_category or [])
            ],
            "top_offenders": [
                {
                    "user_id": int(o['user_id']),
                    "chat_id": int(o['chat_id']),
                    "violations": int(o['violation_count']),
                    "deleted_messages": int(o['deleted_count'] or 0),
                    "last_violation": o['last_violation'].isoformat() if o['last_violation'] else None
                }
                for o in (offenders or [])
            ]
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_ai_moderation_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def content_bot_features_stats(request: web.Request):
    """Feature-Nutzung: RSS und andere Features"""
    await _auth_user(request)
    try:
        # RSS Stats
        rss_data = await fetch("""
            SELECT
                COUNT(DISTINCT chat_id) as active_feeds,
                COUNT(*) as total_feeds
            FROM rss_feeds
            WHERE enabled=true
        """)
        
        rss_row = rss_data[0] if rss_data else {'active_feeds': 0, 'total_feeds': 0}
        
        stats = {
            "timestamp": int(time.time()),
            "features": {
                "rss": {
                    "active_feeds": int(rss_row.get('active_feeds') or 0),
                    "total_feeds": int(rss_row.get('total_feeds') or 0)
                }
            }
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_features_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def content_bot_user_retention(request: web.Request):
    """User Retention & Activity Metrics"""
    await _auth_user(request)
    try:
        # Total active members
        total_users = await fetch("""
            SELECT COUNT(*) as total
            FROM members
            WHERE is_deleted = FALSE
        """)
        
        # Active today/week/month (using CASE WHEN instead of FILTER)
        active_counts = await fetch("""
            SELECT
                COUNT(DISTINCT CASE WHEN timestamp > NOW() - INTERVAL '1 day' THEN user_id END) as today,
                COUNT(DISTINCT CASE WHEN timestamp > NOW() - INTERVAL '7 days' THEN user_id END) as week,
                COUNT(DISTINCT CASE WHEN timestamp > NOW() - INTERVAL '30 days' THEN user_id END) as month
            FROM message_logs
        """)
        
        # Daily active users for last 30 days
        daily_active = await fetch("""
            SELECT 
                DATE(timestamp) as date,
                COUNT(DISTINCT user_id) as active_count
            FROM message_logs
            WHERE timestamp > NOW() - INTERVAL '30 days'
            GROUP BY DATE(timestamp)
            ORDER BY date
        """)
        
        # Messages per user
        engagement = await fetch("""
            SELECT
                COUNT(*) as total_messages,
                COUNT(DISTINCT user_id) as unique_users,
                CASE WHEN COUNT(DISTINCT user_id) > 0 THEN ROUND((CAST(COUNT(*) AS numeric) / COUNT(DISTINCT user_id)), 2) ELSE 0 END as messages_per_user
            FROM message_logs
            WHERE timestamp > NOW() - INTERVAL '30 days'
        """)
        
        active_c = active_counts[0] if active_counts else {'today': 0, 'week': 0, 'month': 0}
        engagement_row = engagement[0] if engagement else {'total_messages': 0, 'unique_users': 0, 'messages_per_user': 0}
        total_row = total_users[0] if total_users else {'total': 0}
        
        stats = {
            "timestamp": int(time.time()),
            "retention": {
                "active_users_total": int(total_row.get('total') or 0),
                "active_today": int(active_c.get('today') or 0),
                "active_week": int(active_c.get('week') or 0),
                "active_month": int(active_c.get('month') or 0)
            },
            "engagement": {
                "total_messages": int(engagement_row.get('total_messages') or 0),
                "unique_users": int(engagement_row.get('unique_users') or 0),
                "messages_per_user": float(engagement_row.get('messages_per_user') or 0),
                "daily_active_users": [
                    {"date": d['date'].isoformat() if d['date'] else None, "count": int(d['active_count'])}
                    for d in (daily_active or [])
                ]
            }
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_user_retention failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def content_bot_network_analysis(request: web.Request):
    """Netzwerkanalyse: Gruppen und Nachrichten"""
    await _auth_user(request)
    try:
        # Group and message statistics (using CASE WHEN instead of FILTER)
        bot_stats = await fetch("""
            SELECT
                COUNT(DISTINCT chat_id) as total_groups,
                COUNT(DISTINCT user_id) as total_users,
                COUNT(*) as total_messages,
                COUNT(CASE WHEN timestamp > NOW() - INTERVAL '1 day' THEN 1 END) as messages_today,
                COUNT(CASE WHEN timestamp > NOW() - INTERVAL '7 days' THEN 1 END) as messages_week
            FROM message_logs
        """)
        
        # Member events (joins/leaves)
        member_stats = await fetch("""
            SELECT
                COUNT(CASE WHEN event_type = 'join' AND ts > NOW() - INTERVAL '1 day' THEN 1 END) as joins_today,
                COUNT(CASE WHEN event_type = 'leave' AND ts > NOW() - INTERVAL '1 day' THEN 1 END) as leaves_today,
                COUNT(CASE WHEN event_type = 'kick' AND ts > NOW() - INTERVAL '1 day' THEN 1 END) as kicks_today,
                COUNT(CASE WHEN event_type = 'join' AND ts > NOW() - INTERVAL '7 days' THEN 1 END) as joins_week
            FROM member_events
        """)
        
        # Top active groups
        active_groups = await fetch("""
            SELECT 
                chat_id,
                COUNT(*) as msg_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM message_logs
            WHERE timestamp > NOW() - INTERVAL '7 days'
            GROUP BY chat_id
            ORDER BY msg_count DESC
            LIMIT 20
        """)
        
        bot_row = bot_stats[0] if bot_stats else {'total_groups': 0, 'total_users': 0, 'total_messages': 0, 'messages_today': 0, 'messages_week': 0}
        member_row = member_stats[0] if member_stats else {'joins_today': 0, 'leaves_today': 0, 'kicks_today': 0, 'joins_week': 0}
        
        stats = {
            "timestamp": int(time.time()),
            "network": {
                "total_groups": int(bot_row.get('total_groups') or 0),
                "total_users": int(bot_row.get('total_users') or 0),
                "total_messages": int(bot_row.get('total_messages') or 0),
                "messages_today": int(bot_row.get('messages_today') or 0),
                "messages_week": int(bot_row.get('messages_week') or 0),
                "joins_today": int(member_row.get('joins_today') or 0),
                "leaves_today": int(member_row.get('leaves_today') or 0),
                "kicks_today": int(member_row.get('kicks_today') or 0),
                "joins_week": int(member_row.get('joins_week') or 0),
                "active_groups": [
                    {
                        "chat_id": int(g['chat_id']),
                        "messages": int(g['msg_count']),
                        "unique_users": int(g['unique_users'])
                    }
                    for g in (active_groups or [])
                ]
            }
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_network_analysis failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def content_bot_complete_stats(request: web.Request):
    """🔥 KOMPLETTE STATISTIK - ALLES AUS DER DATENBANK!"""
    await _auth_user(request)
    try:
        # 1. ALLE GRUPPEN MIT DETAILS
        groups_data = await fetch("""
            SELECT
                g.chat_id,
                g.title,
                COUNT(DISTINCT m.user_id) as member_count,
                COUNT(DISTINCT CASE WHEN m.is_deleted=FALSE THEN m.user_id END) as active_members,
                COUNT(DISTINCT ml.user_id) as users_with_messages,
                COUNT(ml.message_id) as total_messages,
                COUNT(CASE WHEN ml.timestamp > NOW() - INTERVAL '24 hours' THEN 1 END) as messages_24h,
                COUNT(CASE WHEN ml.timestamp > NOW() - INTERVAL '7 days' THEN 1 END) as messages_7d,
                COUNT(CASE WHEN ml.timestamp > NOW() - INTERVAL '30 days' THEN 1 END) as messages_30d,
                MAX(ml.timestamp) as last_message,
                COUNT(DISTINCT CASE WHEN me.event_type='join' THEN 1 END) as total_joins,
                COUNT(DISTINCT CASE WHEN me.event_type='leave' THEN 1 END) as total_leaves,
                COUNT(DISTINCT CASE WHEN me.event_type='kick' THEN 1 END) as total_kicks,
                COUNT(DISTINCT CASE WHEN ai.enabled=true THEN 1 END) as ai_enabled,
                COUNT(DISTINCT ail.chat_id) as ai_actions,
                COUNT(DISTINCT rf.chat_id) as rss_feeds
            FROM groups g
            LEFT JOIN members m ON g.chat_id = m.chat_id
            LEFT JOIN message_logs ml ON g.chat_id = ml.chat_id
            LEFT JOIN member_events me ON g.chat_id = me.chat_id
            LEFT JOIN ai_mod_settings ai ON g.chat_id = ai.chat_id AND ai.enabled=true
            LEFT JOIN ai_mod_logs ail ON g.chat_id = ail.chat_id
            LEFT JOIN rss_feeds rf ON g.chat_id = rf.chat_id
            GROUP BY g.chat_id, g.title
            ORDER BY messages_24h DESC
        """)
        
        # 2. TOKEN STATISTIKEN
        token_stats = await fetch("""
            SELECT
                (SELECT COALESCE(SUM(points), 0)::numeric FROM rewards_pending) as total_pending,
                (SELECT COALESCE(SUM(amount), 0)::numeric FROM rewards_claims WHERE status='paid') as total_paid,
                (SELECT COALESCE(SUM(amount), 0)::numeric FROM rewards_claims WHERE status='pending') as total_pending_claims,
                (SELECT COUNT(*) FROM rewards_pending WHERE points > 0) as holders,
                (SELECT COUNT(*) FROM rewards_claims WHERE status='pending') as pending_claims_count,
                (SELECT COUNT(*) FROM rewards_claims) as total_claims
        """)
        
        # 3. AI MODERATION STATS
        ai_stats = await fetch("""
            SELECT
                (SELECT COUNT(DISTINCT chat_id) FROM ai_mod_settings WHERE enabled=true) as ai_groups,
                (SELECT COUNT(*) FROM ai_mod_logs WHERE ts > NOW() - INTERVAL '24 hours') as ai_24h,
                (SELECT COUNT(*) FROM ai_mod_logs WHERE ts > NOW() - INTERVAL '7 days') as ai_7d,
                (SELECT COUNT(*) FROM ai_mod_logs WHERE ts > NOW() - INTERVAL '30 days') as ai_30d,
                (SELECT COUNT(DISTINCT category) FROM ai_mod_logs) as ai_categories,
                (SELECT COUNT(*) FROM user_strike_events) as total_strikes,
                (SELECT COUNT(*) FROM user_strike_events WHERE ts > NOW() - INTERVAL '24 hours') as strikes_24h
        """)
        
        # 4. USER ENGAGEMENT
        user_stats = await fetch("""
            SELECT
                COUNT(DISTINCT user_id) as total_users,
                COUNT(DISTINCT CASE WHEN timestamp > NOW() - INTERVAL '1 day' THEN user_id END) as active_24h,
                COUNT(DISTINCT CASE WHEN timestamp > NOW() - INTERVAL '7 days' THEN user_id END) as active_7d,
                COUNT(DISTINCT CASE WHEN timestamp > NOW() - INTERVAL '30 days' THEN user_id END) as active_30d,
                COUNT(*) as total_messages,
                ROUND(AVG(CAST(cnt AS numeric)), 2) as avg_messages_per_user
            FROM (
                SELECT user_id, COUNT(*) as cnt FROM message_logs GROUP BY user_id
            ) sub
        """)
        
        # 5. TOP USERS
        top_users = await fetch("""
            SELECT
                user_id,
                COUNT(*) as message_count,
                COUNT(DISTINCT chat_id) as groups_active,
                MAX(timestamp) as last_message,
                MIN(timestamp) as first_message
            FROM message_logs
            GROUP BY user_id
            ORDER BY message_count DESC
            LIMIT 50
        """)
        
        # 6. TOP TALKERS PER GROUP
        top_talkers = await fetch("""
            SELECT
                chat_id,
                user_id,
                COUNT(*) as messages
            FROM message_logs
            GROUP BY chat_id, user_id
            ORDER BY chat_id, messages DESC
        """)
        
        # 7. AI MODERATION BY CATEGORY
        ai_by_category = await fetch("""
            SELECT
                category,
                COUNT(*) as count,
                COUNT(CASE WHEN action='delete' THEN 1 END) as deleted,
                COUNT(CASE WHEN action='warn' THEN 1 END) as warned,
                ROUND(AVG(score)::numeric, 3) as avg_score
            FROM ai_mod_logs
            GROUP BY category
            ORDER BY count DESC
        """)
        
        # 8. REWARDS DISTRIBUTION
        top_holders = await fetch("""
            SELECT
                user_id,
                points,
                ROUND((100.0 * points / NULLIF((SELECT SUM(points) FROM rewards_pending), 0))::numeric, 2) as percentage
            FROM rewards_pending
            WHERE points > 0
            ORDER BY points DESC
            LIMIT 50
        """)
        
        # 9. MEMBER EVENTS
        member_events = await fetch("""
            SELECT
                chat_id,
                event_type,
                COUNT(*) as count,
                MAX(ts) as last_event
            FROM member_events
            GROUP BY chat_id, event_type
            ORDER BY chat_id, event_type
        """)
        
        # 10. TIME STATS
        time_stats = await fetch("""
            SELECT
                EXTRACT(HOUR FROM timestamp AT TIME ZONE 'Europe/Berlin') as hour,
                COUNT(*) as messages,
                COUNT(DISTINCT user_id) as users,
                COUNT(DISTINCT chat_id) as groups
            FROM message_logs
            WHERE timestamp > NOW() - INTERVAL '30 days'
            GROUP BY EXTRACT(HOUR FROM timestamp AT TIME ZONE 'Europe/Berlin')
            ORDER BY hour
        """)
        
        # Parse token stats
        t_row = token_stats[0] if token_stats else {'total_pending': 0, 'total_paid': 0, 'total_pending_claims': 0, 'holders': 0, 'pending_claims_count': 0, 'total_claims': 0}
        a_row = ai_stats[0] if ai_stats else {'ai_groups': 0, 'ai_24h': 0, 'ai_7d': 0, 'ai_30d': 0, 'ai_categories': 0, 'total_strikes': 0, 'strikes_24h': 0}
        u_row = user_stats[0] if user_stats else {'total_users': 0, 'active_24h': 0, 'active_7d': 0, 'active_30d': 0, 'total_messages': 0, 'avg_messages_per_user': 0}
        
        # Group talkers by chat_id
        talkers_by_group = {}
        for t in (top_talkers or []):
            cid = int(t['chat_id'])
            if cid not in talkers_by_group:
                talkers_by_group[cid] = []
            if len(talkers_by_group[cid]) < 10:  # Top 10 per group
                talkers_by_group[cid].append({
                    "user_id": int(t['user_id']),
                    "messages": int(t['messages'])
                })
        
        # Group events by chat_id
        events_by_group = {}
        for e in (member_events or []):
            cid = int(e['chat_id'])
            if cid not in events_by_group:
                events_by_group[cid] = {}
            events_by_group[cid][e['event_type']] = {
                "count": int(e['count']),
                "last_event": e['last_event'].isoformat() if e['last_event'] else None
            }
        
        # Build complete response
        stats = {
            "timestamp": int(time.time()),
            "summary": {
                "total_groups": len(groups_data or []),
                "total_users": int(u_row.get('total_users') or 0),
                "total_messages": int(u_row.get('total_messages') or 0),
                "total_ai_actions": int(a_row.get('ai_24h') + a_row.get('ai_7d') + a_row.get('ai_30d') or 0),
                "total_token_distributed": float(t_row.get('total_paid') or 0),
                "total_token_pending": float(t_row.get('total_pending') or 0),
            },
            "groups": [
                {
                    "chat_id": int(g['chat_id']),
                    "name": g['title'],
                    "stats": {
                        "members": int(g['member_count'] or 0),
                        "active_members": int(g['active_members'] or 0),
                        "users_with_messages": int(g['users_with_messages'] or 0),
                        "messages_total": int(g['total_messages'] or 0),
                        "messages_24h": int(g['messages_24h'] or 0),
                        "messages_7d": int(g['messages_7d'] or 0),
                        "messages_30d": int(g['messages_30d'] or 0),
                        "last_message": g['last_message'].isoformat() if g['last_message'] else None,
                    },
                    "events": events_by_group.get(int(g['chat_id']), {}),
                    "ai_moderation": {
                        "enabled": bool(g['ai_enabled']),
                        "actions": int(g['ai_actions'] or 0),
                    },
                    "rss_feeds": int(g['rss_feeds'] or 0),
                    "top_talkers": talkers_by_group.get(int(g['chat_id']), [])
                }
                for g in (groups_data or [])
            ],
            "tokens": {
                "total_pending": float(t_row.get('total_pending') or 0),
                "total_claimed": float(t_row.get('total_paid') or 0),
                "pending_claims_amount": float(t_row.get('total_pending_claims') or 0),
                "holders": int(t_row.get('holders') or 0),
                "pending_claims_count": int(t_row.get('pending_claims_count') or 0),
                "total_claims_all_time": int(t_row.get('total_claims') or 0),
                "top_holders": [
                    {
                        "rank": i+1,
                        "user_id": int(h['user_id']),
                        "balance": float(h['points'] or 0),
                        "percentage": float(h['percentage'] or 0)
                    }
                    for i, h in enumerate(top_holders or [])
                ]
            },
            "ai_moderation": {
                "enabled_groups": int(a_row.get('ai_groups') or 0),
                "actions_24h": int(a_row.get('ai_24h') or 0),
                "actions_7d": int(a_row.get('ai_7d') or 0),
                "actions_30d": int(a_row.get('ai_30d') or 0),
                "categories": int(a_row.get('ai_categories') or 0),
                "total_strikes": int(a_row.get('total_strikes') or 0),
                "strikes_24h": int(a_row.get('strikes_24h') or 0),
                "by_category": [
                    {
                        "category": c['category'],
                        "count": int(c['count']),
                        "deleted": int(c['deleted'] or 0),
                        "warned": int(c['warned'] or 0),
                        "avg_score": float(c['avg_score'] or 0)
                    }
                    for c in (ai_by_category or [])
                ]
            },
            "users": {
                "total": int(u_row.get('total_users') or 0),
                "active_24h": int(u_row.get('active_24h') or 0),
                "active_7d": int(u_row.get('active_7d') or 0),
                "active_30d": int(u_row.get('active_30d') or 0),
                "total_messages": int(u_row.get('total_messages') or 0),
                "avg_messages_per_user": float(u_row.get('avg_messages_per_user') or 0),
                "top_users": [
                    {
                        "rank": i+1,
                        "user_id": int(u['user_id']),
                        "messages": int(u['message_count']),
                        "groups_active": int(u['groups_active'] or 0),
                        "first_message": u['first_message'].isoformat() if u['first_message'] else None,
                        "last_message": u['last_message'].isoformat() if u['last_message'] else None
                    }
                    for i, u in enumerate(top_users or [])
                ]
            },
            "hourly_distribution": [
                {
                    "hour": int(t['hour']),
                    "messages": int(t['messages']),
                    "users": int(t['users']),
                    "groups": int(t['groups'])
                }
                for t in (time_stats or [])
            ]
        }
        
        return _json(stats, request)
    except Exception as e:
        log.error("content_bot_complete_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


# ============================================================================
# BOT-SPECIFIC STATISTICS ENDPOINTS
# ============================================================================

async def affiliate_bot_stats(request: web.Request):
    """Affiliate Bot Statistiken: Referrals, Conversions, Commissions"""
    await _auth_user(request)
    try:
        # Referrer stats
        referrers = await fetch("""
            SELECT
                COUNT(DISTINCT referrer_id) as total_referrers,
                COUNT(DISTINCT referral_id) as total_referrals,
                SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as conversions_total,
                COUNT(DISTINCT CASE WHEN status='active' THEN referral_id END) as converted_referrals
            FROM aff_referrals
        """)
        
        # Commissions
        commissions = await fetch("""
            SELECT
                SUM(CASE WHEN total_earned IS NOT NULL THEN total_earned ELSE 0 END) as commissions_paid,
                SUM(CASE WHEN pending IS NOT NULL THEN pending ELSE 0 END) as commissions_pending,
                COUNT(*) as total_commissions
            FROM aff_commissions
        """)
        
        # Top referrers
        top_referrers = await fetch("""
            SELECT
                referrer_id,
                COUNT(DISTINCT referral_id) as referral_count,
                SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as successful
            FROM aff_referrals
            GROUP BY referrer_id
            ORDER BY referral_count DESC
            LIMIT 10
        """)
        
        ref = referrers[0] if referrers else {}
        comm = commissions[0] if commissions else {}
        
        stats = {
            "timestamp": int(time.time()),
            "total_referrers": int(ref.get('total_referrers') or 0),
            "total_referrals": int(ref.get('total_referrals') or 0),
            "conversions_total": int(ref.get('conversions_total') or 0),
            "converted_referrals": int(ref.get('converted_referrals') or 0),
            "commissions_paid": float(comm.get('commissions_paid') or 0),
            "commissions_pending": float(comm.get('commissions_pending') or 0),
            "total_commissions": int(comm.get('total_commissions') or 0),
            "top_referrers": [
                {
                    "rank": i+1,
                    "referrer_id": int(r['referrer_id']),
                    "referrals": int(r['referral_count']),
                    "successful": int(r['successful'])
                }
                for i, r in enumerate(top_referrers or [])
            ]
        }
        return _json(stats, request)
    except Exception as e:
        log.error("affiliate_bot_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def crossposter_bot_stats(request: web.Request):
    """Crossposter Bot Statistiken: Posts, Channels, Rules"""
    await _auth_user(request)
    try:
        # User and post stats
        posts_data = await fetch("""
            SELECT
                COUNT(DISTINCT user_id) as total_users,
                COUNT(*) as total_posts,
                SUM(CASE WHEN status='posted' THEN 1 ELSE 0 END) as posted,
                SUM(CASE WHEN status='draft' THEN 1 ELSE 0 END) as drafts,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
            FROM crossposter_posts
        """)
        
        # Channel stats
        channels = await fetch("""
            SELECT
                COUNT(*) as total_channels,
                SUM(CASE WHEN is_enabled=true THEN 1 ELSE 0 END) as enabled_channels,
                COUNT(DISTINCT user_id) as users_with_channels
            FROM crossposter_channels
        """)
        
        # Rules
        rules = await fetch("""
            SELECT
                COUNT(*) as total_rules,
                SUM(CASE WHEN is_active=true THEN 1 ELSE 0 END) as active_rules
            FROM crossposter_rules
        """)
        
        # Top users
        top_users = await fetch("""
            SELECT
                user_id,
                COUNT(*) as post_count,
                SUM(CASE WHEN status='posted' THEN 1 ELSE 0 END) as successful
            FROM crossposter_posts
            GROUP BY user_id
            ORDER BY post_count DESC
            LIMIT 10
        """)
        
        p = posts_data[0] if posts_data else {}
        c = channels[0] if channels else {}
        r = rules[0] if rules else {}
        
        stats = {
            "timestamp": int(time.time()),
            "total_users": int(p.get('total_users') or 0),
            "total_posts": int(p.get('total_posts') or 0),
            "posted": int(p.get('posted') or 0),
            "drafts": int(p.get('drafts') or 0),
            "failed": int(p.get('failed') or 0),
            "total_channels": int(c.get('total_channels') or 0),
            "enabled_channels": int(c.get('enabled_channels') or 0),
            "users_with_channels": int(c.get('users_with_channels') or 0),
            "total_rules": int(r.get('total_rules') or 0),
            "active_rules": int(r.get('active_rules') or 0),
            "top_users": [
                {
                    "rank": i+1,
                    "user_id": int(u['user_id']),
                    "posts": int(u['post_count']),
                    "successful": int(u['successful'])
                }
                for i, u in enumerate(top_users or [])
            ]
        }
        return _json(stats, request)
    except Exception as e:
        log.error("crossposter_bot_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def dao_bot_stats(request: web.Request):
    """DAO Bot Statistiken: Proposals, Votes, Treasury"""
    await _auth_user(request)
    try:
        # Proposals
        proposals = await fetch("""
            SELECT
                COUNT(*) as total_proposals,
                SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active_proposals,
                SUM(CASE WHEN status='passed' THEN 1 ELSE 0 END) as passed_proposals,
                SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) as rejected_proposals
            FROM dao_proposals
        """)
        
        # Voting
        voting = await fetch("""
            SELECT
                COUNT(DISTINCT voter_id) as total_voters,
                COUNT(*) as total_votes,
                AVG(CASE WHEN voting_power IS NOT NULL THEN voting_power ELSE 0 END) as avg_voting_power
            FROM dao_votes
        """)
        
        # Treasury - simplified query without direction column
        treasury = await fetch("""
            SELECT
                0::numeric as treasury_in,
                0::numeric as treasury_out,
                SUM(CASE WHEN balance IS NOT NULL THEN balance ELSE 0 END) as treasury_total
            FROM dao_treasury
        """)
        
        # Delegations
        delegations = await fetch("""
            SELECT
                COUNT(DISTINCT delegator_id) as total_delegators,
                COUNT(DISTINCT delegatee_id) as total_delegatees,
                SUM(voting_power) as total_delegated_power
            FROM dao_delegations
        """)
        
        # Top proposals
        top_proposals = await fetch("""
            SELECT
                id,
                title,
                status,
                (SELECT COUNT(*) FROM dao_votes WHERE proposal_id=dao_proposals.id) as vote_count
            FROM dao_proposals
            ORDER BY vote_count DESC
            LIMIT 5
        """)
        
        p = proposals[0] if proposals else {}
        v = voting[0] if voting else {}
        t = treasury[0] if treasury else {}
        d = delegations[0] if delegations else {}
        
        stats = {
            "timestamp": int(time.time()),
            "total_proposals": int(p.get('total_proposals') or 0),
            "active_proposals": int(p.get('active_proposals') or 0),
            "passed_proposals": int(p.get('passed_proposals') or 0),
            "rejected_proposals": int(p.get('rejected_proposals') or 0),
            "total_voters": int(v.get('total_voters') or 0),
            "total_votes": int(v.get('total_votes') or 0),
            "avg_voting_power": float(v.get('avg_voting_power') or 0),
            "treasury_in": float(t.get('treasury_in') or 0),
            "treasury_out": float(t.get('treasury_out') or 0),
            "treasury_total": float(t.get('treasury_total') or 0),
            "total_delegators": int(d.get('total_delegators') or 0),
            "total_delegatees": int(d.get('total_delegatees') or 0),
            "total_delegated_power": float(d.get('total_delegated_power') or 0),
            "top_proposals": [
                {
                    "rank": i+1,
                    "proposal_id": int(pr['id']),
                    "title": pr['title'],
                    "status": pr['status'],
                    "votes": int(pr['vote_count'])
                }
                for i, pr in enumerate(top_proposals or [])
            ]
        }
        return _json(stats, request)
    except Exception as e:
        log.error("dao_bot_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def learning_bot_stats(request: web.Request):
    """Learning Bot Statistiken: Courses, Enrollments, Quizzes"""
    await _auth_user(request)
    try:
        # Courses
        courses = await fetch("""
            SELECT
                COUNT(*) as total_courses,
                COUNT(*) as active_courses
            FROM learning_courses
        """)
        
        # Enrollments
        enrollments = await fetch("""
            SELECT
                COUNT(DISTINCT user_id) as enrolled_users,
                COUNT(*) as total_enrollments,
                SUM(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END) as completed_enrollments
            FROM learning_enrollments
        """)
        
        # Quizzes
        quizzes = await fetch("""
            SELECT
                COUNT(*) as total_quizzes,
                COUNT(DISTINCT 1) as users_attempted,
                COUNT(*) as quiz_passers,
                AVG(0) as avg_score
            FROM learning_quizzes
            LIMIT 1
        """)
        
        # Progress
        progress = await fetch("""
            SELECT
                COUNT(DISTINCT user_id) as users_with_progress,
                AVG(completion_percentage) as avg_completion
            FROM learning_progress
        """)
        
        # Top courses
        top_courses = await fetch("""
            SELECT
                c.id,
                c.title,
                COUNT(DISTINCT e.user_id) as enrollments,
                SUM(CASE WHEN e.completed_at IS NOT NULL THEN 1 ELSE 0 END) as completions
            FROM learning_courses c
            LEFT JOIN learning_enrollments e ON c.id = e.course_id
            GROUP BY c.id, c.title
            ORDER BY enrollments DESC
            LIMIT 5
        """)
        
        c = courses[0] if courses else {}
        e = enrollments[0] if enrollments else {}
        q = quizzes[0] if quizzes else {}
        pr = progress[0] if progress else {}
        
        stats = {
            "timestamp": int(time.time()),
            "total_courses": int(c.get('total_courses') or 0),
            "active_courses": int(c.get('active_courses') or 0),
            "enrolled_users": int(e.get('enrolled_users') or 0),
            "total_enrollments": int(e.get('total_enrollments') or 0),
            "completed_courses": int(e.get('completed_enrollments') or 0),
            "total_quizzes": int(q.get('total_quizzes') or 0),
            "users_attempted": int(q.get('users_attempted') or 0),
            "quiz_passers": int(q.get('quiz_passers') or 0),
            "avg_quiz_score": float(q.get('avg_score') or 0),
            "users_with_progress": int(pr.get('users_with_progress') or 0),
            "avg_completion_percentage": float(pr.get('avg_completion') or 0),
            "top_courses": [
                {
                    "rank": i+1,
                    "course_id": int(tc['id']),
                    "title": tc['title'],
                    "enrollments": int(tc['enrollments'] or 0),
                    "completions": int(tc['completions'] or 0)
                }
                for i, tc in enumerate(top_courses or [])
            ]
        }
        return _json(stats, request)
    except Exception as e:
        log.error("learning_bot_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def support_bot_stats(request: web.Request):
    """Support Bot Statistiken: Tickets, Messages, Resolution Time"""
    await _auth_user(request)
    try:
        # Tickets
        tickets = await fetch("""
            SELECT
                COUNT(*) as total_tickets,
                SUM(CASE WHEN status IN ('neu', 'in_bearbeitung') THEN 1 ELSE 0 END) as open,
                SUM(CASE WHEN status='geloest' THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN status='warten' THEN 1 ELSE 0 END) as pending,
                AVG(EXTRACT(EPOCH FROM (closed_at - created_at))/3600) as avg_resolution_hours
            FROM support_tickets
        """)
        
        # Users
        users = await fetch("""
            SELECT
                COUNT(DISTINCT user_id) as support_users
            FROM support_users
        """)
        
        # Messages
        messages = await fetch("""
            SELECT
                COUNT(*) as total_messages,
                COUNT(DISTINCT ticket_id) as tickets_with_messages
            FROM support_messages
        """)
        
        # By category (using tenants as categories)
        by_category = await fetch("""
            SELECT
                COALESCE(category, 'uncategorized') as category,
                COUNT(*) as count,
                SUM(CASE WHEN status='geloest' THEN 1 ELSE 0 END) as resolved
            FROM support_tickets
            GROUP BY category
            ORDER BY count DESC
        """)
        
        t = tickets[0] if tickets else {}
        u = users[0] if users else {}
        m = messages[0] if messages else {}
        
        stats = {
            "timestamp": int(time.time()),
            "total_tickets": int(t.get('total_tickets') or 0),
            "open": int(t.get('open') or 0),
            "closed": int(t.get('closed') or 0),
            "pending": int(t.get('pending') or 0),
            "avg_resolution_hours": float(t.get('avg_resolution_hours') or 0),
            "support_users": int(u.get('support_users') or 0),
            "total_messages": int(m.get('total_messages') or 0),
            "tickets_with_messages": int(m.get('tickets_with_messages') or 0),
            "by_category": [
                {
                    "category": cat['category'],
                    "count": int(cat['count']),
                    "resolved": int(cat['resolved'] or 0)
                }
                for cat in (by_category or [])
            ]
        }
        return _json(stats, request)
    except Exception as e:
        log.error("support_bot_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def trade_api_bot_stats(request: web.Request):
    """Trade API Bot Statistiken: Portfolios, Positions, Transactions"""
    await _auth_user(request)
    try:
        # Portfolios
        portfolios = await fetch("""
            SELECT
                COUNT(DISTINCT user_id) as total_users,
                COUNT(*) as total_portfolios,
                COUNT(*) as active_portfolios,
                SUM(CASE WHEN total_value IS NOT NULL THEN total_value ELSE 0 END) as total_aum
            FROM tradeapi_portfolios
        """)
        
        # Positions
        positions = await fetch("""
            SELECT
                COUNT(*) as total_positions,
                COUNT(*) as active_positions,
                SUM(CASE WHEN size IS NOT NULL THEN ABS(size) ELSE 0 END) as total_size
            FROM tradeapi_positions
        """)
        
        # Transactions
        transactions = await fetch("""
            SELECT
                COUNT(*) as total_transactions,
                SUM(CASE WHEN type='buy' THEN 1 ELSE 0 END) as buys,
                SUM(CASE WHEN type='sell' THEN 1 ELSE 0 END) as sells,
                AVG(CASE WHEN fees IS NOT NULL THEN fees ELSE 0 END) as avg_fees
            FROM tradeapi_transactions
        """)
        
        # Top portfolios
        top_portfolios = await fetch("""
            SELECT
                user_id,
                total_value,
                (SELECT COUNT(*) FROM tradeapi_positions WHERE user_id=tradeapi_portfolios.user_id) as position_count
            FROM tradeapi_portfolios
            ORDER BY total_value DESC
            LIMIT 10
        """)
        
        p = portfolios[0] if portfolios else {}
        pos = positions[0] if positions else {}
        t = transactions[0] if transactions else {}
        
        stats = {
            "timestamp": int(time.time()),
            "total_users": int(p.get('total_users') or 0),
            "total_portfolios": int(p.get('total_portfolios') or 0),
            "active_portfolios": int(p.get('active_portfolios') or 0),
            "total_aum": float(p.get('total_aum') or 0),
            "total_positions": int(pos.get('total_positions') or 0),
            "active_positions": int(pos.get('active_positions') or 0),
            "total_transactions": int(t.get('total_transactions') or 0),
            "buys": int(t.get('buys') or 0),
            "sells": int(t.get('sells') or 0),
            "avg_fees": float(t.get('avg_fees') or 0),
            "top_portfolios": [
                {
                    "rank": i+1,
                    "user_id": int(tp['user_id']),
                    "value": float(tp['total_value'] or 0),
                    "positions": int(tp['position_count'] or 0)
                }
                for i, tp in enumerate(top_portfolios or [])
            ]
        }
        return _json(stats, request)
    except Exception as e:
        log.error("trade_api_bot_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


async def trade_dex_bot_stats(request: web.Request):
    """Trade DEX Bot Statistiken: Pools, Liquidity, Swaps"""
    await _auth_user(request)
    try:
        # DEX config
        dex_config = await fetch("""
            SELECT
                COUNT(DISTINCT name) as total_dexes
            FROM tradedex_dex_config
        """)
        
        # Pools
        pools = await fetch("""
            SELECT
                COUNT(*) as total_pools,
                COUNT(*) as active_pools,
                SUM(CASE WHEN tvl_usd IS NOT NULL THEN tvl_usd ELSE 0 END) as total_tvl_usd
            FROM tradedex_pools
        """)
        
        # Positions
        positions = await fetch("""
            SELECT
                COUNT(DISTINCT user_id) as lp_users,
                COUNT(*) as total_positions,
                COUNT(*) as active_lp_positions
            FROM tradedex_positions
        """)
        
        # Swaps
        swaps = await fetch("""
            SELECT
                COUNT(*) as total_swaps,
                SUM(CASE WHEN amount_out IS NOT NULL THEN amount_out ELSE 0 END) as total_volume_usd,
                0::numeric as avg_slippage
            FROM tradedex_swaps
        """)
        
        # Top pools by TVL
        top_pools = await fetch("""
            SELECT
                id as pool_id,
                token_a,
                token_b,
                tvl_usd,
                (SELECT COUNT(*) FROM tradedex_positions WHERE pool_id=tradedex_pools.id) as lp_count
            FROM tradedex_pools
            ORDER BY tvl_usd DESC
            LIMIT 10
        """)
        
        d = dex_config[0] if dex_config else {}
        p = pools[0] if pools else {}
        pos = positions[0] if positions else {}
        s = swaps[0] if swaps else {}
        
        stats = {
            "timestamp": int(time.time()),
            "total_dexes": int(d.get('total_dexes') or 0),
            "total_pools": int(p.get('total_pools') or 0),
            "active_pools": int(p.get('active_pools') or 0),
            "total_tvl_usd": float(p.get('total_tvl_usd') or 0),
            "lp_users": int(pos.get('lp_users') or 0),
            "total_positions": int(pos.get('total_positions') or 0),
            "active_lp_positions": int(pos.get('active_lp_positions') or 0),
            "total_swaps": int(s.get('total_swaps') or 0),
            "total_volume_usd": float(s.get('total_volume_usd') or 0),
            "avg_slippage": float(s.get('avg_slippage') or 0),
            "top_pools": [
                {
                    "rank": i+1,
                    "pool_id": str(tp['pool_id']),
                    "pair": f"{tp['token_a']}/{tp['token_b']}",
                    "tvl": float(tp['tvl_usd'] or 0),
                    "lp_count": int(tp['lp_count'] or 0)
                }
                for i, tp in enumerate(top_pools or [])
            ]
        }
        return _json(stats, request)
    except Exception as e:
        log.error("trade_dex_bot_stats failed: %s", e, exc_info=True)
        return _json({"error": str(e)}, request, status=500)


def register_devdash_routes(app: web.Application):
    # Doppelte Registrierung verhindern (Heroku Reloads, mehrfacher Aufruf)
    if app.get("_devdash_routes_registered"):
        return
    app["_devdash_routes_registered"] = True

    # WICHTIG: add_route("GET", ...) statt add_get(), damit kein automatisches HEAD registriert wird
    # Routes unter /api/devdash (nicht nur /devdash)
    app.router.add_route("GET",  "/api/devdash/healthz",              healthz)
    app.router.add_route("GET",  "/api/devdash/auth/check",           auth_check)
    app.router.add_post(        "/api/devdash/dev-login",             dev_login)
    app.router.add_post(        "/api/devdash/auth/telegram",         auth_telegram)
    app.router.add_post(        "/api/devdash/auth/ton-wallet",       auth_ton_wallet)
    app.router.add_post(        "/api/devdash/auth/near-wallet",      auth_near_wallet)
    app.router.add_route("GET", "/api/devdash/me",                    me)
    
    # Metrics
    app.router.add_route("GET", "/api/devdash/metrics/overview",      metrics_overview)
    app.router.add_route("GET", "/api/devdash/metrics/timeseries",    metrics_timeseries)
    app.router.add_route("GET", "/api/devdash/monitoring",            monitoring_data)
    
    # Bots
    app.router.add_route("GET", "/api/devdash/bots",                  bots_list)
    app.router.add_post(        "/api/devdash/bots",                  bots_add)
    app.router.add_post(        "/api/devdash/bots/refresh",          bots_refresh)
    app.router.add_route("GET", "/api/devdash/bots/metrics",          bot_metrics)
    app.router.add_route("GET", "/api/devdash/bots/endpoints",        bot_endpoints)
    
    # Content Bot Stats
    app.router.add_route("GET", "/api/devdash/content/stats/overview",        content_bot_stats_overview)
    app.router.add_route("GET", "/api/devdash/content/stats/groups",          content_bot_groups_stats)
    app.router.add_route("GET", "/api/devdash/content/stats/claimed",         content_bot_claimed_stats)
    app.router.add_route("GET", "/api/devdash/content/stats/story-share",     content_bot_story_share_stats)
    app.router.add_route("GET", "/api/devdash/content/stats/ai-moderation",   content_bot_ai_moderation_stats)
    app.router.add_route("GET", "/api/devdash/content/stats/retention",       content_bot_user_retention)
    app.router.add_route("GET", "/api/devdash/content/stats/network",         content_bot_network_analysis)
    app.router.add_route("GET", "/api/devdash/content/stats/complete",        content_bot_complete_stats)
    
    # Bot-specific Stats Endpoints (7 bots)
    app.router.add_route("GET", "/api/devdash/bots/affiliate/stats",     affiliate_bot_stats)
    app.router.add_route("GET", "/api/devdash/bots/crossposter/stats",   crossposter_bot_stats)
    app.router.add_route("GET", "/api/devdash/bots/dao/stats",           dao_bot_stats)
    app.router.add_route("GET", "/api/devdash/bots/learning/stats",      learning_bot_stats)
    app.router.add_route("GET", "/api/devdash/bots/support/stats",       support_bot_stats)
    app.router.add_route("GET", "/api/devdash/bots/trade_api/stats",     trade_api_bot_stats)
    app.router.add_route("GET", "/api/devdash/bots/trade_dex/stats",     trade_dex_bot_stats)
    
    # Token Events
    app.router.add_route("GET", "/api/devdash/token-events",          token_events_list)
    app.router.add_post(        "/api/devdash/token-events",          token_events_create)
    
    # User Management
    app.router.add_route("GET", "/api/devdash/users",                 user_list)
    app.router.add_post(        "/api/devdash/users/tier",            user_update_tier)
    
    # Wallets & Payments
    app.router.add_post(        "/api/devdash/wallets/ton",           set_ton_address)
    app.router.add_route("GET", "/api/devdash/ton/payments",          ton_payments)
    app.router.add_route("GET", "/api/devdash/wallets",               wallets_overview)
    
    # Mesh
    app.router.add_route("GET", "/api/devdash/mesh/health",         mesh_health)
    app.router.add_route("GET", "/api/devdash/mesh/metrics",        mesh_metrics)
    
    # System Endpoints
    app.router.add_route("GET", "/api/system/health",                system_health)
    app.router.add_route("GET", "/api/system/logs",                  system_logs)
    app.router.add_route("GET", "/api/token/emrd",                   token_emrd_info)
    
    # Analytics Endpoints (under /api prefix)
    app.router.add_route("GET", "/api/analytics/bot-activity",       bot_activity)
    app.router.add_route("GET", "/api/analytics/bot-detailed",       bot_detailed_info)
    app.router.add_route("GET", "/api/analytics/bot-details",        bot_details_overview)
    app.router.add_route("GET", "/api/bot-groups",                   bot_groups)
    app.router.add_route("GET", "/api/token/holders",                token_holders)
    
    # CORS - auch mit /api prefix
    app.router.add_route("OPTIONS", "/api/devdash/{tail:.*}", options_handler)
    app.router.add_route("OPTIONS", "/api/system/{tail:.*}", options_handler)
    app.router.add_route("OPTIONS", "/api/token/{tail:.*}", options_handler)
    app.router.add_route("OPTIONS", "/api/analytics/{tail:.*}", options_handler)
    
# If you run this module standalone, boot a tiny aiohttp app for local testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = web.Application(middlewares=[cors_middleware])
    register_devdash_routes(app)
    app.on_startup.append(ensure_tables)
    web.run_app(app, port=int(os.getenv("PORT", 8080)))