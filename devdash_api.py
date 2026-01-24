import os, time, json, logging, hmac, hashlib, base64, asyncio, datetime, sys, pathlib, secrets
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

async def near_account_overview(request: web.Request):
    await _auth_user(request)
    account_id = request.query.get("account_id")
    if not account_id:
        raise web.HTTPBadRequest(text="account_id required")
    tokens_csv = request.query.get("tokens","").strip()
    tokens = [t for t in tokens_csv.split(",") if t]

    acct = await _rpc_view_account_near(account_id)
    out = {
        "account_id": account_id,
        "near": {
            "amount_yocto": acct.get("amount","0"),
            "amount_near":  yocto_to_near_str(acct.get("amount","0")),
            "locked_yocto": acct.get("locked","0"),
            "locked_near":  yocto_to_near_str(acct.get("locked","0")),
            "storage_usage": acct.get("storage_usage", 0),
            "code_hash": acct.get("code_hash")
        },
        "tokens": {}
    }
    for c in tokens:
        try:
            bal = await _rpc_view_function(c, "ft_balance_of", {"account_id": account_id})
        except Exception as e:
            bal = {"error": str(e)}
        out["tokens"][c] = bal
    return _json(out, request)

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
create table if not exists dashboard_ads (
  id serial primary key,
  name text not null,
  placement text not null check (placement in ('header','sidebar','in-bot','story','inline')),
  content text not null,
  is_active boolean not null default true,
  start_at timestamp,
  end_at timestamp,
  targeting jsonb not null default '{}'::jsonb,
  bot_slug text,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now()
);

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
    """System health check endpoint"""
    try:
        # Test database connection
        row = await fetchrow("select 1 as ok")
        db_status = "healthy" if row else "unhealthy"
    except Exception as e:
        log.warning("Database health check failed: %s", e)
        db_status = "unhealthy"
    
    return _json({
        "status": "ok",
        "timestamp": int(time.time()),
        "database": db_status,
        "uptime": int(time.time())
    }, request)


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
    """EMRD Token info endpoint"""
    return _json({
        "token": "EMRD",
        "name": "Emerald Token",
        "symbol": "EMRD",
        "decimals": 18,
        "contract": os.getenv("TOKEN_CONTRACT", ""),
        "network": os.getenv("NEAR_NETWORK", "mainnet"),
        "total_supply": "1000000000000000000000000000",
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
        row = await fetchrow(
            "select username, role, tier, first_name, last_name, photo_url, "
            "near_account_id, near_public_key, near_connected_at, ton_address "
            "from dashboard_users where telegram_id=%s",
            (user_id,)
        )
    except Exception as e:
        # Falls ton_address Spalte nicht existiert, versuche ohne sie
        log.warning("Query with ton_address failed: %s, retrying without ton_address", e)
        try:
            row = await fetchrow(
                "select username, role, tier, first_name, last_name, photo_url, "
                "near_account_id, near_public_key, near_connected_at "
                "from dashboard_users where telegram_id=%s",
                (user_id,)
            )
            if row:
                row = dict(row)
                row['ton_address'] = None  # Add missing field
        except Exception as e2:
            log.error("me() query failed: %s", e2)
            raise web.HTTPInternalServerError(text=f"Database error: {e2}")
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


async def near_verify(request: web.Request):
    user_id = await _auth_user(request)
    body = await request.json()
    account_id = body.get("account_id")
    public_key = body.get("public_key")  # e.g. "ed25519:..." base58
    sig_b64 = body.get("signature_b64")
    nonce_b64 = body.get("nonce_b64")
    message = body.get("message")

    if not (account_id and public_key and sig_b64 and nonce_b64 and message):
        raise web.HTTPBadRequest(text="missing fields")

    row = await fetchrow("select nonce from dashboard_nonces where telegram_id=%s", (user_id,))
    if not row:
        raise web.HTTPBadRequest(text="no challenge")
    expected_nonce = row["nonce"]  # bytes

    if base64.b64encode(expected_nonce).decode() != nonce_b64:
        raise web.HTTPBadRequest(text="nonce mismatch")

    # verify ed25519 signature against message bytes (NEP‑413 compatible wallets sign a canonical payload; most also accept raw message)
    if VerifyKey is None:
        raise web.HTTPBadRequest(text="pynacl not installed on server")

    try:
        if public_key.startswith("ed25519:"):
            pk_raw = b58decode(public_key.split(":", 1)[1])
        else:
            pk_raw = b58decode(public_key)
        verify_key = VerifyKey(pk_raw)
        verify_key.verify(message.encode(), base64.b64decode(sig_b64))
    except BadSignatureError:
        raise web.HTTPBadRequest(text="bad signature")
    except Exception as e:
        raise web.HTTPBadRequest(text=f"verify error: {e}")

    # (Optional) sanity‑check that public key belongs to account via RPC (access key list)
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            payload = {
                "jsonrpc": "2.0",
                "id": "verify-key",
                "method": "query",
                "params": {
                    "request_type": "view_access_key_list",
                    "finality": "final",
                    "account_id": account_id,
                },
            }
            r = await client.post(NEAR_RPC_URL, json=payload)
            r.raise_for_status()
            keys = [k.get("public_key") for k in r.json().get("result", {}).get("keys", [])]
            if public_key not in keys:
                log.warning("public key not in access_key_list for %s", account_id)
    except Exception as e:
        log.warning("rpc verify_owner failed: %s", e)

    await execute(
        "update dashboard_users set near_account_id=%s, near_public_key=%s, near_connected_at=now(), updated_at=now() where telegram_id=%s",
        (account_id, public_key, user_id),
    )
    # one‑time use nonce
    await execute("delete from dashboard_nonces where telegram_id=%s", (user_id,))

    return _json({"ok": True, "account_id": account_id}, request)


# ------------------------------ NEAR Token (NEP‑141) ------------------------------
async def _rpc_view_function(contract_id: str, method: str, args: Dict[str, Any]):
    args_b64 = base64.b64encode(json.dumps(args).encode()).decode()
    async with httpx.AsyncClient(timeout=10.0) as client:
        payload = {
            "jsonrpc": "2.0",
            "id": "view",
            "method": "query",
            "params": {
                "request_type": "call_function",
                "finality": "final",
                "account_id": contract_id,
                "method_name": method,
                "args_base64": args_b64,
            },
        }
        r = await client.post(NEAR_RPC_URL, json=payload)
        r.raise_for_status()
        res = r.json()["result"]["result"]
        return json.loads(bytes(res).decode())


async def near_token_summary(request: web.Request):
    await _auth_user(request)
    if not NEAR_TOKEN_CONTRACT:
        return _json({"enabled": False, "reason": "NEAR_TOKEN_CONTRACT not set"}, request)

    try:
        meta = await _rpc_view_function(NEAR_TOKEN_CONTRACT, "ft_metadata", {})
        total = await _rpc_view_function(NEAR_TOKEN_CONTRACT, "ft_total_supply", {})
    except Exception as e:
        return _json({"enabled": True, "error": f"rpc failed: {e}"}, request, status=502)

    # off‑chain events rollup
    roll = await fetchrow(
        """
        select
          coalesce(sum(case when kind='mint'  then amount when kind='reward' then amount else 0 end),0) as issued,
          coalesce(sum(case when kind='burn'  then amount when kind='fee'    then amount else 0 end),0) as burned_or_fees
        from dashboard_token_events
        """
    )

    return _json(
        {
            "enabled": True,
            "contract": NEAR_TOKEN_CONTRACT,
            "network": NEAR_NETWORK,
            "metadata": meta,
            "total_supply": total,
            "offchain": roll or {},
        },
        request,
    )

# ------------------------------ NEAR Account Overview ------------------------------
async def _rpc_view_account_near(account_id: str):
    async with httpx.AsyncClient(timeout=10.0) as client:
        payload = {
            "jsonrpc": "2.0","id":"view_account","method":"query",
            "params":{"request_type":"view_account","finality":"final","account_id":account_id}
        }
        r = await client.post(NEAR_RPC_URL, json=payload)
        r.raise_for_status()
        return r.json()["result"]

def yocto_to_near_str(yocto: str) -> str:
    try: return str(Decimal(yocto) / Decimal(10**24))
    except Exception: return "0"

async def near_account_overview(request: web.Request):
    await _auth_user(request)
    account_id = request.query.get("account_id")
    if not account_id:
        raise web.HTTPBadRequest(text="account_id required")
    tokens_csv = request.query.get("tokens","").strip()
    tokens = [t for t in tokens_csv.split(",") if t]
    acct = await _rpc_view_account_near(account_id)
    out = {
        "account_id": account_id,
        "near": {
            "amount_yocto": acct.get("amount","0"),
            "amount_near":  yocto_to_near_str(acct.get("amount","0")),
            "locked_yocto": acct.get("locked","0"),
            "locked_near":  yocto_to_near_str(acct.get("locked","0")),
            "storage_usage": acct.get("storage_usage",0),
            "code_hash": acct.get("code_hash")
        },
        "tokens": {}
    }
    for c in tokens:
        try:
            bal = await _rpc_view_function(c, "ft_balance_of", {"account_id": account_id})
        except Exception as e:
            bal = {"error": str(e)}
        out["tokens"][c] = bal
    return _json(out, request)

# ---------- NEAR: Wallet verbinden & Zahlungen ----------
async def set_near_account(request: web.Request):
    uid = await _auth_user(request)
    body = await request.json()
    acc  = (body.get("account_id") or "").strip()
    if not acc:
        raise web.HTTPBadRequest(text="account_id required")
    await execute("update dashboard_users set near_account_id=%s, near_connected_at=now(), updated_at=now() where telegram_id=%s",
                  (acc, uid))
    return _json({"ok": True, "near_account_id": acc}, request)

async def near_payments(request: web.Request):
    await _auth_user(request)
    account_id = request.query.get("account_id")
    limit = int(request.query.get("limit", "20"))
    if not account_id:
        raise web.HTTPBadRequest(text="account_id required")
    url = f"{NEARBLOCKS_API}/v1/account/{account_id}/activity?limit={limit}&order=desc"
    async with httpx.AsyncClient(timeout=10.0) as cx:
        r = await cx.get(url, headers={"accept":"application/json"})
        r.raise_for_status()
        j = r.json()
    # Filter: nur eingehende Native-NEAR Transfers
    items = []
    for it in j.get("activity", []):
        if it.get("type") == "TRANSFER" and it.get("receiver") == account_id:
            items.append({
              "ts": it.get("block_timestamp"),
              "tx_hash": it.get("tx_hash"),
              "from": it.get("signer"),
              "to": it.get("receiver"),
              "amount_yocto": it.get("delta_amount") or it.get("amount") or "0",
              "amount_near": yocto_to_near_str(it.get("delta_amount") or it.get("amount") or "0"),
            })
    return _json({"account_id": account_id, "incoming": items[:limit]}, request)


# ------------------------------ TON Wallet ------------------------------
async def set_ton_address(request: web.Request):
    user_id = await _auth_user(request)
    body = await request.json()
    address = (body.get("address") or "").strip()
    await execute("update dashboard_users set ton_address=%s, updated_at=now() where telegram_id=%s",
                    (address, user_id))
    return _json({"ok": True, "ton_address": address}, request)

async def ton_payments(request: web.Request):
    await _auth_user(request)
    address = request.query.get("address", "").strip()
    limit   = int(request.query.get("limit", "20"))
    if not address:
        raise web.HTTPBadRequest(text="address required")
    headers={}
    if TON_API_KEY: headers["Authorization"] = f"Bearer {TON_API_KEY}"
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

async def wallets_overview(request: web.Request):
    user_id = await _auth_user(request)
    me = await fetchrow("select near_account_id, ton_address from dashboard_users where telegram_id=%s", (user_id,))
    watches = await fetch("select id, chain, account_id, label, meta, created_at from dashboard_watch_accounts order by id asc")
    return _json({"me": me, "watch": watches}, request)
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
    """Liste alle Werbungen auf (optional gefiltert nach Bot)"""
    await _auth_user(request)
    bot_slug = request.query.get("bot_slug", "")
    sql = "select id, name, placement, content, is_active, targeting, bot_slug, created_at, updated_at from dashboard_ads"
    params = ()
    if bot_slug:
        sql += " where bot_slug = %s"
        params = (bot_slug,)
    sql += " order by created_at desc"
    rows = await fetch(sql, params)
    return _json({"ads": rows}, request)


async def ads_create(request: web.Request):
    """Erstelle eine neue Werbung"""
    await _auth_user(request)
    body = await request.json()
    name = (body.get("name") or "").strip()
    placement = (body.get("placement") or "header").strip()
    content = (body.get("content") or "").strip()
    is_active = bool(body.get("is_active", True))
    targeting = body.get("targeting", {})
    bot_slug = (body.get("bot_slug") or "").strip() or None
    start_at = body.get("start_at")
    end_at = body.get("end_at")
    
    if not name or not content:
        return _json({"error": "name und content erforderlich"}, request, status=400)
    
    await execute("""
        insert into dashboard_ads(name, placement, content, is_active, targeting, bot_slug, start_at, end_at)
        values (%s, %s, %s, %s, %s::jsonb, %s, to_timestamp(%s), to_timestamp(%s))
    """, (name, placement, content, is_active, json.dumps(targeting), bot_slug, start_at, end_at))
    
    return _json({"ok": True, "name": name}, request, status=201)


async def ads_delete(request: web.Request):
    """Lösche eine Werbung"""
    await _auth_user(request)
    ad_id = request.match_info.get("id")
    await execute("delete from dashboard_ads where id = %s", (ad_id,))
    return _json({"ok": True}, request)


async def ads_update(request: web.Request):
    """Aktualisiere eine Werbung"""
    await _auth_user(request)
    ad_id = request.match_info.get("id")
    body = await request.json()
    
    updates = []
    params = []
    if "name" in body:
        updates.append("name = %s")
        params.append(body["name"])
    if "placement" in body:
        updates.append("placement = %s")
        params.append(body["placement"])
    if "content" in body:
        updates.append("content = %s")
        params.append(body["content"])
    if "is_active" in body:
        updates.append("is_active = %s")
        params.append(body["is_active"])
    if "targeting" in body:
        updates.append("targeting = %s::jsonb")
        params.append(json.dumps(body["targeting"]))
    
    if not updates:
        return _json({"error": "keine Felder zum Aktualisieren"}, request, status=400)
    
    updates.append("updated_at = now()")
    params.append(ad_id)
    sql = "update dashboard_ads set " + ", ".join(updates) + " where id = %s"
    await execute(sql, tuple(params))
    return _json({"ok": True}, request)


# ------------------------------ Metrics & Analytics ------------------------------
async def metrics_overview(request: web.Request):
    """Übersicht: Benutzer, Werbungen, Bots, Events"""
    await _auth_user(request)
    users_total = await fetchrow("select count(1) as c from dashboard_users")
    ads_active = await fetchrow("select count(1) as c from dashboard_ads where is_active=true")
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
            near_account_id,
            ton_address,
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

# ------------------------------ route wiring ------------------------------
async def options_root(request: web.Request):
    return options_handler(request)


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
    
    # Bots
    app.router.add_route("GET", "/api/devdash/bots",                  bots_list)
    app.router.add_post(        "/api/devdash/bots",                  bots_add)
    app.router.add_post(        "/api/devdash/bots/refresh",          bots_refresh)
    app.router.add_route("GET", "/api/devdash/bots/metrics",          bot_metrics)
    app.router.add_route("GET", "/api/devdash/bots/endpoints",        bot_endpoints)
    
    # Ads (Werbungen)
    app.router.add_route("GET", "/api/devdash/ads",                   ads_list)
    app.router.add_post(        "/api/devdash/ads",                   ads_create)
    app.router.add_put(         "/api/devdash/ads/{id}",              ads_update)
    app.router.add_delete(      "/api/devdash/ads/{id}",              ads_delete)
    
    # Token Events
    app.router.add_route("GET", "/api/devdash/token-events",          token_events_list)
    app.router.add_post(        "/api/devdash/token-events",          token_events_create)
    
    # User Management
    app.router.add_route("GET", "/api/devdash/users",                 user_list)
    app.router.add_post(        "/api/devdash/users/tier",            user_update_tier)
    
    # Wallets & Payments
    app.router.add_route("GET", "/api/devdash/near/account/overview", near_account_overview)
    app.router.add_post(        "/api/devdash/wallets/near",          set_near_account)
    app.router.add_route("GET", "/api/devdash/near/payments",         near_payments)
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
    
    # CORS - auch mit /api prefix
    app.router.add_route("OPTIONS", "/api/devdash/{tail:.*}", options_handler)
    app.router.add_route("OPTIONS", "/api/system/{tail:.*}", options_handler)
    app.router.add_route("OPTIONS", "/api/token/{tail:.*}", options_handler)
    
    
        # Advanced Analytics
    app.router.add_route("GET", "/devdash/analytics/ads/performance", ad_performance_report)
    app.router.add_route("GET", "/devdash/analytics/ads/roi", ad_roi_analysis)
    app.router.add_route("GET", "/devdash/analytics/users/segments", user_segments_analysis)
    app.router.add_route("GET", "/devdash/analytics/users/retention", user_retention_analysis)
    app.router.add_route("GET", "/devdash/analytics/token/economics", token_economics_summary)
    app.router.add_route("GET", "/devdash/analytics/token/velocity", token_velocity_analysis)
    app.router.add_route("GET", "/devdash/analytics/bots/health", bot_health_dashboard)
    
    # Webhooks
    app.router.add_post("/devdash/webhooks", webhook_create)
    app.router.add_post("/devdash/webhooks/{id}/test", webhook_test)
    
    # Exports
    app.router.add_route("GET", "/devdash/export/users.csv", export_users_csv)
    app.router.add_route("GET", "/devdash/export/ads-report", export_ads_report)
    
# If you run this module standalone, boot a tiny aiohttp app for local testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = web.Application(middlewares=[cors_middleware])
    register_devdash_routes(app)
    app.on_startup.append(ensure_tables)
    web.run_app(app, port=int(os.getenv("PORT", 8080)))