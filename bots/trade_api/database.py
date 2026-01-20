"""Emerald Trade API — Database

Single source of truth for DB access.

✅ One file only (replaces old db.py + database.py)
✅ Connection pooling (psycopg_pool) with sane defaults
✅ Schema init for Trade API features (keys, portfolios, signals, alerts, cache)
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


DATABASE_URL = os.getenv("DATABASE_URL")


# -----------------------------
# Connection / Pool
# -----------------------------

_pool = None


def _init_pool() -> None:
    """Initialize the connection pool once."""
    global _pool
    if _pool is not None:
        return

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env fehlt")

    min_size = int(os.getenv("DB_POOL_MIN", "1"))
    max_size = int(os.getenv("DB_POOL_MAX", "5"))

    try:
        from psycopg_pool import ConnectionPool

        _pool = ConnectionPool(
            conninfo=DATABASE_URL,
            min_size=min_size,
            max_size=max_size,
            kwargs={"autocommit": True},
        )
        logger.info("DB pool ready (psycopg_pool) min=%s max=%s", min_size, max_size)
    except Exception as e:
        # Fallback: psycopg2 without pooling (still works for low traffic)
        _pool = "psycopg2"
        logger.warning("psycopg_pool not available, falling back to psycopg2: %s", e)


def _connect_psycopg2():
    import psycopg2

    return psycopg2.connect(DATABASE_URL)


def execute(sql: str, params: Sequence[Any] = ()) -> None:
    _init_pool()
    if _pool == "psycopg2":
        with _connect_psycopg2() as con, con.cursor() as cur:
            cur.execute(sql, params)
            con.commit()
        return

    with _pool.connection() as con, con.cursor() as cur:
        cur.execute(sql, params)


def fetch(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    _init_pool()
    if _pool == "psycopg2":
        with _connect_psycopg2() as con, con.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name if hasattr(c, "name") else c[0] for c in (cur.description or [])]
            rows = cur.fetchall() if cur.description else []
            return [dict(zip(cols, r)) for r in rows]

    with _pool.connection() as con, con.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in (cur.description or [])]
        rows = cur.fetchall() if cur.description else []
        return [dict(zip(cols, r)) for r in rows]


def fetchrow(sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
    rows = fetch(sql, params)
    return rows[0] if rows else None


def fetchval(sql: str, params: Sequence[Any] = ()) -> Any:
    row = fetchrow(sql, params)
    if not row:
        return None
    return next(iter(row.values()))


# -----------------------------
# Schema
# -----------------------------

# Note: We store `label` as NOT NULL default '' so we can use a clean UNIQUE constraint
# and a clean ON CONFLICT clause (no COALESCE tricks).
INIT_SQL = """
create table if not exists tradeapi_keys (
  id bigserial primary key,
  telegram_id bigint not null,
  provider text not null,
  label text not null default '',
  api_fields_enc text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(telegram_id, provider, label)
);
create index if not exists tradeapi_keys_tid_idx on tradeapi_keys(telegram_id);

-- Migrations / hardening (safe to re-run)
alter table tradeapi_keys alter column label set default '';
update tradeapi_keys set label='' where label is null;
alter table tradeapi_keys alter column label set not null;
create unique index if not exists idx_tradeapi_keys_unique on tradeapi_keys(telegram_id, provider, label);

create table if not exists tradeapi_user_settings (
  telegram_id bigint primary key,
  theme text default 'dark',
  notifications_enabled boolean default true,
  alert_threshold_usd numeric(18,2) default 100.00,
  preferred_currency text default 'USD',
  language text default 'de',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Practical risk defaults (can be tuned later in the miniapp)
create table if not exists tradeapi_user_risk (
  telegram_id bigint primary key,
  risk_per_trade numeric(6,5) not null default 0.005, -- 0.5%
  max_day_loss numeric(6,5) not null default 0.020,   -- 2.0%
  max_positions int not null default 3,
  min_confidence numeric(4,3) not null default 0.600,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists tradeapi_portfolios (
  id bigserial primary key,
  telegram_id bigint not null,
  name text not null,
  description text,
  total_value numeric(18,8),
  cash numeric(18,8),
  risk_level text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(telegram_id, name)
);
create index if not exists tradeapi_portfolios_tid_idx on tradeapi_portfolios(telegram_id);

create table if not exists tradeapi_positions (
  id bigserial primary key,
  portfolio_id bigint references tradeapi_portfolios(id) on delete cascade,
  asset_symbol text,
  quantity numeric(18,8),
  entry_price numeric(18,8),
  current_price numeric(18,8),
  cost_basis numeric(18,8),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create index if not exists tradeapi_positions_pid_idx on tradeapi_positions(portfolio_id);

create table if not exists tradeapi_alerts (
  id bigserial primary key,
  telegram_id bigint not null,
  symbol text,
  alert_type text,
  target_price numeric(18,8),
  comparison text,
  is_active boolean default true,
  is_triggered boolean default false,
  created_at timestamptz not null default now(),
  triggered_at timestamptz
);
create index if not exists tradeapi_alerts_tid_idx on tradeapi_alerts(telegram_id);
create index if not exists tradeapi_alerts_active_idx on tradeapi_alerts(is_active) where is_active = true;

create table if not exists tradeapi_signals (
  id bigserial primary key,
  telegram_id bigint not null,
  symbol text,
  signal_type text,
  confidence numeric(6,5),
  strength numeric(6,5),
  regime text,
  atr_value numeric(18,8),
  entry_price numeric(18,8),
  stop_loss numeric(18,8),
  take_profit numeric(18,8),
  position_size numeric(18,8),
  created_at timestamptz not null default now()
);
create index if not exists tradeapi_signals_tid_idx on tradeapi_signals(telegram_id);
create index if not exists tradeapi_signals_sym_idx on tradeapi_signals(symbol);

create table if not exists tradeapi_trades (
  id bigserial primary key,
  telegram_id bigint not null,
  portfolio_id bigint references tradeapi_portfolios(id),
  symbol text,
  side text,
  quantity numeric(18,8),
  entry_price numeric(18,8),
  exit_price numeric(18,8),
  commission numeric(18,8),
  pnl numeric(18,8),
  pnl_percent numeric(10,4),
  status text,
  opened_at timestamptz,
  closed_at timestamptz,
  notes text,
  created_at timestamptz not null default now()
);
create index if not exists tradeapi_trades_tid_idx on tradeapi_trades(telegram_id);

create table if not exists tradeapi_sentiment (
  id bigserial primary key,
  text text,
  sentiment text,
  positive numeric(6,5),
  neutral numeric(6,5),
  negative numeric(6,5),
  created_at timestamptz not null default now()
);
create index if not exists tradeapi_sentiment_created_idx on tradeapi_sentiment(created_at);

create table if not exists tradeapi_market_cache (
  id bigserial primary key,
  symbol text,
  provider text,
  price numeric(18,8),
  volume numeric(18,8),
  change_24h numeric(10,4),
  high_24h numeric(18,8),
  low_24h numeric(18,8),
  created_at timestamptz not null default now(),
  unique(symbol, provider)
);
create index if not exists tradeapi_market_symbol_idx on tradeapi_market_cache(symbol);
"""


def init_schema() -> None:
    """Create / migrate the Trade API tables (idempotent)."""
    for stmt in [s.strip() for s in INIT_SQL.split(";") if s.strip()]:
        execute(stmt + ";")
