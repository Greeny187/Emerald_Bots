import hashlib
import json
import time
from ..database import execute, fetch

INIT_SQL = """
create table if not exists tradeapi_signal_proofs (
  id bigserial primary key,
  telegram_id bigint not null,
  provider text not null,
  symbol text not null,
  signal_json jsonb not null,
  signal_hash text not null,
  created_at timestamptz not null default now()
);
create index if not exists tradeapi_signal_proofs_tid_idx on tradeapi_signal_proofs(telegram_id);
"""

def ensure_table():
    for stmt in [s.strip() for s in INIT_SQL.split(";") if s.strip()]:
        execute(stmt + ";")

def hash_signal(payload: dict) -> str:
    canon = json.dumps(payload, sort_keys=True, separators=(",",":")).encode()
    return hashlib.sha256(canon).hexdigest()

def record_proof(telegram_id: int, provider: str, symbol: str, signal: dict) -> dict:
    h = hash_signal(signal)
    execute("insert into tradeapi_signal_proofs(telegram_id, provider, symbol, signal_json, signal_hash) values (%s,%s,%s,%s,%s)",
            (telegram_id, provider, symbol, json.dumps(signal), h))
    return {"hash": h, "ts": int(time.time())}

def list_proofs(telegram_id: int, limit: int = 50) -> list[dict]:
    rows = fetch("select provider, symbol, signal_hash, created_at from tradeapi_signal_proofs where telegram_id=%s order by id desc limit %s", (telegram_id, limit))
    return rows
