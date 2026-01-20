import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger(__name__)

try:
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover
    ConnectionPool = None

_POOL = None

def _get_pool():
    global _POOL
    if _POOL is not None:
        return _POOL
    if ConnectionPool is None:
        return None
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        return None
    _POOL = ConnectionPool(db_url, min_size=1, max_size=5, kwargs={"autocommit": True})
    return _POOL

def execute(sql: str, params: tuple = ()):
    """
    Pool-first execute (autocommit). Falls Pool nicht verfügbar -> psycopg2 fallback.
    """
    pool = _get_pool()
    if pool:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(sql, params)
        return
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("DB connection unavailable")
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
    finally:
        try: cur.close()
        except Exception: pass
        conn.close()

def fetch(sql: str, params: tuple = ()):
    pool = _get_pool()
    if pool:
        with pool.connection() as con, con.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
            return [dict(zip(cols, r)) for r in rows]
    conn = get_db_connection()
    if not conn:
        return []
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, params)
        return cur.fetchall()
    except Exception as e:
        logger.error(f"DB fetch error: {e}")
        return []
    finally:
        try: cur.close()
        except Exception: pass
        conn.close()

def fetchrow(sql: str, params: tuple = ()):
    rows = fetch(sql, params)
    return rows[0] if rows else None

def get_db_connection():
    """Get database connection"""
    try:
        import os
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None


def init_all_schemas():
    """Initialize Trade API database schemas"""
    conn = get_db_connection()
    if not conn:
        logger.error("Cannot initialize schemas: No database connection")
        return
    
    cur = None
    try:
        cur = conn.cursor()

        # Portfolios table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_portfolios (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL UNIQUE,
                name VARCHAR(255),
                total_value NUMERIC(18,8),
                cash NUMERIC(18,8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Portfolio positions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_positions (
                id SERIAL PRIMARY KEY,
                portfolio_id INTEGER REFERENCES tradeapi_portfolios(id) ON DELETE CASCADE,
                asset_symbol VARCHAR(50),
                quantity NUMERIC(18,8),
                entry_price NUMERIC(18,8),
                current_price NUMERIC(18,8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Trading signals
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_signals (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                symbol VARCHAR(50),
                signal_type VARCHAR(20),
                confidence NUMERIC(3,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Price alerts
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_alerts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                symbol VARCHAR(50),
                alert_type VARCHAR(20),
                target_price NUMERIC(18,8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                triggered_at TIMESTAMP
            )
        """)
        
        # On-chain proofs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_proofs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tx_hash VARCHAR(255),
                asset VARCHAR(100),
                amount NUMERIC(18,8),
                proof_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                verified_at TIMESTAMP
            )
        """)
        # --- NEW (needed for MiniApp-first setup) ---
        # API Keys (encrypted blob is stored by server layer)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_keys (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                provider TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '',
                api_fields_enc TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE(telegram_id, provider, COALESCE(label,''))
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS tradeapi_keys_tid_idx ON tradeapi_keys(telegram_id)")

        # Signal proof hashes (DB = source of truth; optional On-Chain later)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_signal_proofs (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                provider TEXT NOT NULL,
                symbol TEXT NOT NULL,
                signal_json JSONB NOT NULL,
                signal_hash TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS tradeapi_signal_proofs_tid_idx ON tradeapi_signal_proofs(telegram_id)")

        # User settings for MiniApp
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tradeapi_settings (
                user_id BIGINT PRIMARY KEY,
                theme TEXT,
                language TEXT,
                notifications_enabled BOOLEAN DEFAULT TRUE,
                alert_threshold_usd NUMERIC(18,8) DEFAULT 100,
                preferred_currency TEXT DEFAULT 'USD',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)

        conn.commit()
        logger.info("Trade API schemas initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing schemas: {e}")
        conn.rollback()
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass
        conn.close()

def create_portfolio(user_id: int, name: str) -> bool:
    """Create new portfolio for user"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO tradeapi_portfolios (user_id, name, total_value, cash) VALUES (%s, %s, %s, %s)",
            (user_id, name, 0, 0)
        )
        conn.commit()
        logger.info(f"Portfolio created for user {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error creating portfolio: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def get_portfolio(user_id: int) -> Optional[dict]:
    """Get user's portfolio"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM tradeapi_portfolios WHERE user_id = %s", (user_id,))
        return cur.fetchone()
    except Exception as e:
        logger.error(f"Error fetching portfolio: {e}")
        return None
    finally:
        cur.close()
        conn.close()


def add_position(portfolio_id: int, asset_symbol: str, quantity: float, entry_price: float) -> bool:
    """Add position to portfolio"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tradeapi_positions (portfolio_id, asset_symbol, quantity, entry_price)
            VALUES (%s, %s, %s, %s)
        """, (portfolio_id, asset_symbol, quantity, entry_price))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding position: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def get_positions(portfolio_id: int) -> list:
    """Get all positions in portfolio"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM tradeapi_positions WHERE portfolio_id = %s", (portfolio_id,))
        return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching positions: {e}")
        return []
    finally:
        cur.close()
        conn.close()


def add_alert(user_id: int, symbol: str, alert_type: str, target_price: float) -> bool:
    """Add price alert"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tradeapi_alerts (user_id, symbol, alert_type, target_price)
            VALUES (%s, %s, %s, %s)
        """, (user_id, symbol, alert_type, target_price))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding alert: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def get_alerts(user_id: int) -> list:
    """Get user's active alerts"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT * FROM tradeapi_alerts WHERE user_id = %s AND triggered_at IS NULL",
            (user_id,)
        )
        return cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return []
    finally:
        cur.close()
        conn.close()
