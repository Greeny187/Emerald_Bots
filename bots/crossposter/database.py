import asyncio
import asyncpg
import os
import logging
import psycopg2

logger = logging.getLogger(__name__)
_pool = None
_pool_lock = asyncio.Lock()

async def get_pool():
    global _pool
    if _pool is not None:
        return _pool

    async with _pool_lock:
        # Doppelt prüfen (Race-Condition vermeiden)
        if _pool is not None:
            return _pool

        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            logger.error("DATABASE_URL ist nicht gesetzt")
            raise RuntimeError("DATABASE_URL ist nicht gesetzt")

        try:
            logger.info("Erstelle asyncpg Pool (min=1, max=5)")
            _pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
            logger.info("DB-Pool bereit")
            return _pool
        except Exception:
            logger.exception("Fehler beim Erstellen des DB-Pools")
            raise


def init_all_schemas():
    """Initialize Crossposter database schemas"""
    conn = None
    try:
        if not os.environ.get("DATABASE_URL"):
            logger.error("DATABASE_URL ist nicht gesetzt (schema init)")
            return False
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()

        # Crossposter posts table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crossposter_posts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                source_channel BIGINT,
                source_message_id BIGINT,
                target_channels TEXT DEFAULT '[]',
                content TEXT,
                media JSONB DEFAULT '[]',
                status VARCHAR(50) DEFAULT 'draft',
                scheduled_at TIMESTAMP,
                posted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Crossposter channels configuration
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crossposter_channels (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                channel_name VARCHAR(255),
                is_enabled BOOLEAN DEFAULT TRUE,
                auto_forward BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, channel_id)
            )
        """)

        # Crossposter rules
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crossposter_rules (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                source_channel BIGINT NOT NULL,
                target_channels TEXT DEFAULT '[]',
                keywords TEXT DEFAULT '[]',
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_crossposter_posts_user ON crossposter_posts(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crossposter_posts_status ON crossposter_posts(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crossposter_channels_user ON crossposter_channels(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crossposter_rules_user ON crossposter_rules(user_id)")

        conn.commit()
        logger.info("Crossposter schemas initialized successfully")
        return True
    except Exception:
        logger.exception("Crossposter schema init error")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

