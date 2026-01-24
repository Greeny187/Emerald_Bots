"""
Emerald Story Sharing System
Users können direkt aus der Miniapp Stories teilen mit automatischem Referral-Link.
Settings/Rate-Limits werden pro Gruppe (chat_id) gesteuert (siehe database.get_story_settings()).
"""

import logging
import os
import json
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

import psycopg2

logger = logging.getLogger(__name__)

BOT_USERNAME = (os.getenv("BOT_USERNAME") or "emerald_bot").lstrip("@")

# Story Template Typen
STORY_TEMPLATES = {
    "group_bot": {
        "title": "✨ Emerald nutzen – Story teilen",
        "description": "Diese Gruppe wird mit Emerald automatisiert",
        "emoji": "🤖",
        "color": "#10C7A0",
        "reward_points": 50
    },
    "stats": {
        "title": "📊 Meine Stats teilen",
        "description": "Schau dir meine Gruppe-Statistiken an",
        "emoji": "📊",
        "color": "#0FA890",
        "reward_points": 40
    },
    "content": {
        "title": "📝 Content Bot nutzen",
        "description": "Automatische Posts mit Emerald Content",
        "emoji": "📝",
        "color": "#10C7A0",
        "reward_points": 45
    },
    "emrd_rewards": {
        "title": "💎 EMRD erhalten – Story teilen",
        "description": "Verdiene Rewards mit Emerald",
        "emoji": "💎",
        "color": "#10C7A0",
        "reward_points": 60
    },
    "affiliate": {
        "title": "🔥 Zeig deinen Gruppen-Bot",
        "description": "Meine Gruppe wird mit Emerald gemanagt",
        "emoji": "🔥",
        "color": "#22c55e",
        "reward_points": 100
    }
}

@contextmanager
def _conn():
    """DB Connection über den vorhandenen psycopg2 Pool aus database.py (wenn verfügbar)."""
    pool_inst = None
    conn = None
    try:
        from .database import _db_pool  # type: ignore
        pool_inst = _db_pool
    except Exception:
        pool_inst = None

    try:
        if pool_inst:
            conn = pool_inst.getconn()
        else:
            conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        yield conn
    finally:
        try:
            if pool_inst and conn:
                pool_inst.putconn(conn)
            elif conn:
                conn.close()
        except Exception:
            pass


def init_story_sharing_schema() -> bool:
    """Initialize story sharing tables + Indizes (Leaderboards/Rate-Limits)."""
    try:
        with _conn() as conn:
            cur = conn.cursor()

            # Story Shares tracking
            cur.execute("""
                CREATE TABLE IF NOT EXISTS story_shares (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    chat_id BIGINT NOT NULL,
                    story_template VARCHAR(50),
                    referral_link TEXT,
                    shared_at TIMESTAMP DEFAULT NOW(),
                    clicks INT DEFAULT 0,
                    conversions INT DEFAULT 0,
                    click_rewarded_steps INT DEFAULT 0,
                    meta_json JSONB DEFAULT '{}'::jsonb,
                    status VARCHAR(50) DEFAULT 'shared'
                )
            """)

            # kleine Migrationen (falls Tabelle bereits existiert)
            cur.execute("ALTER TABLE story_shares ADD COLUMN IF NOT EXISTS click_rewarded_steps INT DEFAULT 0;")
            cur.execute("ALTER TABLE story_shares ADD COLUMN IF NOT EXISTS meta_json JSONB DEFAULT '{}'::jsonb;")

            # Story Click tracking
            cur.execute("""
                CREATE TABLE IF NOT EXISTS story_clicks (
                    id SERIAL PRIMARY KEY,
                    share_id INT NOT NULL,
                    visitor_id BIGINT,
                    clicked_at TIMESTAMP DEFAULT NOW(),
                    source VARCHAR(50),
                    FOREIGN KEY (share_id) REFERENCES story_shares(id) ON DELETE CASCADE
                )
            """)

            # Story Conversions (visitor joined group/bot)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS story_conversions (
                    id SERIAL PRIMARY KEY,
                    share_id INT NOT NULL,
                    referrer_id BIGINT NOT NULL,
                    visitor_id BIGINT NOT NULL,
                    conversion_type VARCHAR(50),
                    reward_earned NUMERIC(10,2),
                    converted_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (share_id) REFERENCES story_shares(id) ON DELETE CASCADE
                )
            """)

            # Indizes (extra, aber sehr sinnvoll)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_story_shares_user_chat_day ON story_shares(user_id, chat_id, shared_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_story_shares_chat_day ON story_shares(chat_id, shared_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_story_clicks_share ON story_clicks(share_id, clicked_at);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_story_conversions_share ON story_conversions(share_id, converted_at);")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uidx_story_clicks_share_visitor ON story_clicks(share_id, visitor_id) WHERE visitor_id IS NOT NULL;")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uidx_story_conversions_share_visitor ON story_conversions(share_id, visitor_id);")

            conn.commit()
            logger.info("✅ Story sharing schema initialized (+indizes)")
            return True
    except Exception as e:
        logger.error(f"Schema init error: {e}", exc_info=True)
        return False


def create_story_share(
    user_id: int,
    chat_id: int,
    template: str,
    group_name: str = "Meine Gruppe"
) -> Optional[Dict[str, Any]]:
    """Create a new story share and return share info."""

    if template not in STORY_TEMPLATES:
        logger.error(f"Unknown template: {template}")
        return None

    try:
        with _conn() as conn:
            cur = conn.cursor()

            # 1) Insert first to get share_id
            cur.execute("""
                INSERT INTO story_shares (user_id, chat_id, story_template, referral_link, status)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (user_id, chat_id, template, "", "active"))
            share_id = int(cur.fetchone()[0])

            # 2) Build referral link (tamper-resistant: share_id reicht)
            referral_link = f"https://t.me/{BOT_USERNAME}?start=story_{share_id}"

            cur.execute("UPDATE story_shares SET referral_link=%s WHERE id=%s;", (referral_link, share_id))
            conn.commit()

            t = STORY_TEMPLATES[template]
            return {
                "share_id": share_id,
                "referral_link": referral_link,
                "template": template,
                "title": t["title"],
                "description": t["description"],
                "emoji": t["emoji"],
                "color": t["color"],
                "reward_points": t["reward_points"],
                "group_name": group_name
            }
    except Exception as e:
        logger.error(f"Create share error: {e}", exc_info=True)
        return None


def get_share_by_id(share_id: int) -> Optional[Dict[str, Any]]:
    try:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, user_id, chat_id, story_template, referral_link, clicks, conversions, click_rewarded_steps, shared_at, status, meta_json
                FROM story_shares WHERE id=%s
            """, (share_id,))
            r = cur.fetchone()
            if not r:
                return None
            meta = r[10]
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            if meta is None:
                meta = {}
            return {
                "share_id": int(r[0]),
                "user_id": int(r[1]),
                "chat_id": int(r[2]),
                "template": r[3],
                "referral_link": r[4] or "",
                "clicks": int(r[5] or 0),
                "conversions": int(r[6] or 0),
                "click_rewarded_steps": int(r[7] or 0),
                "shared_at": r[8].isoformat() if r[8] else None,
                "status": r[9] or "",
                "meta": meta,
            }
    except Exception as e:
        logger.error(f"get_share_by_id error: {e}", exc_info=True)
        return None

def set_share_meta(share_id: int, meta: Dict[str, Any]) -> bool:
    """Merge-Update der meta_json am Share (JSONB)."""
    try:
        payload = json.dumps(meta or {}, ensure_ascii=False)
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE story_shares SET meta_json = COALESCE(meta_json,'{}'::jsonb) || %s::jsonb WHERE id=%s;",
                (payload, share_id)
            )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"set_share_meta error: {e}", exc_info=True)
        return False

def set_click_rewarded_steps(share_id: int, steps: int) -> bool:
    """Merkt sich, bis zu welchem 10er-Step Click-Rewards bereits vergeben wurden."""
    try:
        steps = max(0, int(steps or 0))
    except Exception:
        steps = 0
    try:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE story_shares SET click_rewarded_steps=%s WHERE id=%s;", (steps, share_id))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"set_click_rewarded_steps error: {e}", exc_info=True)
        return False


def count_shares_today(user_id: int, chat_id: int) -> int:
    """Zählt Shares seit Tagesbeginn (DB-Zeit) – pro User + Gruppe."""
    try:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(1)
                FROM story_shares
                WHERE user_id=%s AND chat_id=%s
                  AND shared_at >= date_trunc('day', NOW())
            """, (user_id, chat_id))
            return int(cur.fetchone()[0] or 0)
    except Exception as e:
        logger.error(f"count_shares_today error: {e}", exc_info=True)
        return 0


def track_story_click(share_id: int, visitor_id: int, source: str = "story") -> bool:
    """Track when someone clicks on a shared story.
    Anti-farm: pro visitor_id nur ein Click pro Share (wenn visitor_id > 0).
    """
    try:
        with _conn() as conn:
            cur = conn.cursor()

            if visitor_id and visitor_id > 0:
                cur.execute("SELECT 1 FROM story_clicks WHERE share_id=%s AND visitor_id=%s LIMIT 1;", (share_id, visitor_id))
                if cur.fetchone():
                    return True  # already counted

            cur.execute("INSERT INTO story_clicks (share_id, visitor_id, source) VALUES (%s, %s, %s);",
                        (share_id, visitor_id or None, source))
            cur.execute("UPDATE story_shares SET clicks = clicks + 1 WHERE id = %s;", (share_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Track click error: {e}", exc_info=True)
        return False


def record_story_conversion(
    share_id: int,
    referrer_id: int,
    visitor_id: int,
    conversion_type: str = "joined_group",
    reward_points: float = 50.0
) -> bool:
    """Record a conversion from a story share.
    Anti-farm: pro visitor_id nur eine Conversion pro Share.
    """
    try:
        with _conn() as conn:
            cur = conn.cursor()

            cur.execute("SELECT 1 FROM story_conversions WHERE share_id=%s AND visitor_id=%s LIMIT 1;", (share_id, visitor_id))
            if cur.fetchone():
                return True  # already recorded

            cur.execute("""
                INSERT INTO story_conversions (share_id, referrer_id, visitor_id, conversion_type, reward_earned)
                VALUES (%s, %s, %s, %s, %s)
            """, (share_id, referrer_id, visitor_id, conversion_type, float(reward_points or 0.0)))

            cur.execute("UPDATE story_shares SET conversions = conversions + 1 WHERE id = %s;", (share_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Record conversion error: {e}", exc_info=True)
        return False


def get_share_stats(share_id: int) -> Optional[Dict[str, Any]]:
    """Get statistics for a story share"""
    try:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id, user_id, chat_id, story_template, referral_link, clicks, conversions, shared_at
                FROM story_shares
                WHERE id = %s
            """, (share_id,))
            row = cur.fetchone()
            if not row:
                return None

            clicks = int(row[5] or 0)
            conv = int(row[6] or 0)
            return {
                "share_id": int(row[0]),
                "user_id": int(row[1]),
                "chat_id": int(row[2]),
                "template": row[3],
                "referral_link": row[4],
                "clicks": clicks,
                "conversions": conv,
                "shared_at": row[7].isoformat() if row[7] else None,
                "conversion_rate": (conv / clicks * 100) if clicks > 0 else 0.0
            }
    except Exception as e:
        logger.error(f"Get stats error: {e}", exc_info=True)
        return None


def get_user_shares(user_id: int, limit: int = 10, chat_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Get all story shares by a user (optional: only within a group)."""
    try:
        with _conn() as conn:
            cur = conn.cursor()

            if chat_id:
                cur.execute("""
                    SELECT id, story_template, referral_link, clicks, conversions, shared_at, chat_id
                    FROM story_shares
                    WHERE user_id = %s AND chat_id=%s
                    ORDER BY shared_at DESC
                    LIMIT %s
                """, (user_id, chat_id, limit))
            else:
                cur.execute("""
                    SELECT id, story_template, referral_link, clicks, conversions, shared_at, chat_id
                    FROM story_shares
                    WHERE user_id = %s
                    ORDER BY shared_at DESC
                    LIMIT %s
                """, (user_id, limit))

            rows = cur.fetchall()
            return [
                {
                    "share_id": int(r[0]),
                    "template": r[1],
                    "referral_link": r[2],
                    "clicks": int(r[3] or 0),
                    "conversions": int(r[4] or 0),
                    "shared_at": r[5].isoformat() if r[5] else None,
                    "chat_id": int(r[6] or 0),
                }
                for r in rows
            ]
    except Exception as e:
        logger.error(f"Get user shares error: {e}", exc_info=True)
        return []


def get_top_shares(days: int = 7, limit: int = 10, chat_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Get top performing shares (leaderboard)."""
    try:
        days = max(1, int(days or 7))
        limit = max(1, min(int(limit or 10), 50))
    except Exception:
        days, limit = (7, 10)

    try:
        with _conn() as conn:
            cur = conn.cursor()

            if chat_id:
                cur.execute("""
                    SELECT id, user_id, story_template, clicks, conversions, shared_at
                    FROM story_shares
                    WHERE chat_id=%s AND shared_at > NOW() - (%s * INTERVAL '1 day')
                    ORDER BY conversions DESC, clicks DESC, shared_at DESC
                    LIMIT %s
                """, (chat_id, days, limit))
            else:
                cur.execute("""
                    SELECT id, user_id, story_template, clicks, conversions, shared_at
                    FROM story_shares
                    WHERE shared_at > NOW() - (%s * INTERVAL '1 day')
                    ORDER BY conversions DESC, clicks DESC, shared_at DESC
                    LIMIT %s
                """, (days, limit))

            rows = cur.fetchall()
            return [
                {
                    "share_id": int(r[0]),
                    "user_id": int(r[1]),
                    "template": r[2],
                    "clicks": int(r[3] or 0),
                    "conversions": int(r[4] or 0),
                    "shared_at": r[5].isoformat() if r[5] else None
                }
                for r in rows
            ]
    except Exception as e:
        logger.error(f"Get top shares error: {e}", exc_info=True)
        return []
