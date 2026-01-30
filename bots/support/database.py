"""database.py — Emerald Support Bot Database (single source of truth)

✅ Only file for DB operations (no sql.py needed)
✅ Psycopg2 (sync) but safe to call from FastAPI using anyio.to_thread
✅ Support-only schema: users, tickets, messages, kb, tenants
✅ Strong logging at critical points
"""

import logging
import os
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("bot.support.db")


def get_db_connection():
    logger.debug("[DB] Attempting database connection")
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        logger.error("[DB] DATABASE_URL missing")
        return None
    try:
        conn = psycopg2.connect(dsn)
        logger.debug("[DB] ✅ Database connection established")
        return conn
    except Exception as e:
        logger.exception(f"[DB] ❌ Database connection failed: {e}")
        return None


# ---------------- Schema ----------------

def init_all_schemas() -> None:
    """Initialize Support database schemas (idempotent)."""
    logger.info("[SCHEMA] Starting schema initialization")
    conn = get_db_connection()
    if not conn:
        logger.error("[SCHEMA] Cannot initialize schemas - no DB connection")
        return

    cur = None
    try:
        cur = conn.cursor()
        logger.debug("[SCHEMA] Creating tenants table")

        # Tenants
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                id SERIAL PRIMARY KEY,
                slug TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """)
        logger.debug("[SCHEMA] ✅ Tenants table created/verified")

        # Tenant ↔ Telegram chats/groups mapping
        logger.debug("[SCHEMA] Creating tenant_groups table")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tenant_groups (
                tenant_id INTEGER NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                chat_id BIGINT UNIQUE NOT NULL,
                title TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """)
        logger.debug("[SCHEMA] ✅ Tenant_groups table created/verified")

        # Users
        logger.debug("[SCHEMA] Creating support_users table")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS support_users (
                user_id BIGINT PRIMARY KEY,
                handle TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            );
        """)
        logger.debug("[SCHEMA] ✅ Support_users table created/verified")

        # Tickets
        logger.debug("[SCHEMA] Creating support_tickets table")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS support_tickets (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES support_users(user_id) ON DELETE CASCADE,
                tenant_id INTEGER REFERENCES tenants(id) ON DELETE SET NULL,
                category VARCHAR(48) DEFAULT 'allgemein',
                subject VARCHAR(140) NOT NULL,
                status VARCHAR(32) DEFAULT 'neu',
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now(),
                closed_at TIMESTAMPTZ
            );
        """)
        logger.debug("[SCHEMA] ✅ Support_tickets table created/verified")

        # Messages
        logger.debug("[SCHEMA] Creating support_messages table")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS support_messages (
                id SERIAL PRIMARY KEY,
                ticket_id INTEGER NOT NULL REFERENCES support_tickets(id) ON DELETE CASCADE,
                author_user_id BIGINT NOT NULL,
                is_public BOOLEAN DEFAULT TRUE,
                text TEXT NOT NULL,
                attachments JSONB,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """)
        logger.debug("[SCHEMA] ✅ Support_messages table created/verified")

        # KB articles
        logger.debug("[SCHEMA] Creating kb_articles table")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kb_articles (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                tags TEXT[],
                score INTEGER DEFAULT 0,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
        """)
        logger.debug("[SCHEMA] ✅ KB_articles table created/verified")

        # Helpful indexes
        logger.debug("[SCHEMA] Creating indexes")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_user ON support_tickets(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_support_tickets_tenant ON support_tickets(tenant_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_support_messages_ticket ON support_messages(ticket_id);")
        logger.debug("[SCHEMA] ✅ Indexes created/verified")

        conn.commit()
        logger.info("✅ [SCHEMA] All database schemas initialized successfully")
    except Exception as e:
        logger.exception(f"❌ [SCHEMA] Schema initialization failed: {e}")
        conn.rollback()
    finally:
        if cur:
            cur.close()
        conn.close()


# ---------------- Core ops ----------------

def upsert_user(user: Dict[str, Any]) -> bool:
    logger.debug(f"[UPSERT_USER] Upserting user: user_id={user['user_id']}")
    conn = get_db_connection()
    if not conn:
        logger.error(f"[UPSERT_USER] No DB connection")
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO support_users (user_id, handle, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET
                handle = EXCLUDED.handle,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                updated_at = now()
            """,
            (user["user_id"], user.get("username"), user.get("first_name"), user.get("last_name")),
        )
        conn.commit()
        logger.info(f"✅ [UPSERT_USER] User upserted: user_id={user['user_id']}")
        return True
    except Exception as e:
        logger.exception(f"❌ [UPSERT_USER] Failed to upsert user: user_id={user.get('user_id')}: {e}")
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        conn.close()


def resolve_tenant_id_by_chat(chat_id: int) -> Optional[int]:
    logger.debug(f"[RESOLVE_TENANT] Resolving tenant for chat: chat_id={chat_id}")
    conn = get_db_connection()
    if not conn:
        logger.error(f"[RESOLVE_TENANT] No DB connection")
        return None
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT tenant_id FROM tenant_groups WHERE chat_id=%s LIMIT 1", (chat_id,))
        row = cur.fetchone()
        tenant_id = int(row["tenant_id"]) if row else None
        if tenant_id:
            logger.debug(f"[RESOLVE_TENANT] ✅ Resolved tenant_id={tenant_id} for chat_id={chat_id}")
        else:
            logger.debug(f"[RESOLVE_TENANT] No tenant found for chat_id={chat_id}")
        return tenant_id
    except Exception as e:
        logger.exception(f"❌ [RESOLVE_TENANT] Failed to resolve tenant for chat_id={chat_id}: {e}")
        return None
    finally:
        if cur:
            cur.close()
        conn.close()


def ensure_tenant_for_chat(chat_id: int, title: Optional[str] = None, slug: Optional[str] = None) -> int:
    logger.info(f"[ENSURE_TENANT] Ensuring tenant for chat_id={chat_id} title={title}")
    existing = resolve_tenant_id_by_chat(chat_id)
    if existing:
        logger.debug(f"[ENSURE_TENANT] Tenant already exists: tenant_id={existing}")
        return existing

    conn = get_db_connection()
    if not conn:
        logger.error(f"[ENSURE_TENANT] No DB connection")
        raise RuntimeError("No DB connection")

    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        gen_slug = (slug or f"tg-{chat_id}").lower().replace(" ", "-")
        logger.debug(f"[ENSURE_TENANT] Generated slug: {gen_slug}")

        # Create tenant if missing
        logger.debug(f"[ENSURE_TENANT] Creating or finding tenant with slug={gen_slug}")
        cur.execute(
            """
            INSERT INTO tenants (slug, name)
            VALUES (%s, %s)
            ON CONFLICT (slug) DO NOTHING
            RETURNING id
            """,
            (gen_slug, title or gen_slug),
        )
        row = cur.fetchone()
        if row and row.get("id"):
            tid = int(row["id"])
            logger.debug(f"[ENSURE_TENANT] Created new tenant: tenant_id={tid}")
        else:
            cur.execute("SELECT id FROM tenants WHERE slug=%s LIMIT 1", (gen_slug,))
            row2 = cur.fetchone()
            if not row2:
                logger.error(f"[ENSURE_TENANT] Failed to create or find tenant for slug={gen_slug}")
                raise RuntimeError("Failed to create or find tenant")
            tid = int(row2["id"])
            logger.debug(f"[ENSURE_TENANT] Found existing tenant: tenant_id={tid}")

        # Create mapping
        logger.debug(f"[ENSURE_TENANT] Creating tenant_group mapping: tenant_id={tid} chat_id={chat_id}")
        cur.execute(
            """
            INSERT INTO tenant_groups (tenant_id, chat_id, title)
            VALUES (%s, %s, %s)
            ON CONFLICT (chat_id) DO NOTHING
            """,
            (tid, chat_id, title),
        )

        conn.commit()
        logger.info(f"✅ [ENSURE_TENANT] Tenant ensured: tenant_id={tid} chat_id={chat_id}")
        return tid
    except Exception as e:
        logger.exception(f"❌ [ENSURE_TENANT] Failed to ensure tenant for chat_id={chat_id}: {e}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        conn.close()


def create_ticket(user_id: int, category: str, subject: str, body: str, tenant_id: Optional[int] = None) -> Optional[int]:
    logger.info(f"[CREATE_TICKET] Creating ticket - user_id={user_id} category={category} tenant_id={tenant_id}")
    conn = get_db_connection()
    if not conn:
        logger.error(f"[CREATE_TICKET] No DB connection")
        return None
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        logger.debug(f"[CREATE_TICKET] Inserting ticket record")
        cur.execute(
            """
            INSERT INTO support_tickets (user_id, tenant_id, category, subject)
            VALUES (%s, %s, %s, %s)
            RETURNING id
            """,
            (user_id, tenant_id, category, subject),
        )
        row = cur.fetchone()
        tid = int(row["id"])
        logger.debug(f"[CREATE_TICKET] Ticket created with id={tid}")

        logger.debug(f"[CREATE_TICKET] Inserting initial message")
        cur.execute(
            """
            INSERT INTO support_messages (ticket_id, author_user_id, is_public, text)
            VALUES (%s, %s, TRUE, %s)
            """,
            (tid, user_id, body),
        )

        conn.commit()
        logger.info(f"✅ [CREATE_TICKET] Ticket created successfully: id={tid} user_id={user_id}")
        return tid
    except Exception as e:
        logger.exception(f"❌ [CREATE_TICKET] Failed to create ticket for user_id={user_id}: {e}")
        conn.rollback()
        return None
    finally:
        if cur:
            cur.close()
        conn.close()


def get_my_tickets(user_id: int, limit: int = 30, tenant_id: Optional[int] = None) -> List[Dict[str, Any]]:
    logger.debug(f"[GET_TICKETS] Fetching tickets - user_id={user_id} limit={limit} tenant_id={tenant_id}")
    conn = get_db_connection()
    if not conn:
        logger.error(f"[GET_TICKETS] No DB connection")
        return []
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if tenant_id is not None:
            logger.debug(f"[GET_TICKETS] Fetching tickets with tenant filter")
            cur.execute(
                """
                SELECT id, category, subject, status, created_at, closed_at
                FROM support_tickets
                WHERE user_id=%s AND (tenant_id=%s OR tenant_id IS NULL)
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (user_id, tenant_id, limit),
            )
        else:
            logger.debug(f"[GET_TICKETS] Fetching all tickets (no tenant filter)")
            cur.execute(
                """
                SELECT id, category, subject, status, created_at, closed_at
                FROM support_tickets
                WHERE user_id=%s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
        rows = cur.fetchall()
        result = [dict(r) for r in rows]
        logger.info(f"✅ [GET_TICKETS] Retrieved {len(result)} tickets for user_id={user_id}")
        return result
    except Exception as e:
        logger.exception(f"❌ [GET_TICKETS] Failed to get tickets for user_id={user_id}: {e}")
        return []
    finally:
        if cur:
            cur.close()
        conn.close()


def get_ticket(user_id: int, ticket_id: int) -> Optional[Dict[str, Any]]:
    logger.debug(f"[GET_TICKET] Fetching ticket - ticket_id={ticket_id} user_id={user_id}")
    conn = get_db_connection()
    if not conn:
        logger.error(f"[GET_TICKET] No DB connection")
        return None
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        logger.debug(f"[GET_TICKET] Querying ticket record")
        cur.execute(
            """
            SELECT id, user_id, category, subject, status, created_at, closed_at
            FROM support_tickets
            WHERE id=%s AND user_id=%s
            """,
            (ticket_id, user_id),
        )
        ticket = cur.fetchone()
        if not ticket:
            logger.warning(f"[GET_TICKET] Ticket not found or access denied: ticket_id={ticket_id} user_id={user_id}")
            return None

        logger.debug(f"[GET_TICKET] Fetching messages for ticket_id={ticket_id}")
        cur.execute(
            """
            SELECT id, author_user_id, is_public, text, attachments, created_at
            FROM support_messages
            WHERE ticket_id=%s AND is_public=TRUE
            ORDER BY created_at ASC
            """,
            (ticket_id,),
        )
        msgs = cur.fetchall()
        out = dict(ticket)
        out["messages"] = [dict(m) for m in msgs]
        logger.info(f"✅ [GET_TICKET] Retrieved ticket {ticket_id} with {len(msgs)} messages for user {user_id}")
        return out
    except Exception as e:
        logger.exception(f"❌ [GET_TICKET] Failed to get ticket {ticket_id} for user {user_id}: {e}")
        return None
    finally:
        if cur:
            cur.close()
        conn.close()


def add_public_message(user_id: int, ticket_id: int, text: str) -> bool:
    logger.debug(f"[ADD_MESSAGE] Adding message to ticket - ticket_id={ticket_id} user_id={user_id} text_len={len(text)}")
    conn = get_db_connection()
    if not conn:
        logger.error(f"[ADD_MESSAGE] No DB connection")
        return False
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        logger.debug(f"[ADD_MESSAGE] Verifying ticket ownership")
        cur.execute("SELECT user_id FROM support_tickets WHERE id=%s", (ticket_id,))
        row = cur.fetchone()
        if not row or int(row["user_id"]) != int(user_id):
            logger.warning(f"[ADD_MESSAGE] Access denied - ticket_id={ticket_id} user_id={user_id}")
            return False

        logger.debug(f"[ADD_MESSAGE] Inserting message record")
        cur.execute(
            """
            INSERT INTO support_messages (ticket_id, author_user_id, is_public, text)
            VALUES (%s, %s, TRUE, %s)
            """,
            (ticket_id, user_id, text),
        )
        cur.execute("UPDATE support_tickets SET updated_at=now() WHERE id=%s", (ticket_id,))
        conn.commit()
        logger.info(f"✅ [ADD_MESSAGE] Message added to ticket {ticket_id} by user {user_id}")
        return True
    except Exception as e:
        logger.exception(f"❌ [ADD_MESSAGE] Failed to add message to ticket {ticket_id} for user {user_id}: {e}")
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        conn.close()


def kb_search(query: str, limit: int = 8) -> List[Dict[str, Any]]:
    logger.debug(f"[KB_SEARCH] Searching KB - query_len={len(query)} limit={limit}")
    if not query or len(query) < 2:
        logger.warning(f"[KB_SEARCH] Invalid query: too short")
        return []
    conn = get_db_connection()
    if not conn:
        logger.error(f"[KB_SEARCH] No DB connection")
        return []
    cur = None
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        like = f"%{query}%"
        logger.debug(f"[KB_SEARCH] Executing search with LIKE pattern")
        cur.execute(
            """
            SELECT id, title, left(body, 200) AS snippet, tags
            FROM kb_articles
            WHERE title ILIKE %s OR body ILIKE %s
            ORDER BY score DESC, updated_at DESC
            LIMIT %s
            """,
            (like, like, limit),
        )
        rows = cur.fetchall()
        result = [dict(r) for r in rows]
        logger.info(f"✅ [KB_SEARCH] Found {len(result)} KB articles for query: {query[:50]}")
        return result
    except Exception as e:
        logger.exception(f"❌ [KB_SEARCH] KB search failed for query: {query}: {e}")
        return []
    finally:
        if cur:
            cur.close()
        conn.close()
