"""Affiliate Database - Referrals, Conversions, Commissions"""

import os
import psycopg2
from datetime import datetime
import logging

logger = logging.getLogger("bot.affiliate.database")


def get_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        logger.debug("[DB_CONNECT] Database connection established")
        return conn
    except Exception as e:
        logger.error(f"[DB_CONNECT] Database connection error: {e}", exc_info=True)
        return None


def init_all_schemas():
    """Initialize all database schemas"""
    logger.info("[DB_SCHEMA] Initializing affiliate database schemas...")
    conn = get_connection()
    if not conn:
        logger.error("[DB_SCHEMA] Failed to get database connection")
        return False
    
    try:
        cur = conn.cursor()
        
        # Referrals table
        logger.debug("[DB_SCHEMA] Creating aff_referrals table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aff_referrals (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                referral_id BIGINT NOT NULL,
                referral_link TEXT,
                status VARCHAR(50) DEFAULT 'pending',
                conversion_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(referrer_id, referral_id)
            )
        """)
        
        # Global referrer mapping (ONE referrer per user across ALL bots, first-touch wins)
        logger.debug("[DB_SCHEMA] Creating aff_user_referrers table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aff_user_referrers (
                user_id BIGINT PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                source_bot VARCHAR(64),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Conversions table (no FK to aff_referrals: referrer_id is a Telegram user id, not aff_referrals.id)
        logger.debug("[DB_SCHEMA] Creating aff_conversions table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aff_conversions (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                referral_id BIGINT NOT NULL,
                conversion_type VARCHAR(50),
                value NUMERIC(20,2),
                commission NUMERIC(20,2),
                status VARCHAR(50) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # If the table existed before with a wrong FK, drop all FKs safely
        cur.execute("""
        DO $$
        DECLARE r RECORD;
        BEGIN
            IF to_regclass('public.aff_conversions') IS NOT NULL THEN
                FOR r IN (
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = 'aff_conversions'::regclass
                      AND contype = 'f'
                ) LOOP
                    EXECUTE format('ALTER TABLE aff_conversions DROP CONSTRAINT IF EXISTS %I', r.conname);
                END LOOP;
            END IF;
        END$$;
        """)
        
        # Commissions table
        logger.debug("[DB_SCHEMA] Creating aff_commissions table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aff_commissions (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT NOT NULL UNIQUE,
                total_earned NUMERIC(20,2) DEFAULT 0,
                total_withdrawn NUMERIC(20,2) DEFAULT 0,
                pending NUMERIC(20,2) DEFAULT 0,
                tier VARCHAR(50) DEFAULT 'bronze',
                wallet_address VARCHAR(255),
                ton_connect_verified BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Payouts table
        logger.debug("[DB_SCHEMA] Creating aff_payouts table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aff_payouts (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                amount NUMERIC(20,2),
                status VARCHAR(50) DEFAULT 'pending',
                tx_hash VARCHAR(255),
                wallet_address VARCHAR(255),
                requested_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES aff_commissions(referrer_id)
            )
        """)
        
        # Helpful indexes
        logger.debug("[DB_SCHEMA] Creating indexes...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aff_referrals_referrer ON aff_referrals(referrer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aff_conversions_referrer ON aff_conversions(referrer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aff_payouts_referrer ON aff_payouts(referrer_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aff_user_referrers_referrer ON aff_user_referrers(referrer_id)")


        conn.commit()
        logger.info("✅ [DB_SCHEMA] Affiliate schemas initialized successfully")
        return True
    except Exception as e:
        logger.error(f"❌ [DB_SCHEMA] Schema initialization error: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



def ensure_commission_row(referrer_id: int) -> bool:
    """Ensure a commissions row exists for a referrer (idempotent)."""
    logger.debug(f"[DB_COMMISSION] Ensuring commission row for user {referrer_id}...")
    conn = get_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO aff_commissions (referrer_id)
            VALUES (%s)
            ON CONFLICT (referrer_id) DO NOTHING
            """,
            (referrer_id,),
        )
        conn.commit()
        logger.debug(f"[DB_COMMISSION] Commission row ensured for {referrer_id}")
        return True
    except Exception as e:
        logger.error(f"[DB_COMMISSION] Error ensuring commission row: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()



def create_referral(referrer_id, referral_id, referral_link: str | None = None):
    """Create referral"""
    logger.info(f"[DB_CREATE_REFERRAL] Creating referral: referrer={referrer_id} referral={referral_id}")
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        # make sure the referrer exists in aff_commissions
        ensure_commission_row(referrer_id)

        cur.execute("""
            INSERT INTO aff_referrals (referrer_id, referral_id, referral_link, status)
            VALUES (%s, %s, %s, 'active')
            ON CONFLICT DO NOTHING
        """, (referrer_id, referral_id, referral_link))
        
        # Global mapping: first-touch wins (do NOT overwrite existing referrer)
        cur.execute("""
            INSERT INTO aff_user_referrers (user_id, referrer_id, source_bot)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
        """, (referral_id, referrer_id, os.getenv("BOT_USERNAME")))
        
        conn.commit()
        logger.debug(f"[DB_CREATE_REFERRAL] Referral created successfully")
        return True
    except Exception as e:
        logger.error(f"[DB_CREATE_REFERRAL] Error creating referral: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_global_referrer(user_id: int) -> int | None:
    """Returns the global referrer for a user (if set)."""
    conn = get_connection()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT referrer_id FROM aff_user_referrers WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        return int(row[0]) if row else None
    except Exception as e:
        logger.error(f"get_global_referrer error: {e}")
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

def record_conversion(referrer_id, referral_id, conversion_type, value):
    """Record conversion event"""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        # Calculate commission based on current tier
        ensure_commission_row(referrer_id)
        cur.execute("SELECT total_earned FROM aff_commissions WHERE referrer_id = %s", (referrer_id,))
        row = cur.fetchone()
        total_earned = float(row[0]) if row and row[0] is not None else 0.0
        # tier thresholds are mirrored in get_tier_info(); we re-use the same logic here
        if total_earned >= 10000:
            rate = 0.20
        elif total_earned >= 5000:
            rate = 0.15
        elif total_earned >= 1000:
            rate = 0.10
        else:
            rate = 0.05
        commission = float(value) * rate
        
        cur.execute("""
            INSERT INTO aff_conversions
            (referrer_id, referral_id, conversion_type, value, commission)
            VALUES (%s, %s, %s, %s, %s)
        """, (referrer_id, referral_id, conversion_type, value, commission))
        
        # Update commission total
        cur.execute("""
            INSERT INTO aff_commissions (referrer_id, total_earned, pending)
            VALUES (%s, %s, %s)
            ON CONFLICT (referrer_id) DO UPDATE SET
                total_earned = aff_commissions.total_earned + EXCLUDED.total_earned,
                pending = aff_commissions.pending + EXCLUDED.pending,
                updated_at = NOW()
        """, (referrer_id, commission, commission))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Record conversion error: {e}")
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_referral_stats(referrer_id):
    """Get referrer stats"""
    logger.debug(f"[DB_GET_STATS] Retrieving stats for referrer {referrer_id}")
    conn = get_connection()
    if not conn:
        return {}
    
    try:
        cur = conn.cursor()
        
        ensure_commission_row(referrer_id)

        cur.execute("""
            SELECT
                COUNT(DISTINCT r.referral_id) AS total_referrals,
                COUNT(*) FILTER (WHERE r.status = 'active') AS active_referrals,
                COALESCE(SUM(c.commission), 0) AS total_commissions
            FROM aff_referrals r
            LEFT JOIN aff_conversions c
                ON c.referrer_id = r.referrer_id AND c.referral_id = r.referral_id
            WHERE r.referrer_id = %s
        """, (referrer_id,))
        
        row = cur.fetchone()
        
        cur.execute("""
            SELECT total_earned, pending, tier
            FROM aff_commissions WHERE referrer_id = %s
        """, (referrer_id,))
        
        comm_row = cur.fetchone()
        
        stats = {
            'total_referrals': row[0] if row else 0,
            'active_referrals': row[1] if row else 0,
            'total_commissions': float(row[2]) if row else 0,
            'total_earned': float(comm_row[0]) if comm_row else 0,
            'pending': float(comm_row[1]) if comm_row else 0,
            'tier': comm_row[2] if comm_row else 'bronze'
        }
        logger.debug(f"[DB_GET_STATS] Stats retrieved: {stats}")
        return stats
    except Exception as e:
        logger.error(f"[DB_GET_STATS] Error retrieving stats: {e}", exc_info=True)
        return {}
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def request_payout(referrer_id, amount, wallet_address: str | None = None):
    """Request payout.

    Requires a verified TON wallet address (stored via TON Connect).
    """
    logger.info(f"[DB_REQUEST_PAYOUT] Payout request: referrer={referrer_id} amount={amount}")
    conn = get_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()

        ensure_commission_row(referrer_id)

        # Check pending balance + wallet verification
        cur.execute(
            """
            SELECT pending, ton_connect_verified, wallet_address
            FROM aff_commissions
            WHERE referrer_id = %s
            """,
            (referrer_id,),
        )

        row = cur.fetchone()
        if not row:
            logger.warning(f"[DB_REQUEST_PAYOUT] User {referrer_id} not found")
            return False

        pending, verified, stored_wallet = row

        if pending is None or float(pending) < float(amount):
            logger.warning(f"[DB_REQUEST_PAYOUT] Insufficient balance for {referrer_id}: pending={pending} requested={amount}")
            return False

        # If caller provided a wallet, store/verify it
        if wallet_address:
            stored_wallet = wallet_address
            verified = True
            cur.execute(
                """
                UPDATE aff_commissions
                SET wallet_address=%s, ton_connect_verified=TRUE, updated_at=NOW()
                WHERE referrer_id=%s
                """,
                (wallet_address, referrer_id),
            )

        if not verified or not stored_wallet:
            logger.warning(f"[DB_REQUEST_PAYOUT] Wallet not verified for {referrer_id}")
            return False

        # Create payout request
        cur.execute(
            """
            INSERT INTO aff_payouts (referrer_id, amount, status, wallet_address)
            VALUES (%s, %s, 'pending', %s)
            """,
            (referrer_id, amount, stored_wallet),
        )

        # Update pending
        cur.execute(
            """
            UPDATE aff_commissions
            SET pending = pending - %s, updated_at = NOW()
            WHERE referrer_id = %s
            """,
            (amount, referrer_id),
        )

        conn.commit()
        logger.info(f"✅ [DB_REQUEST_PAYOUT] Payout request created successfully")
        return True
    except Exception as e:
        logger.error(f"❌ [DB_REQUEST_PAYOUT] Error: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

def get_pending_payouts(referrer_id):
    """Get pending payouts"""
    logger.debug(f"[DB_GET_PAYOUTS] Retrieving pending payouts for {referrer_id}")
    conn = get_connection()
    if not conn:
        return []
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, amount, status, requested_at
            FROM aff_payouts
            WHERE referrer_id = %s AND status = 'pending'
            ORDER BY requested_at DESC
        """, (referrer_id,))
        
        rows = cur.fetchall()
        payouts = [
            {
                'id': row[0],
                'amount': float(row[1]),
                'status': row[2],
                'requested_at': row[3].isoformat()
            }
            for row in rows
        ]
        logger.debug(f"[DB_GET_PAYOUTS] Found {len(payouts)} pending payouts")
        return payouts
    except Exception as e:
        logger.error(f"[DB_GET_PAYOUTS] Error: {e}", exc_info=True)
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def verify_ton_wallet(referrer_id, wallet_address):
    """Verify TON wallet"""
    logger.info(f"[DB_TON_VERIFY] Verifying wallet for {referrer_id}")
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO aff_commissions (referrer_id, wallet_address, ton_connect_verified)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (referrer_id) DO UPDATE SET
                wallet_address = EXCLUDED.wallet_address,
                ton_connect_verified = TRUE,
                updated_at = NOW()
        """, (referrer_id, wallet_address))
        
        conn.commit()
        logger.debug(f"[DB_TON_VERIFY] Wallet verified successfully")
        return True
    except Exception as e:
        logger.error(f"[DB_TON_VERIFY] Error: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def get_tier_info(referrer_id):
    """Get tier information"""
    logger.debug(f"[DB_GET_TIER] Retrieving tier info for {referrer_id}")
    conn = get_connection()
    if not conn:
        return None
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
            total_earned,
            CASE 
                WHEN total_earned >= 10000 THEN 'platinum'
                WHEN total_earned >= 5000 THEN 'gold'
                WHEN total_earned >= 1000 THEN 'silver'
                ELSE 'bronze'
            END as tier,
            CASE 
                WHEN total_earned >= 10000 THEN '🔶'
                WHEN total_earned >= 5000 THEN '🥇'
                WHEN total_earned >= 1000 THEN '🥈'
                ELSE '🥉'
            END as tier_emoji,
            CASE 
                WHEN total_earned >= 10000 THEN 0.20
                WHEN total_earned >= 5000 THEN 0.15
                WHEN total_earned >= 1000 THEN 0.10
                ELSE 0.05
            END as commission_rate
            FROM aff_commissions
            WHERE referrer_id = %s
        """, (referrer_id,))
        
        row = cur.fetchone()
        if row:
            tier_info = {
                'total_earned': float(row[0]),
                'tier': row[1],
                'tier_emoji': row[2],
                'commission_rate': float(row[3])
            }
            logger.debug(f"[DB_GET_TIER] Tier info: {tier_info['tier']} {tier_info['tier_emoji']}")
            return tier_info
        logger.debug(f"[DB_GET_TIER] No tier info found for {referrer_id}")
        return None
    except Exception as e:
        logger.error(f"[DB_GET_TIER] Error: {e}", exc_info=True)
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def complete_payout(payout_id, tx_hash):
    """Complete payout with transaction hash"""
    logger.info(f"[DB_COMPLETE_PAYOUT] Completing payout {payout_id} with tx={tx_hash[:16]}...")
    conn = get_connection()
    if not conn:
        return False
    
    try:
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE aff_payouts SET
            status = 'completed',
            tx_hash = %s,
            completed_at = NOW()
            WHERE id = %s
        """, (tx_hash, payout_id))
        
        # Get referrer_id and amount
        cur.execute("""
            SELECT referrer_id, amount FROM aff_payouts WHERE id = %s
        """, (payout_id,))
        
        row = cur.fetchone()
        if row:
            referrer_id, amount = row
            logger.debug(f"[DB_COMPLETE_PAYOUT] Updating withdrawn amount for {referrer_id}")
            # Update total_withdrawn
            cur.execute("""
                UPDATE aff_commissions SET
                total_withdrawn = total_withdrawn + %s
                WHERE referrer_id = %s
            """, (amount, referrer_id))
        
        conn.commit()
        logger.info(f"✅ [DB_COMPLETE_PAYOUT] Payout completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ [DB_COMPLETE_PAYOUT] Error: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
