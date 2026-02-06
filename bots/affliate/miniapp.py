"""Affiliate MiniApp - Referral Dashboard"""

import logging
import os
from aiohttp import web
from .database import (
    get_referral_stats, request_payout, get_pending_payouts,
    get_tier_info, verify_ton_wallet, complete_payout,
    ensure_commission_row
)
import psycopg2

logger = logging.getLogger(__name__)

AFFILIATE_BOT_USERNAME = os.getenv("AFFILIATE_BOT_USERNAME", os.getenv("BOT_USERNAME", "emerald_affiliate_bot")).lstrip("@")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

MINIMUM_PAYOUT = int(os.getenv("AFFILIATE_MINIMUM_PAYOUT", "1000"))
EMRD_CONTRACT = os.getenv("EMRD_CONTRACT", "EQA0rJDTy_2sS30KxQW8HO0_ERqmOGUhMWlwdL-2RpDmCrK5")

@web.middleware
async def cors_middleware(request, handler):
    # Handle CORS preflight (needed because fetch uses application/json)
    if request.method == "OPTIONS":
        resp = web.Response(status=204)
    else:
        resp = await handler(request)

    origin = request.headers.get("Origin", "*")
    resp.headers["Access-Control-Allow-Origin"] = origin
    resp.headers["Vary"] = "Origin"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    resp.headers["Access-Control-Max-Age"] = "86400"
    return resp

async def register_miniapp(webapp):
    """Register Affiliate miniapp routes"""
    logger.info("[MINIAPP_INIT] Starting affiliate miniapp registration...")
    
    # ensure CORS is enabled for MiniApp calls from GitHub Pages / Telegram WebView
    # Check if any CORS middleware already exists
    has_cors = any(
        'cors_middleware' in str(m) or 'Access-Control' in str(m)
        for m in getattr(webapp, "middlewares", [])
    )
    
    if not has_cors:
        logger.debug("[MINIAPP_INIT] Adding CORS middleware")
        webapp.middlewares.insert(0, cors_middleware)  # Add at front
    else:
        logger.debug("[MINIAPP_INIT] CORS middleware already present")

    logger.debug("[MINIAPP_INIT] Registering API routes...")
    try:
        webapp.router.add_get("/api/affiliate/stats", get_stats)
        webapp.router.add_post("/api/affiliate/stats", get_stats)
        webapp.router.add_post("/api/affiliate/referrals", get_referral_list)
        webapp.router.add_post("/api/affiliate/payout", request_payout_route)
        webapp.router.add_post("/api/affiliate/pending", get_pending)
        webapp.router.add_post("/api/affiliate/ton-verify", verify_wallet)
        webapp.router.add_post("/api/affiliate/claim", claim_rewards)
        webapp.router.add_post("/api/affiliate/complete-payout", complete_payout_route)
        logger.debug("[MINIAPP_INIT] API routes registered")

        # Optional: serve TonConnect manifest + HTML when hosted on the same backend
        webapp.router.add_get("/tonconnect-manifest.json", get_tonconnect_manifest)
        webapp.router.add_get("/miniapp/appaffiliate.html", get_affiliate_app)
        logger.debug("[MINIAPP_INIT] Manifest and HTML routes registered")

        logger.info("✅ [MINIAPP_INIT] Affiliate miniapp routes registered successfully")
    except Exception as e:
        logger.error(f"❌ [MINIAPP_INIT] Failed to register routes: {e}", exc_info=True)


async def register_miniapp_routes(webapp):
    """Alias for compatibility"""
    await register_miniapp(webapp)


async def get_stats(request):
    """GET/POST /api/affiliate/stats - Get referral stats"""
    try:
        if request.method == "POST":
            data = await request.json()
        else:
            data = dict(request.rel_url.query)
        
        referrer_id = data.get('referrer_id')
        logger.debug(f"[API_STATS] Request for referrer_id={referrer_id}")
        
        if not referrer_id:
            logger.warning("[API_STATS] Missing referrer_id")
            return web.json_response(
                {'success': False, 'error': 'Referrer ID erforderlich'},
                status=400
            )
        
        referrer_id = int(referrer_id)
        ensure_commission_row(referrer_id)
        stats = get_referral_stats(referrer_id)
        tier_info = get_tier_info(referrer_id)
        logger.debug(f"[API_STATS] Retrieved stats: {stats}")
        
        return web.json_response({
            'success': True,
            'stats': stats,
            'tier': tier_info,
            'referral_link': f"https://t.me/{AFFILIATE_BOT_USERNAME}?start=aff_{referrer_id}",
            'minimum_payout': MINIMUM_PAYOUT,
            'emrd_contract': EMRD_CONTRACT
        })
    except Exception as e:
        logger.error(f"[API_STATS] Error: {e}", exc_info=True)
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def request_payout_route(request):
    """POST /api/affiliate/payout - Request payout"""
    try:
        data = await request.json()
        referrer_id = int(data.get('referrer_id'))
        amount = float(data.get('amount'))
        wallet_address = data.get('wallet_address', '')
        
        logger.info(f"[PAYOUT_REQUEST] referrer_id={referrer_id} amount={amount}")
        
        if not referrer_id or not amount:
            logger.warning("[PAYOUT_REQUEST] Missing required fields")
            return web.json_response(
                {'success': False, 'error': 'Erforderliche Felder fehlen'},
                status=400
            )
        
        if amount < MINIMUM_PAYOUT:
            logger.warning(f"[PAYOUT_REQUEST] Amount {amount} below minimum {MINIMUM_PAYOUT}")
            return web.json_response(
                {'success': False, 'error': f'Minimum {MINIMUM_PAYOUT} EMRD erforderlich'},
                status=400
            )
        
        # Verify wallet if provided and include it on the payout record
        success = request_payout(referrer_id, amount, wallet_address=wallet_address or None)
        
        if success:
            logger.info(f"✅ [PAYOUT_REQUEST] Payout requested: referrer={referrer_id} amount={amount}")
        else:
            logger.warning(f"❌ [PAYOUT_REQUEST] Payout failed for referrer={referrer_id}")
        
        return web.json_response({
            'success': success,
            'message': 'Auszahlung angefordert' if success else 'Fehler bei Auszahlung'
        })
    except Exception as e:
        logger.error(f"[PAYOUT_REQUEST] Error: {e}", exc_info=True)
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def get_referral_list(request):
    """POST /api/affiliate/referrals - Get referrals list"""
    try:
        data = await request.json()
        referrer_id = int(data.get('referrer_id'))
        logger.debug(f"[API_REFERRALS] Request for referrer_id={referrer_id}")
        
        if not referrer_id:
            return web.json_response(
                {'success': False, 'error': 'Referrer ID erforderlich'},
                status=400
            )
        
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        if not conn:
            return web.json_response(
                {'success': False, 'error': 'Datenbankverbindung fehlgeschlagen'},
                status=500
            )
        
        cur = conn.cursor()
        
        cur.execute("""
            SELECT r.id, r.referral_id, r.status, r.created_at
            FROM aff_referrals r
            WHERE r.referrer_id = %s
            ORDER BY r.created_at DESC
            LIMIT 50
        """, (referrer_id,))
        
        rows = cur.fetchall()
        referrals = [
            {
                'id': row[0],
                'referral_id': row[1],
                'status': row[2],
                'created_at': row[3].isoformat() if row[3] else None
            }
            for row in rows
        ]
        cur.close()
        conn.close()
        
        logger.debug(f"[API_REFERRALS] Found {len(referrals)} referrals")
        return web.json_response({'success': True, 'referrals': referrals})
    except Exception as e:
        logger.error(f"[API_REFERRALS] Error: {e}", exc_info=True)
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def get_pending(request):
    """POST /api/affiliate/pending - Get pending payouts"""
    try:
        data = await request.json()
        referrer_id = int(data.get('referrer_id'))
        logger.debug(f"[API_PENDING] Request for referrer_id={referrer_id}")
        
        if not referrer_id:
            logger.warning("[API_PENDING] Missing referrer_id")
            return web.json_response(
                {'success': False, 'error': 'Referrer ID erforderlich'},
                status=400
            )
        
        payouts = get_pending_payouts(referrer_id)
        logger.debug(f"[API_PENDING] Found {len(payouts)} pending payouts for {referrer_id}")
        
        return web.json_response({
            'success': True,
            'payouts': payouts
        })
    except Exception as e:
        logger.error(f"[API_PENDING] Error: {e}", exc_info=True)
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def verify_wallet(request):
    """POST /api/affiliate/ton-verify - Verify TON wallet"""
    try:
        data = await request.json()
        referrer_id = int(data.get('referrer_id'))
        wallet_address = data.get('wallet_address', '')
        
        logger.info(f"[TON_VERIFY] Verifying wallet for referrer_id={referrer_id}")
        
        if not referrer_id or not wallet_address:
            logger.warning("[TON_VERIFY] Missing wallet data")
            return web.json_response(
                {'success': False, 'error': 'Wallet-Daten erforderlich'},
                status=400
            )
        
        success = verify_ton_wallet(referrer_id, wallet_address)
        logger.info(f"{'✅' if success else '❌'} [TON_VERIFY] Wallet verification: {success}")
        
        return web.json_response({
            'success': success,
            'message': 'Wallet verifiziert' if success else 'Fehler'
        })
    except Exception as e:
        logger.error(f"[TON_VERIFY] Error: {e}", exc_info=True)
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def claim_rewards(request):
    """POST /api/affiliate/claim - Claim EMRD rewards (server-side marker).

    NOTE: Actual on-chain claiming depends on your EMRD contract flow.
    Here we at least require a TON wallet (via TonConnect) and store/verify it.
    """
    try:
        data = await request.json()
        referrer_id = int(data.get('referrer_id'))
        wallet_address = data.get('wallet_address', '')

        logger.info(f"[CLAIM_REWARDS] Claim request: referrer_id={referrer_id}")

        if not referrer_id:
            logger.warning("[CLAIM_REWARDS] Missing referrer_id")
            return web.json_response(
                {'success': False, 'error': 'Referrer ID erforderlich'},
                status=400
            )

        if not wallet_address:
            logger.warning("[CLAIM_REWARDS] Missing wallet address")
            return web.json_response(
                {'success': False, 'error': 'Wallet-Adresse erforderlich (TON Connect)'},
                status=400
            )

        # Persist wallet verification
        verify_ton_wallet(referrer_id, wallet_address)
        logger.info(f"✅ [CLAIM_REWARDS] Wallet verified for {referrer_id}")

        return web.json_response({
            'success': True,
            'message': 'Claim Anfrage gespeichert'
        })
    except Exception as e:
        logger.error(f"[CLAIM_REWARDS] Error: {e}", exc_info=True)
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def complete_payout_route(request):
    """POST /api/affiliate/complete-payout - Complete payout with tx hash"""
    try:
        data = await request.json()
        payout_id = int(data.get('payout_id'))
        tx_hash = data.get('tx_hash', '')
        
        logger.info(f"[COMPLETE_PAYOUT] payout_id={payout_id} tx_hash={tx_hash[:16]}...")
        
        if not payout_id or not tx_hash:
            logger.warning("[COMPLETE_PAYOUT] Missing required fields")
            return web.json_response(
                {'success': False, 'error': 'Daten erforderlich'},
                status=400
            )
        
        success = complete_payout(payout_id, tx_hash)
        logger.info(f"{'✅' if success else '❌'} [COMPLETE_PAYOUT] Payout completed: {success}")
        
        return web.json_response({
            'success': success,
            'message': 'Auszahlung abgeschlossen' if success else 'Fehler'
        })
    except Exception as e:
        logger.error(f"[COMPLETE_PAYOUT] Error: {e}", exc_info=True)
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def get_tonconnect_manifest(request):
    """GET /tonconnect-manifest.json - Serve TonConnect manifest"""
    try:
        base_url = PUBLIC_BASE_URL or request.url.scheme + "://" + request.url.host
        manifest = {
            "url": base_url,
            "name": "Emerald Affiliate",
            "iconUrl": f"{base_url}/assets/logo.png"
        }
        logger.debug(f"[TONCONNECT_MANIFEST] Serving manifest with url={base_url}")
        return web.json_response(manifest)
    except Exception as e:
        logger.error(f"[TONCONNECT_MANIFEST] Error: {e}", exc_info=True)
        return web.json_response(
            {"url": "https://localhost", "name": "Emerald Affiliate",
             "iconUrl": "https://localhost/assets/logo.png"}
        )


async def get_affiliate_app(request):
    """GET /miniapp/appaffiliate.html - Serve affiliate app HTML"""
    try:
        app_path = os.path.join(
            os.path.dirname(__file__),
            '../../Emerald_Content/miniapp/appaffiliate.html'
        )
        if os.path.exists(app_path):
            with open(app_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            return web.Response(text=html_content, content_type='text/html')
        return web.Response(text='Affiliate app not found', status=404)
    except Exception as e:
        logger.error(f"Error serving affiliate app: {e}")
        return web.Response(text='Error loading affiliate app', status=500)
