"""Affiliate MiniApp - Referral Dashboard"""

import logging
import os
import json
from aiohttp import web
from .database import (
    get_referral_stats, request_payout, get_pending_payouts,
    create_referral, get_tier_info, verify_ton_wallet, complete_payout
)

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
    # ensure CORS is enabled for MiniApp calls from GitHub Pages / Telegram WebView
    if cors_middleware not in getattr(webapp, "middlewares", []):
        webapp.middlewares.append(cors_middleware)

    webapp.router.add_get("/api/affiliate/stats", get_stats)
    webapp.router.add_post("/api/affiliate/stats", get_stats)
    webapp.router.add_post("/api/affiliate/payout", request_payout_route)
    webapp.router.add_post("/api/affiliate/pending", get_pending)
    webapp.router.add_post("/api/affiliate/ton-verify", verify_wallet)
    webapp.router.add_post("/api/affiliate/claim", claim_rewards)
    webapp.router.add_post("/api/affiliate/complete-payout", complete_payout_route)

    # Optional: serve TonConnect manifest + HTML when hosted on the same backend
    webapp.router.add_get("/tonconnect-manifest.json", get_tonconnect_manifest)
    webapp.router.add_get("/miniapp/appaffiliate.html", get_affiliate_app)

    logger.info("Affiliate miniapp routes registered")


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
        
        if not referrer_id:
            return web.json_response(
                {'success': False, 'error': 'Referrer ID erforderlich'},
                status=400
            )
        
        referrer_id = int(referrer_id)
        stats = get_referral_stats(referrer_id)
        tier_info = get_tier_info(referrer_id)
        
        return web.json_response({
            'success': True,
            'stats': stats,
            'tier': tier_info,
            'referral_link': f"https://t.me/{AFFILIATE_BOT_USERNAME}?start=aff_{referrer_id}",
            'minimum_payout': MINIMUM_PAYOUT,
            'emrd_contract': EMRD_CONTRACT
        })
    except Exception as e:
        logger.error(f"Get stats error: {e}")
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
        
        if not referrer_id or not amount:
            return web.json_response(
                {'success': False, 'error': 'Erforderliche Felder fehlen'},
                status=400
            )
        
        if amount < MINIMUM_PAYOUT:
            return web.json_response(
                {'success': False, 'error': f'Minimum {MINIMUM_PAYOUT} EMRD erforderlich'},
                status=400
            )
        
        # Verify wallet if provided and include it on the payout record
        success = request_payout(referrer_id, amount, wallet_address=wallet_address or None)
        
        if success:
            logger.info(f"Payout requested: {referrer_id} - {amount} EMRD")
        
        return web.json_response({
            'success': success,
            'message': 'Auszahlung angefordert' if success else 'Fehler bei Auszahlung'
        })
    except Exception as e:
        logger.error(f"Request payout error: {e}")
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def get_pending(request):
    """POST /api/affiliate/pending - Get pending payouts"""
    try:
        data = await request.json()
        referrer_id = int(data.get('referrer_id'))
        
        if not referrer_id:
            return web.json_response(
                {'success': False, 'error': 'Referrer ID erforderlich'},
                status=400
            )
        
        payouts = get_pending_payouts(referrer_id)
        
        return web.json_response({
            'success': True,
            'payouts': payouts
        })
    except Exception as e:
        logger.error(f"Get pending error: {e}")
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
        
        if not referrer_id or not wallet_address:
            return web.json_response(
                {'success': False, 'error': 'Wallet-Daten erforderlich'},
                status=400
            )
        
        success = verify_ton_wallet(referrer_id, wallet_address)
        
        return web.json_response({
            'success': success,
            'message': 'Wallet verifiziert' if success else 'Fehler'
        })
    except Exception as e:
        logger.error(f"Verify wallet error: {e}")
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

        if not referrer_id:
            return web.json_response(
                {'success': False, 'error': 'Referrer ID erforderlich'},
                status=400
            )

        if not wallet_address:
            return web.json_response(
                {'success': False, 'error': 'Wallet-Adresse erforderlich (TON Connect)'},
                status=400
            )

        # Persist wallet verification
        verify_ton_wallet(referrer_id, wallet_address)

        logger.info(f"Claim request for {referrer_id} wallet={wallet_address}")

        return web.json_response({
            'success': True,
            'message': 'Claim Anfrage gespeichert'
        })
    except Exception as e:
        logger.error(f"Claim rewards error: {e}")
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
        
        if not payout_id or not tx_hash:
            return web.json_response(
                {'success': False, 'error': 'Daten erforderlich'},
                status=400
            )
        
        success = complete_payout(payout_id, tx_hash)
        
        return web.json_response({
            'success': success,
            'message': 'Auszahlung abgeschlossen' if success else 'Fehler'
        })
    except Exception as e:
        logger.error(f"Complete payout error: {e}")
        return web.json_response(
            {'success': False, 'error': str(e)},
            status=400
        )


async def get_tonconnect_manifest(request):
    """GET /tonconnect-manifest.json - Serve TonConnect manifest"""
    manifest = {
        "url": PUBLIC_BASE_URL,
        "name": "Emerald Affiliate",
        "iconUrl": f"{PUBLIC_BASE_URL}/assets/logo.png"
    }
    return web.json_response(manifest)


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
