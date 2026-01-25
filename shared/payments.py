# payments.py - Zentrale Zahlungsintegration für alle Bots
import logging
import os
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logger = logging.getLogger(__name__)

try:
    from bots.content.database import (
        ensure_payments_schema, create_payment_order, mark_payment_paid,
        set_pro_until, get_subscription_info
    )
except ImportError:
    # Fallback wenn database nicht verfügbar (z.B. in anderen Bots)
    def ensure_payments_schema(*args, **kwargs): pass
    def create_payment_order(*args, **kwargs): pass
    def mark_payment_paid(*args, **kwargs): return False, None, None
    def set_pro_until(*args, **kwargs): pass
    def get_subscription_info(*args, **kwargs): return {}

WEBSITE  = "https://greeny187.github.io/EmeraldContentBots/"
SUPPORT  = "https://t.me/+DkUfIvjyej8zNGVi"
TON_WALLET = os.getenv("TON_WALLET_ADDRESS", "UQBopac1WFJGC_K48T8T8...")

# WalletConnect Konfiguration
WALLETCONNECT_PROJECT_ID = os.getenv("WALLETCONNECT_PROJECT_ID", "")
WALLETCONNECT_ICON = "https://emeraldcontent.com/icon.png"  # Bot Icon URL

PROVIDERS = {
    "walletconnect_ton": {
        "label": "TonConnect (TON / EMRD)",
        "wallet": TON_WALLET,
    },
    "paypal": {
        "label": "PayPal",
        "link_base": os.getenv("PAYPAL_LINK_BASE"),
    },
}

PLANS = {
    "pro_monthly": {
        "label": "Pro (1 Monat)",
        "months": 1,
        "price_eur": os.getenv("PRO_PRICE_EUR", "4.95"),
        "price_usd": "5.49",  # grob, nur Info
    },
    "pro_quarterly": {
        "label": "Pro (3 Monate)",
        "months": 3,
        "price_eur": os.getenv("PRO_QUARTER_EUR", "13.95"),
        "price_usd": "15.49",
    },
    "pro_yearly": {
        "label": "Pro (12 Monate)",
        "months": 12,
        "price_eur": os.getenv("PRO_YEAR_EUR", "49.95"),
        "price_usd": "54.99",
    },
}


def _build_link(link_base:str, order_id:str, price:str)->str:
    """Generic payment link builder."""
    sep = "&" if "?" in (link_base or "") else "?"
    return f"{link_base}{sep}ref={order_id}&amount={price}"

def build_walletconnect_uri(provider: str, amount_eur: str, order_id: str) -> str:
    """
    Erstelle WalletConnect URI für TON Wallets.
    Format für WalletConnect Deep Link:
    - TON: tonclient://transfer?address=<addr>&amount=<nano>&text=<text>
    - Allgemein: wc://...<project_id>
    """
    if provider == "walletconnect_ton":
        # TON nanograms (1 TON = 1e9 nano)
        ton_amount = str(int(float(amount_eur) * 4))  # Vereinfachte Konvertierung
        return f"ton://transfer?address={TON_WALLET}&amount={ton_amount}&text=Order%20{order_id}"
    return ""

def create_checkout(chat_id: int, provider: str, plan_key: str, user_id: int, webhook_url: str = "") -> dict:

    ensure_payments_schema()
    plan = PLANS.get(plan_key, {})
    if not plan:
        return {"error": f"Unknown plan: {plan_key}"}
    
    order_id = str(uuid.uuid4())
    price_eur = str(plan.get("price_eur", "4.99"))
    months = int(plan.get("months", 1))
    
    # Speichere Order
    try:
        create_payment_order(order_id, chat_id, provider, plan_key, price_eur, months, user_id)
    except Exception as e:
        logger.warning(f"[checkout] Failed to create payment order: {e}")
    
    if provider == "walletconnect_ton":
        uri = build_walletconnect_uri(provider, price_eur, order_id)
        return {
            "provider": provider,
            "order_id": order_id,
            "price": price_eur,
            "months": months,
            "uri": uri,  # Deep Link für Wallet
            "label": PROVIDERS[provider]["label"]
        }
    
    elif provider == "paypal":
        pb = PROVIDERS.get(provider, {})
        link_base = pb.get("link_base")
        if link_base:
            link = _build_link(link_base, order_id, price_eur)
        else:
            link = WEBSITE
        return {
            "provider": provider,
            "url": link,
            "order_id": order_id,
            "price": price_eur,
            "months": months
        }
    
    else:
        # Fallback für andere Provider
        pb = PROVIDERS.get(provider, {})
        link_base = pb.get("link_base")
        if link_base:
            link = _build_link(link_base, order_id, price_eur)
        else:
            link = WEBSITE
        return {
            "provider": provider,
            "url": link,
            "order_id": order_id,
            "price": price_eur,
            "months": months
        }

def build_pro_menu(chat_id: int) -> InlineKeyboardMarkup:
    """Erstelle PRO Membership Menü."""
    kb = []
    
    for key, plan in PLANS.items():
        row = []
        for prov_key, meta in PROVIDERS.items():
            if prov_key == "walletconnect_ton":
                row.append(InlineKeyboardButton("walletconnect_ton"))
            elif prov_key == "paypal":
                row.append(InlineKeyboardButton("paypal"))
        if row:
            kb.extend([row])
    
    kb.append([InlineKeyboardButton("🔎 Status prüfen", callback_data="pay:status")])
    return InlineKeyboardMarkup(kb)

def handle_webhook(provider: str, data: dict) -> bool:
    """
    Handle payment provider webhooks.
    Returns True if payment was successfully marked.
    """
    logger.info(f"[webhook] Processing {provider} webhook: {data}")
    
    # Extrahiere Order ID (unterschiedliche Namensgebung je Provider)
    order_id = data.get("order_id") or data.get("ref") or data.get("metadata", {}).get("order_id")
    if not order_id:
        logger.warning(f"[webhook] No order_id found in {provider} webhook")
        return False
    
    # Markiere als bezahlt
    ok, chat_id, months = mark_payment_paid(order_id, provider)
    if ok:
        until = datetime.now(ZoneInfo("UTC")) + timedelta(days=30*months)
        set_pro_until(chat_id, until, tier="pro")
        logger.info(f"[webhook] Payment {order_id} marked successful for chat {chat_id}, {months} months")
    else:
        logger.warning(f"[webhook] Failed to mark payment {order_id} as paid")
    
    return ok
