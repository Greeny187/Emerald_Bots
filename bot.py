import os
import sys
import asyncio
import logging
import pathlib
from typing import Dict
from importlib import import_module
from aiohttp import web
from telegram import Update
from telegram.ext import Application, PicklePersistence

# Support Bot API
try:
    from bots.support.support_api import router as support_router
except Exception as e:
    logging.warning(f"Support API import failed: {e}")
    support_router = None

try:
    from bots.content.miniapp import register_miniapp_routes as _register_content_miniapp_routes
except Exception:
    _register_content_miniapp_routes = None
try:
    from bots.crossposter.miniapp import register_miniapp_routes as _register_crossposter_miniapp_routes
except Exception:
    _register_crossposter_miniapp_routes = None
try:
    from bots.trade_api.miniapp import register_miniapp as _register_tradeapi_miniapp
except Exception:
    _register_tradeapi_miniapp = None
try:
    from bots.trade_dex.miniapp import register_miniapp as _register_tradedex_miniapp
except Exception:
    _register_tradedex_miniapp = None
try:
    from bots.learning.miniapp import register_miniapp as _register_learning_miniapp
except Exception:
    _register_learning_miniapp = None
try:
    from bots.dao.miniapp import register_miniapp as _register_dao_miniapp
except Exception:
    _register_dao_miniapp = None
try:
    from bots.affliate.miniapp import register_miniapp as _register_affiliate_miniapp
except Exception as e:
    logging.error(f"❌ Failed to import affiliate miniapp: {e}", exc_info=True)
    _register_affiliate_miniapp = None


DEFAULT_BOT_NAMES = ["content", "trade_api", "trade_dex", "crossposter", "learning", "support", "dao", "affliate"]
APP_BASE_URL = os.getenv("APP_BASE_URL")
PORT = int(os.getenv("PORT", "8443"))
DEVELOPER_CHAT_ID = os.getenv("DEVELOPER_CHAT_ID", "5114518219")

# Root in sys.path sichern (für shared/* Importe)
ROOT = pathlib.Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def _install_log_security() -> None:
    """Reduziert das Risiko, dass Tokens/Secrets in Logs landen (z.B. Telegram Bot Token in URLs)."""
    import re

    class _RedactTelegramBotTokenFilter(logging.Filter):
        _pat = re.compile(r"(https://api\.telegram\.org/bot)([^/\s]+)")

        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.getMessage()
                redacted = self._pat.sub(r"\1<REDACTED>", msg)
                record.msg = redacted
                record.args = ()
            except Exception:
                pass
            return True

    # httpx/httpcore auf WARNING, damit keine Request-URLs (mit Token) geloggt werden
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    for h in logging.getLogger().handlers:
        h.addFilter(_RedactTelegramBotTokenFilter())


def load_bots_env():
    bots = []
    for idx, name in enumerate(DEFAULT_BOT_NAMES, start=1):
        key = os.getenv(f"BOT{idx}_KEY", name)
        token = os.getenv(f"BOT{idx}_TOKEN")
        username = os.getenv(f"BOT{idx}_USERNAME")  # optional
        bots.append({
            "name": name,
            "route_key": key,
            "token": token,
            "username": username,
            "index": idx
        })
    return bots

BOTS = load_bots_env()

def mask(value: str, show: int = 6):
    if not value:
        return None
    if len(value) <= show:
        return "*" * len(value)
    return "*" * (len(value) - show) + value[-show:]

def sanitize_env() -> Dict[str, str]:
    safe: Dict[str, str] = {}
    # Nur Meta/Runtime Infos – alles andere lieber nicht exposen.
    allowed_exact = {
        "APP_ENV", "APP_NAME", "HEROKU_APP_NAME", "HEROKU_RELEASE_VERSION",
        "HEROKU_SLUG_COMMIT", "DYNO", "PORT", "WEB_CONCURRENCY", "TZ"
    }
    allowed_prefixes = ("APP_", "HEROKU_",)
    for k, v in os.environ.items():
        if k in allowed_exact or k.startswith(allowed_prefixes):
            # Immer als String, None -> ""
            safe[k] = str(v or "")
    return safe

APPLICATIONS: Dict[str, Application] = {}
ROUTEKEY_TO_NAME: Dict[str, str] = {}
WEBHOOK_URLS: Dict[str, str] = {}

async def build_application(bot_cfg: Dict, is_primary: bool) -> Application:
    name = bot_cfg["name"]
    route_key = bot_cfg["route_key"]
    token = bot_cfg["token"]

    persistence = PicklePersistence(filepath=f"state_{route_key}.pickle")
    app_builder = (Application.builder().token(token).arbitrary_callback_data(True).persistence(persistence))
    
    # Optional: HTTPX-Request aus shared.network (empfohlen)
    if name == "content" and os.getenv("SKIP_CUSTOM_REQUEST", "0") != "1":
        try:
            from shared.network import create_httpx_request
            app_builder = app_builder.request(create_httpx_request(pool_size=100))
            logging.info("content: custom HTTPXRequest aktiv")
        except Exception as e:
            logging.warning("content: HTTPXRequest nicht gesetzt: %s", e)

    app = app_builder.build()

    app.bot_data['bot_key'] = route_key        # z.B. "content", "trade_api", ...
    app.bot_data['bot_name'] = name            # logischer Name
    app.bot_data['bot_index'] = bot_cfg["index"]
    
    async def _on_error(update, context):
        logging.exception("Unhandled error", exc_info=context.error)
    app.add_error_handler(_on_error)

    # Bot-Plugin laden (sauber: bots.<name>.app)
    pkg = import_module(f"bots.{name}.app")
    if hasattr(pkg, "register"):
        result = pkg.register(app)
        if asyncio.iscoroutine(result):
            await result
    if is_primary and hasattr(pkg, "register_jobs"):
        result = pkg.register_jobs(app)
        if asyncio.iscoroutine(result):
            await result

    async def _post_init(application: Application) -> None:
        try:
            await application.bot.send_message(chat_id=DEVELOPER_CHAT_ID,
                text=f"🤖 Bot '{name}' ({route_key}) ist online.")
        except Exception:
            pass
    app.post_init = _post_init

    return app

async def webhook_handler(request: web.Request):
    route_key = request.match_info.get("route_key")
    app = APPLICATIONS.get(route_key)
    if not app:
        return web.Response(status=404, text="Unknown bot route key.")
    try:
        data = await request.json()
        keys = list(data.keys())
        utype = "unknown"
        if "callback_query" in data: utype = "callback_query"
        elif "message" in data: utype = "message"
        elif "edited_message" in data: utype = "edited_message"
        elif "chat_member" in data: utype = "chat_member"
        logging.info("[IN] %s type=%s keys=%s", route_key, utype, keys)
        if "callback_query" in data:
            cq = data.get("callback_query") or {}
            logging.info("[IN] %s callback data=%r from=%s",
                         route_key,
                         cq.get("data"),
                         (cq.get("from") or {}).get("id"))
    except Exception:
        return web.Response(status=400, text="Invalid JSON")

    update = Update.de_json(data=data, bot=app.bot)
    await app.process_update(update)
    return web.json_response({"ok": True})

async def health_handler(_: web.Request):
    return web.json_response({
        "status": "ok",
        "bots": list(APPLICATIONS.keys()),
        "webhook_urls": WEBHOOK_URLS
    })

async def env_handler(_: web.Request):
    return web.json_response(sanitize_env())

async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    _install_log_security()
    
    # Initialize all bot schemas
    try:
        from bots.content.database import init_all_schemas as content_init
        content_init()
    except Exception as e:
        logging.warning("Content schema init failed: %s", e)
    
    try:
        from bots.trade_api.database import init_all_schemas as tradeapi_init
        tradeapi_init()
    except Exception as e:
        logging.warning("Trade API schema init failed: %s", e)
    
    try:
        from bots.trade_dex.database import init_all_schemas as tradedex_init
        tradedex_init()
    except Exception as e:
        logging.warning("Trade DEX schema init failed: %s", e)
    
    try:
        from bots.learning.database import init_all_schemas as learning_init
        learning_init()
    except Exception as e:
        logging.warning("Learning schema init failed: %s", e)
    
    try:
        from bots.support.database import init_all_schemas as support_init
        support_init()
    except Exception as e:
        logging.warning("Support schema init failed: %s", e)
    
    try:
        from bots.dao.database import init_all_schemas as dao_init
        dao_init()
    except Exception as e:
        logging.warning("DAO schema init failed: %s", e)
    
    try:
        from bots.affliate.database import init_all_schemas as affiliate_init
        affiliate_init()
    except Exception as e:
        logging.warning("Affiliate schema init failed: %s", e)
    try:
        from bots.crossposter.database import init_all_schemas as crossposter_init
        crossposter_init()
    except Exception as e:
        logging.warning("Crossposter schema init failed: %s", e)
    
    if not BOTS or not BOTS[0]["token"]:
        raise RuntimeError("BOT1_TOKEN (Emerald Content Bot) is required.")

    if not APP_BASE_URL:
        raise RuntimeError("APP_BASE_URL must be set (e.g. https://<app>.herokuapp.com)")

    # Build + start apps
    for idx, cfg in enumerate(BOTS):
        if not cfg["token"]:
            continue
        app = await build_application(cfg, is_primary=(idx == 0))
        if idx == 0:
            webapp = app
        await app.initialize()
        await app.start()
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        webhook_url = f"{APP_BASE_URL}/webhook/{cfg['route_key']}"
        await app.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES
        )
        APPLICATIONS[cfg["route_key"]] = app
        ROUTEKEY_TO_NAME[cfg["route_key"]] = cfg["name"]
        WEBHOOK_URLS[cfg["name"]] = f"{APP_BASE_URL}/webhook/{cfg['route_key']}"

    if not APPLICATIONS:
        raise RuntimeError("No bots configured (no tokens found).")

    from devdash_api import register_devdash_routes, ensure_tables, cors_middleware
    webapp = web.Application(middlewares=[cors_middleware])
    
    # Register miniapp routes for all bots
    if _register_content_miniapp_routes and "content" in APPLICATIONS:
        try:
            _register_content_miniapp_routes(webapp, APPLICATIONS["content"])
        except TypeError:
            _register_content_miniapp_routes(webapp)
    if _register_crossposter_miniapp_routes and "crossposter" in APPLICATIONS:
        try:
            _register_crossposter_miniapp_routes(webapp, APPLICATIONS["crossposter"])
        except TypeError:
            _register_crossposter_miniapp_routes(webapp)
    if _register_tradeapi_miniapp and "trade_api" in APPLICATIONS:
        try:
            _register_tradeapi_miniapp(APPLICATIONS["trade_api"])
        except Exception as e:
            logging.warning("Trade API miniapp registration failed: %s", e)
    if _register_tradedex_miniapp and "trade_dex" in APPLICATIONS:
        try:
            _register_tradedex_miniapp(APPLICATIONS["trade_dex"])
        except Exception as e:
            logging.warning("Trade DEX miniapp registration failed: %s", e)
    if _register_learning_miniapp and "learning" in APPLICATIONS:
        try:
            _register_learning_miniapp(APPLICATIONS["learning"])
        except Exception as e:
            logging.warning("Learning miniapp registration failed: %s", e)
    if _register_dao_miniapp and "dao" in APPLICATIONS:
        try:
            await _register_dao_miniapp(webapp)
        except Exception as e:
            logging.warning("DAO miniapp registration failed: %s", e)
    if _register_affiliate_miniapp and "affliate" in APPLICATIONS:
        try:
            logging.info("[MINIAPP] Initializing affiliate miniapp routes...")
            await _register_affiliate_miniapp(webapp)
            logging.info("✅ [MINIAPP] Affiliate miniapp routes registered successfully")
        except Exception as e:
            logging.error(f"❌ [MINIAPP] Affiliate miniapp registration failed: {e}", exc_info=True)
    
    logging.info("DevDash mounting on: %r", type(webapp))
    
    # Register Support Bot API routes
    if support_router:
        try:
            # Add Support API routes to webapp
            # These will handle /api/support/* requests
            async def support_api_handler(request):
                """Route support API requests to FastAPI router"""
                # Get path info after /api/support
                path = request.rel_url.path_qs
                method = request.method
                
                # Simple proxy: match routes and handle
                # For now, return 404 as FastAPI integration needs ASGI
                return web.json_response(
                    {"error": "Support API requires ASGI support"},
                    status=503
                )
            
            logging.info("Support Bot API router imported (requires ASGI/FastAPI integration)")
        except Exception as e:
            logging.warning(f"Support API registration failed: {e}")
    
    await ensure_tables()
    try:
        from devdash_api import scan_env_bots
        await scan_env_bots()
        logging.info("Bot auto-discovery completed.")
    except Exception as e:
        logging.warning("Bot auto-discovery failed: %s", e)
    register_devdash_routes(webapp)
    
    webapp.router.add_get("/health", health_handler)
    # /env ist ein Debug-Endpoint – standardmäßig AUS (sonst riskanter Leak)
    if os.getenv("ENABLE_ENV_ENDPOINT", "0") == "1":
        webapp.router.add_get("/env", env_handler)
    webapp.router.add_post("/webhook/{route_key}", webhook_handler)
    
    runner = web.AppRunner(webapp)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    logging.info(f"Webhook server listening on 0.0.0.0:{PORT}")
    await site.start()
    
    # Log all started bots
    bot_names = ", ".join(sorted([cfg["name"] for cfg in BOTS if cfg["token"]]))
    logging.info(f"✅ ALL BOTS STARTED SUCCESSFULLY ({len(APPLICATIONS)} bots): {bot_names}")

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        for app in APPLICATIONS.values():
            await app.stop()
            await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())