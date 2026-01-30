import logging

from telegram.ext import MessageHandler, filters

try:
    # bevorzugt: aus Package
    from . import setup_logging
except Exception:
    setup_logging = None

# robuste Importe aus dem Projekt-Root
try:
    from .handler import route_message
except ImportError:
    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from bots.crossposter.handler import route_message

try:
    from .miniapp import crossposter_handler
except ImportError:
    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from .miniapp import crossposter_handler


def register(app):
    # Logging so früh wie möglich aktivieren (idempotent)
    if setup_logging:
        setup_logging(service="crossposter")

    logger = logging.getLogger(__name__)
    flt = (filters.ChatType.GROUPS | filters.ChatType.CHANNEL) & (filters.TEXT | filters.PHOTO | filters.Document.ALL)
    app.add_handler(MessageHandler(flt, route_message))
    app.add_handler(crossposter_handler)
    logger.info("Crossposter handler registered")

