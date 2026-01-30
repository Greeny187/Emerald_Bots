from __future__ import annotations
import json
import logging
import os
import sys
from datetime import datetime, timezone

__all__ = ["app", "miniapp", "setup_logging"]


class _JsonFormatter(logging.Formatter):
    """Kleiner JSON-Formatter für Heroku/LogDrains.

    Aktivierbar via ENV:
      LOG_JSON=1
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # optionale Felder (z.B. request_id/tenant_id via LoggerAdapter)
        for key in ("service", "request_id", "tenant_id", "route_id", "chat_id"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(service: str = "crossposter") -> None:
    """Initialisiert Logging robust (einmalig) und stdout-basiert.

    - LOG_LEVEL: DEBUG/INFO/WARNING/ERROR (Default: INFO)
    - LOG_JSON:  1 -> JSON-Logs
    """

    root = logging.getLogger()
    if getattr(root, "_crossposter_logging_configured", False):
        return

    level_name = (os.getenv("LOG_LEVEL") or os.getenv("PYTHON_LOG_LEVEL") or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(stream=sys.stdout)
    if os.getenv("LOG_JSON", "").strip() in ("1", "true", "TRUE", "yes", "YES"):
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    root.setLevel(level)

    # Falls ein Framework schon Handler gesetzt hat (z.B. PTB), nicht duplizieren.
    if not root.handlers:
        root.addHandler(handler)
    else:
        # Handler hinzufügen, wenn noch kein StreamHandler auf stdout existiert
        has_stdout = False
        for h in root.handlers:
            if isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout:
                has_stdout = True
                break
        if not has_stdout:
            root.addHandler(handler)

    # Standard: httpx nicht zu noisy
    logging.getLogger("httpx").setLevel(max(logging.INFO, level))
    logging.getLogger("asyncpg").setLevel(max(logging.INFO, level))

    # Flag setzen
    setattr(root, "_crossposter_logging_configured", True)
