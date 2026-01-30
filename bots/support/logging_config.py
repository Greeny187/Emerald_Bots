"""
logging_config.py - Zentrale Logging-Konfiguration für Support Bot

Dieses Modul bietet eine zentrale Logging-Konfiguration für den Emerald Support Bot.
Es setzt standardisierte Logging-Formate und Handler für alle Module.

Verwendung:
    from . import logging_config
    logging_config.setup_logging()
"""

import logging
import os
import sys
from typing import Optional

# Logging-Formate
DETAILED_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s - %(funcName)s:%(lineno)d - %(message)s"
SIMPLE_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s"


def setup_logging(
    level: str = None,
    log_file: Optional[str] = None,
    detailed: bool = True
) -> None:
    """
    Konfiguriere zentrales Logging für Support Bot
    
    Args:
        level: Log-Level (DEBUG, INFO, WARNING, ERROR) - default aus LOG_LEVEL env var oder INFO
        log_file: Path zu Log-Datei (optional)
        detailed: Nutze detailliertes Format mit Funktionsnamen und Zeilennummern
    """
    # Bestimme Log-Level
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    log_level = getattr(logging, level, logging.INFO)
    fmt = DETAILED_FORMAT if detailed else SIMPLE_FORMAT
    
    # Basis-Logging konfigurieren
    logging.basicConfig(
        level=log_level,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
    
    # File Handler (falls konfiguriert)
    if log_file is None:
        log_file = os.getenv("SUPPORT_LOG_FILE", "support_bot.log")
    
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(log_level)
            file_handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
            logging.getLogger().addHandler(file_handler)
            logging.info(f"[LOGGING] File logging enabled: {log_file}")
        except Exception as e:
            logging.error(f"[LOGGING] Failed to setup file logging: {e}")
    
    # Konfiguriere spezifische Logger
    logging.getLogger("bot.support").setLevel(log_level)
    logging.getLogger("bot.support.db").setLevel(log_level)
    logging.getLogger("bot.support.api").setLevel(log_level)
    logging.getLogger("bot.support.miniapp").setLevel(log_level)
    logging.getLogger("bot.support.server").setLevel(log_level)
    
    # Reduziere Verbose Logging von Dependencies
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("psycopg2").setLevel(logging.WARNING)
    
    logging.info(f"[LOGGING] Support Bot logging initialized - level={level}")


def get_logger(name: str) -> logging.Logger:
    """
    Hole einen konfigurierten Logger
    
    Args:
        name: Logger-Name (meist __name__)
    
    Returns:
        Konfigurierter Logger
    """
    return logging.getLogger(name)


# Initialisiere Logging beim Import
if __name__ != "__main__":
    # Logging wird automatisch initialisiert, wenn dieses Modul importiert wird
    if os.getenv("AUTO_SETUP_LOGGING", "true").lower() == "true":
        setup_logging()
