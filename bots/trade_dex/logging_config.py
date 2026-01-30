"""
logging_config.py - Zentrale Logging-Konfiguration für Trade DEX Bot

Dieses Modul bietet eine zentrale Logging-Konfiguration für den Emerald Trade DEX Bot.
Es setzt standardisierte Logging-Formate und Handler für alle Module.
"""

import logging
import os
import sys
from typing import Optional

DETAILED_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s - %(funcName)s:%(lineno)d - %(message)s"
SIMPLE_FORMAT = "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s"


def setup_logging(
    level: str = None,
    log_file: Optional[str] = None,
    detailed: bool = True
) -> None:
    """
    Konfiguriere zentrales Logging für Trade DEX Bot
    
    Args:
        level: Log-Level (DEBUG, INFO, WARNING, ERROR)
        log_file: Path zu Log-Datei (optional)
        detailed: Nutze detailliertes Format
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    log_level = getattr(logging, level, logging.INFO)
    fmt = DETAILED_FORMAT if detailed else SIMPLE_FORMAT
    
    logging.basicConfig(
        level=log_level,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))
    
    if log_file is None:
        log_file = os.getenv("TRADE_DEX_LOG_FILE", "trade_dex_bot.log")
    
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
    logging.getLogger("bot.trade_dex").setLevel(log_level)
    logging.getLogger("bot.trade_dex.db").setLevel(log_level)
    logging.getLogger("bot.trade_dex.handlers").setLevel(log_level)
    logging.getLogger("bot.trade_dex.miniapp").setLevel(log_level)
    logging.getLogger("bot.trade_dex.providers").setLevel(log_level)
    
    # Reduziere Verbose Logging von Dependencies
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("psycopg2").setLevel(logging.WARNING)
    
    logging.info(f"[LOGGING] Trade DEX Bot logging initialized - level={level}")


def get_logger(name: str) -> logging.Logger:
    """Hole einen konfigurierten Logger"""
    return logging.getLogger(name)


if __name__ != "__main__":
    if os.getenv("AUTO_SETUP_LOGGING", "true").lower() == "true":
        setup_logging()
