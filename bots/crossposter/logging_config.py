"""
Crossposter Bot - Logging Konfiguration
Strukturiertes Logging mit RotatingFileHandler für Produktion
"""

import os
import logging
from logging.handlers import RotatingFileHandler

def setup_logging(service: str = "crossposter") -> None:
    """Initialize structured logging for Crossposter Bot"""
    
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE_CROSSPOSTER", "logs/crossposter.log")
    
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file) if os.path.dirname(log_file) else ".", exist_ok=True)
    
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level))
    
    # Remove existing handlers to prevent duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # RotatingFileHandler: 10MB per file, keep 5 backups
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(getattr(logging, log_level))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))
    
    # Formatter with detailed info
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)-8s] %(name)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Suppress third-party library noise
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    
    logger.info(f"[INIT] Crossposter logging initialized (level={log_level}, file={log_file})")

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for a specific module"""
    return logging.getLogger(f"bot.crossposter.{name}")
