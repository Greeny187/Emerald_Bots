"""Affiliate Bot - Logging Configuration"""

import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logging(level=None, log_file=None, detailed=True):
    """
    Setup logging for Affiliate Bot
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). Defaults to LOG_LEVEL env or INFO
        log_file: Path to log file. Defaults to AFFILIATE_LOG_FILE env or affiliate_bot.log
        detailed: Include function names and line numbers
    
    Returns:
        Logger instance
    """
    
    # Get log level from parameter or environment
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level, logging.INFO)
    
    # Get log file from parameter or environment
    if log_file is None:
        log_file = os.getenv("AFFILIATE_LOG_FILE", "affiliate_bot.log")
    
    # Root logger for all affiliate modules
    root_logger = logging.getLogger("bot.affiliate")
    root_logger.setLevel(level)
    
    # Clear existing handlers
    root_logger.handlers = []
    
    # Format
    if detailed:
        log_format = (
            "%(asctime)s [%(levelname)-8s] %(name)s - "
            "%(funcName)s:%(lineno)d - %(message)s"
        )
    else:
        log_format = "%(asctime)s [%(levelname)-8s] %(message)s"
    
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler with rotation
    try:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10_000_000,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        root_logger.warning(f"Could not setup file handler: {e}")
    
    # Suppress verbose third-party logging
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger for a specific module"""
    return logging.getLogger(name)


# Auto-setup if enabled via environment variable
if os.getenv("AUTO_SETUP_LOGGING", "true").lower() == "true":
    setup_logging()
