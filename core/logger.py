from core.config import DEBUG_MODE, VERBOSE_MODE
import logging
import os

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")


def get_logger(name: str = __name__) -> logging.Logger:
    """Return a module-specific logger."""
    return logging.getLogger(name)


def set_log_level(level: str) -> None:
    """Dynamically update the root logger level."""
    logging.getLogger().setLevel(level.upper())
