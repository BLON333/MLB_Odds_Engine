import logging
import os

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")

def get_logger(name: str = __name__) -> logging.Logger:
    return logging.getLogger(name)