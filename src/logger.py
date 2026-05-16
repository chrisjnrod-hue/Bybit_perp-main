
import logging
from .config import LOG_LEVEL

def get_logger(name=__name__):
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
