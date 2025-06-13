import logging
import sys

from wealthz.constants import LOG_FORMAT, LOG_LEVEL


def get_logger(logger_name: str | None = None) -> logging.Logger:
    logger = logging.getLogger(logger_name)

    if not logger.handlers:
        # Level
        logger.setLevel(LOG_LEVEL)
        # Formatter
        logger.propagate = False
        formatter = logging.Formatter(LOG_FORMAT)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
