"""
logger.py
---------
Consistent, timestamped logging for all pipelines. Replacing scattered
`print()` statements with a real logger makes pipeline runs easier to
debug, easier to pipe into log aggregation tools (Cloud Logging, Datadog,
etc.), and signals production-readiness rather than a one-off script.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if not logger.handlers:  # avoid duplicate handlers on re-import
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
