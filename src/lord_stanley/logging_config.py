"""
Logging configuration for Lord Stanley
"""

from datetime import datetime, timezone
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import os


class UTCFormatter(logging.Formatter):
    """
    Log formatter that produces ISO 8601 timestamps in UTC.
    """

    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()


def configure_logging() -> None:
    """
    Configure logging for Lord Stanley

    Reads log level from the environment LOG_LEVEL, defaulting to INFO in production and
    DEBUG in development.

    Call once at application startup
    """

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    formatter = UTCFormatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    root_logger.addHandler(console_handler)

    try:
        Path("logs").mkdir(exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            Path("logs") / "app.log", when="d", interval=1, backupCount=7
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        root_logger.addHandler(file_handler)
    except OSError:
        root_logger.warning(
            "Could not create log file handler, logging to console only."
        )
