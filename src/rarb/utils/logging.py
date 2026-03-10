"""Logging configuration for rarb."""

import logging
import sys
from typing import Optional

# Custom log format with colors (if supported)
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"


class CustomFormatter(logging.Formatter):
    """Custom formatter with colors for console output."""

    grey = "\x1b[38;20m"
    blue = "\x1b[34;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: grey + LOG_FORMAT + reset,
        logging.INFO: green + LOG_FORMAT + reset,
        logging.WARNING: yellow + LOG_FORMAT + reset,
        logging.ERROR: red + LOG_FORMAT + reset,
        logging.CRITICAL: bold_red + LOG_FORMAT + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, DATE_FORMAT)
        return formatter.format(record)


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Configure logging for the application."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(CustomFormatter())
    root_logger.addHandler(console_handler)

    # File handler if requested
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        root_logger.addHandler(file_handler)

    # Set levels for noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)