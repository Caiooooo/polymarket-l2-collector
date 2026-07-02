"""
Logging configuration — structured console output.

Provides a ``get_logger(name)`` helper that returns a pre-configured
``logging.Logger`` with a consistent format.  Root-level settings
(log level, destination) are controlled by ``config.Settings``.
"""

from __future__ import annotations

import logging
import sys

_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get (or create) a logger with consistent formatting."""
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers when called multiple times for the same name
    if logger.handlers:
        _loggers[name] = logger
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    _loggers[name] = logger
    return logger


# ── Backward-compat alias ──────────────────────────────────────────
setup_logger = get_logger
