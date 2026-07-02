"""
Logging configuration — dual-mode output.

When *LOG_FORMAT* is ``json``, renders structured JSON lines
suitable for log aggregation.  Default plain-text format is identical
to the previous behaviour.

Extras can be passed via ``logger.info("msg", extra={"coin": "btc", …})``
or via a ``LogContext`` context manager.

::

    log = get_logger("collector")
    log.info("Connecting", extra={"coin": "btc", "interval": "5m"})
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

_LOGGERS: dict[str, logging.Logger] = {}

_LOG_FORMAT = os.getenv("LOG_FORMAT", "plain")


class _JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        import json as json_mod

        obj: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%f+00:00"),
            "level": record.levelname,
            "name": record.name,
            "msg": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            obj["data"] = record.extra_fields  # type: ignore[attr-defined]
        return json_mod.dumps(obj, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get (or create) a logger.

    Respects the ``LOG_FORMAT`` environment variable:

    - ``json`` — single-line JSON records (recommended for production)
    - ``plain`` (default) — human-readable ``timestamp | name | level | msg``
    """
    if name in _LOGGERS:
        return _LOGGERS[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        _LOGGERS[name] = logger
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    if _LOG_FORMAT == "json":
        handler.setFormatter(
            _JsonFormatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
        )
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    logger.addHandler(handler)

    # Patch makeRecord so extra kwargs become record.extra_fields
    _orig = logger.makeRecord

    def _make_record(name, level, fn, lno, msg, args, exc_info, func=None, extra=None, sinfo=None):  # type: ignore[no-untyped-def]
        rec = _orig(name, level, fn, lno, msg, args, exc_info, func=func, extra=extra, sinfo=sinfo)
        if extra:
            rec.extra_fields = extra  # type: ignore[attr-defined]
        return rec

    logger.makeRecord = _make_record  # type: ignore[method-assign]
    _LOGGERS[name] = logger
    return logger


@contextmanager
def log_context(logger: logging.Logger, **fields: Any) -> Iterator[None]:
    """Temporarily add context fields to every log call within the block.

    The fields are appended to the *msg* portion of each record for both
    ``plain`` and ``json`` modes.

    Usage::

        log = get_logger("collector")
        with log_context(log, coin="btc", interval="5m"):
            log.info("Connecting WS")
            log.info("Flush", extra={"rows": 50})
    """
    _orig_handle = logger.handle

    def _context_handle(record: logging.LogRecord) -> None:
        existing = getattr(record, "extra_fields", None) or {}
        merged = {**fields, **existing}
        record.extra_fields = merged  # type: ignore[attr-defined]
        _orig_handle(record)

    logger.handle = _context_handle  # type: ignore[method-assign]
    try:
        yield
    finally:
        logger.handle = _orig_handle  # type: ignore[method-assign]


# ── Backward-compat alias ──────────────────────────────────────────
setup_logger = get_logger
