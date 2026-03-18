"""
Structured Logging — JSON logs for production, human-readable for dev.

Usage:
    from logging_config import setup_logging
    setup_logging()  # call once at startup (api.py / workers.py)

    import logging
    logger = logging.getLogger(__name__)
    logger.info("scan complete", extra={"symbols": 150, "alerts": 3})

Output (production — JSON):
    {"ts": "2026-03-18T12:00:00Z", "level": "INFO", "logger": "workers",
     "msg": "scan complete", "symbols": 150, "alerts": 3}

Output (development — human):
    2026-03-18 12:00:00 INFO  [workers] scan complete symbols=150 alerts=3
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Structured JSON formatter for production log aggregation (ELK / CloudWatch / Loki)."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Merge extra fields (e.g. symbols=150)
        for key in ("symbols", "alerts", "latency_ms", "symbol", "rule",
                     "status_code", "method", "path", "error", "task_id",
                     "duration_s", "ws_conn_id", "rate_remaining"):
            val = getattr(record, key, None)
            if val is not None:
                log_entry[key] = val

        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class DevFormatter(logging.Formatter):
    """Colored, human-readable formatter for local development."""

    COLORS = {
        "DEBUG": "\033[90m",    # gray
        "INFO": "\033[36m",     # cyan
        "WARNING": "\033[33m",  # yellow
        "ERROR": "\033[31m",    # red
        "CRITICAL": "\033[41m", # red bg
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        prefix = f"{ts} {color}{record.levelname:<7}{self.RESET} [{record.name}]"
        msg = record.getMessage()

        # Append known extras as key=value
        extras = []
        for key in ("symbols", "alerts", "latency_ms", "symbol", "rule",
                     "status_code", "method", "path", "error", "task_id",
                     "duration_s", "ws_conn_id", "rate_remaining"):
            val = getattr(record, key, None)
            if val is not None:
                extras.append(f"{key}={val}")

        suffix = " ".join(extras)
        line = f"{prefix} {msg}"
        if suffix:
            line += f" {suffix}"

        if record.exc_info and record.exc_info[1]:
            line += "\n" + self.formatException(record.exc_info)

        return line


def setup_logging(level: str | None = None) -> None:
    """
    Configure root logger. Call once at application startup.

    - SCANNER_ENV=production → JSON to stdout (for log aggregators)
    - SCANNER_ENV=development (default) → colored human output
    - SCANNER_LOG_LEVEL overrides level (default INFO)
    """
    env = os.getenv("SCANNER_ENV", "development")
    log_level = (level or os.getenv("SCANNER_LOG_LEVEL", "INFO")).upper()

    root = logging.getLogger()
    root.setLevel(log_level)

    # Remove existing handlers (prevents duplication on reload)
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)

    if env == "production":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(DevFormatter())

    root.addHandler(handler)

    # Quiet noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("celery.worker.strategy").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
