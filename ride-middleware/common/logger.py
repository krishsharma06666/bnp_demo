"""JSON logs with the 4 levels the brief asks for: INFO, WARNING, RETRYING, ERROR.

Usage:
    from common.logger import get_logger
    log = get_logger("validator")
    log.info("validated", booking_id="7")
    log.retrying("db down, retry in 2s", attempt=1)
"""
import json
import logging
import sys
from datetime import datetime, timezone

RETRYING = 25  # between INFO (20) and WARNING (30)
logging.addLevelName(RETRYING, "RETRYING")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": record.name,
            "message": record.getMessage(),
        }
        line.update(getattr(record, "fields", {}))
        return json.dumps(line, default=str)


class ServiceLogger:
    def __init__(self, name: str):
        self._log = logging.getLogger(name)
        if not self._log.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setFormatter(_JsonFormatter())
            self._log.addHandler(h)
            self._log.setLevel(logging.INFO)
            self._log.propagate = False

    def _emit(self, level: int, msg: str, **fields):
        self._log.log(level, msg, extra={"fields": fields})

    def info(self, msg, **f):
        self._emit(logging.INFO, msg, **f)

    def warning(self, msg, **f):
        self._emit(logging.WARNING, msg, **f)

    def retrying(self, msg, **f):
        self._emit(RETRYING, msg, **f)

    def error(self, msg, **f):
        self._emit(logging.ERROR, msg, **f)


def get_logger(service: str) -> ServiceLogger:
    return ServiceLogger(service)
