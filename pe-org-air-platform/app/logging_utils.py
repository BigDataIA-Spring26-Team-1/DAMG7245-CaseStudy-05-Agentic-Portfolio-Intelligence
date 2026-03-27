from __future__ import annotations

import logging
from typing import Any


class _StdlibLoggerAdapter:
    """Minimal adapter so logger calls stay compatible without structlog."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def _format(self, event: str, **kwargs: Any) -> str:
        if not kwargs:
            return event
        fields = " ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"{event} {fields}"

    def info(self, event: str, **kwargs: Any) -> None:
        self._logger.info(self._format(event, **kwargs))

    def warning(self, event: str, **kwargs: Any) -> None:
        self._logger.warning(self._format(event, **kwargs))

    def error(self, event: str, **kwargs: Any) -> None:
        self._logger.error(self._format(event, **kwargs))

    def debug(self, event: str, **kwargs: Any) -> None:
        self._logger.debug(self._format(event, **kwargs))

    def exception(self, event: str, **kwargs: Any) -> None:
        self._logger.exception(self._format(event, **kwargs))


def get_logger(name: str | None = None):
    try:
        import structlog  # type: ignore

        return structlog.get_logger(name)
    except Exception:
        return _StdlibLoggerAdapter(logging.getLogger(name or "app"))

