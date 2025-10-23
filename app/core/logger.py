from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


_HANDLER_FLAG = "_bagholder_managed_handler"
_LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"


def _ensure_int(value: object, default: int, minimum: int = 1) -> int:
    """Return ``value`` coerced to an integer greater or equal to ``minimum``."""

    try:
        numeric = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return max(minimum, default)
    return max(minimum, numeric)


def configure_logging(
    data_dir: str,
    debug_enabled: bool = False,
    max_bytes: int = 1_048_576,
    retention: int = 5,
) -> Path:
    """Configure application logging with a rotating file handler.

    Parameters
    ----------
    data_dir:
        Directory where log files should be stored.
    debug_enabled:
        When ``True`` the logger captures verbose debug output. Otherwise the
        file and console handlers log informational messages.
    max_bytes:
        Maximum size of the log file before rotation occurs. Values are
        clamped to a reasonable minimum to avoid creating zero-length files.
    retention:
        Number of rotated log files to keep.
    """

    log_dir = Path(data_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "bagholder.log"

    fmt = logging.Formatter(_LOG_FORMAT)
    log_level = logging.DEBUG if debug_enabled else logging.INFO

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in list(root_logger.handlers):
        if getattr(handler, _HANDLER_FLAG, False):
            root_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:  # pragma: no cover - defensive cleanup
                pass

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level if debug_enabled else logging.INFO)
    console_handler.setFormatter(fmt)
    setattr(console_handler, _HANDLER_FLAG, True)
    root_logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=_ensure_int(max_bytes, 1_048_576, minimum=1024),
        backupCount=_ensure_int(retention, 5, minimum=1),
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(fmt)
    setattr(file_handler, _HANDLER_FLAG, True)
    root_logger.addHandler(file_handler)

    root_logger.debug(
        "Logging configured (debug_enabled=%s, log_path=%s)",
        debug_enabled,
        log_path,
    )

    return log_path


def get_logger(name: str = "bagholder") -> logging.Logger:
    """Return a logger using the shared BagHolder logging configuration."""

    return logging.getLogger(name)


__all__ = ["configure_logging", "get_logger"]
