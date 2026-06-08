"""
Shared logging setup so every service emits clean, consistent output.

    from common.log import get_logger, banner
    log = get_logger("sim")
    log.info("started")        ->  20:36:07 INFO  started

Format is deliberately light: `HH:MM:SS LEVEL message`, no pipe-columns. Level is
controlled by LOG_LEVEL (default INFO); callers may raise a logger to DEBUG.
`banner()` prints a boxed header WITHOUT the log prefix — startup config is a header,
not a stream of events.
"""

from __future__ import annotations

import logging
import os
import sys

_CONFIGURED: set[str] = set()


def get_logger(name: str) -> logging.Logger:
    """Return a logger that writes one clean line per record to stdout."""
    logger = logging.getLogger(name)
    if name not in _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(fmt="%(asctime)s %(levelname)-5s %(message)s",
                              datefmt="%H:%M:%S")
        )
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
        _CONFIGURED.add(name)
    return logger


def banner(title: str, rows: list[tuple[str, str]], width: int = 62) -> None:
    """Print a boxed `title` + aligned key/value rows directly to stdout (no log prefix)."""
    bar = "─" * width
    lines = [bar, f" {title}", bar]
    lines += [f" {key:<15}{value}" for key, value in rows]
    lines.append(bar)
    print("\n".join(lines), flush=True)
