"""Pin: ``sqlalchemydqlite`` top-level logger has a ``logging.NullHandler``.
See sibling test in ``python-dqlite-wire``."""

from __future__ import annotations

import logging

import sqlalchemydqlite  # noqa: F401 -- import for side effect


def test_top_level_logger_has_null_handler() -> None:
    logger = logging.getLogger("sqlalchemydqlite")
    assert any(isinstance(h, logging.NullHandler) for h in logger.handlers), (
        "library top-level logger must have a NullHandler attached per "
        "Python logging HOWTO convention"
    )
