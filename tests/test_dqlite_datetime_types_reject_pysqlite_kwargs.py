"""Pin: ``_DqliteDateTime`` / ``_DqliteDate`` / ``_DqliteTime`` eagerly
reject pysqlite-only kwargs (``storage_format`` / ``regexp`` /
``truncate_microseconds``) at construction with a dialect-named
``ArgumentError``.

The dqlite-specific bind/result processors emit a fixed ISO8601 format
and parse via ``datetime.fromisoformat``; they do NOT consult these
kwargs. Without an eager reject, the bare ``sqltypes.DateTime.__init__``
raised a stdlib ``TypeError`` with no dialect name, no rationale, and
no pointer to the divergence vs pysqlite's storage-format /
regexp / truncate-microseconds surface — exactly the diagnostic gap the
existing ``native_datetime`` eager-reject block in
``DqliteDialect.__init__`` closes for that one knob.

Mirrors the ``native_datetime`` discipline at the type-class layer.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import ArgumentError

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


@pytest.mark.parametrize(
    "kwarg",
    ["storage_format", "regexp", "truncate_microseconds"],
)
def test_dqlite_datetime_rejects_pysqlite_kwargs(kwarg: str) -> None:
    with pytest.raises(ArgumentError) as exc_info:
        _DqliteDateTime(**{kwarg: object()})
    message = str(exc_info.value)
    assert "dqlite" in message.lower()
    assert kwarg in message


@pytest.mark.parametrize("kwarg", ["storage_format", "regexp"])
def test_dqlite_date_rejects_pysqlite_kwargs(kwarg: str) -> None:
    with pytest.raises(ArgumentError) as exc_info:
        _DqliteDate(**{kwarg: object()})
    message = str(exc_info.value)
    assert "dqlite" in message.lower()
    assert kwarg in message


@pytest.mark.parametrize(
    "kwarg",
    ["storage_format", "regexp", "truncate_microseconds"],
)
def test_dqlite_time_rejects_pysqlite_kwargs(kwarg: str) -> None:
    with pytest.raises(ArgumentError) as exc_info:
        _DqliteTime(**{kwarg: object()})
    message = str(exc_info.value)
    assert "dqlite" in message.lower()
    assert kwarg in message


def test_dqlite_datetime_accepts_timezone_kwarg() -> None:
    """Sanity: documented kwargs (``timezone``) still work — only the
    pysqlite-only knobs are rejected."""
    tp = _DqliteDateTime(timezone=True)
    assert tp.timezone is True


def test_dqlite_date_construction_no_kwargs_succeeds() -> None:
    """Sanity: no-kwarg construction stays unchanged."""
    assert _DqliteDate() is not None


def test_dqlite_time_accepts_timezone_kwarg() -> None:
    tp = _DqliteTime(timezone=True)
    assert tp.timezone is True
