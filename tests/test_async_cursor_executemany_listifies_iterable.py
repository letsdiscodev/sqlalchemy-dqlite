"""Pin: ``AsyncAdaptedCursor.executemany`` materialises one-shot
iterables (``iter([...])``, generators, ``map``) to a list BEFORE
forwarding to the underlying cursor.

Without this, a disconnect-class exception mid-batch triggers SA's
pool-invalidation + engine-retry path which re-issues the same
``executemany`` against the now-exhausted iterator — silent zero-row
execute followed by COMMIT, a data-loss class.

The ``isinstance(list)`` gate keeps SA's canonical (always a list) call
a no-copy fast path; pin via ``id()`` equality.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


def _make_cursor_with_underlying() -> tuple[AsyncAdaptedCursor, MagicMock]:
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    inner_conn = MagicMock()
    inner_cursor = MagicMock()
    inner_cursor.description = None
    inner_cursor.rowcount = 0
    inner_cursor.lastrowid = None
    inner_conn.cursor.return_value = inner_cursor
    adapter._connection = inner_conn
    cur = AsyncAdaptedCursor(adapter)
    return cur, inner_cursor


def _patch_await(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sqlalchemydqlite.aio.await_only", lambda _: None)


def test_iterator_input_is_materialised_to_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-list iterable (``iter([...])`` is one-shot) must be
    materialised before being forwarded to the underlying cursor; the
    underlying receives a ``list``."""
    cur, inner = _make_cursor_with_underlying()
    _patch_await(monkeypatch)

    cur.executemany("INSERT INTO t VALUES (?)", iter([(1,), (2,), (3,)]))

    # The underlying cursor.executemany was called with a list, not
    # the original (now-exhausted) iterator.
    args, _ = inner.executemany.call_args
    assert args[0] == "INSERT INTO t VALUES (?)"
    assert isinstance(args[1], list)
    assert args[1] == [(1,), (2,), (3,)]


def test_generator_input_is_materialised_to_list(monkeypatch: pytest.MonkeyPatch) -> None:
    cur, inner = _make_cursor_with_underlying()
    _patch_await(monkeypatch)

    from collections.abc import Iterator

    def gen() -> Iterator[tuple[int, str]]:
        yield (1, "a")
        yield (2, "b")

    cur.executemany("INSERT INTO t VALUES (?, ?)", gen())

    args, _ = inner.executemany.call_args
    assert isinstance(args[1], list)
    assert args[1] == [(1, "a"), (2, "b")]


def test_list_input_is_forwarded_unchanged_no_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    """SA's canonical call site always hands a ``list``. Pin the
    no-copy fast path via ``id()`` equality."""
    cur, inner = _make_cursor_with_underlying()
    _patch_await(monkeypatch)

    seq = [(1,), (2,), (3,)]
    cur.executemany("INSERT INTO t VALUES (?)", seq)

    args, _ = inner.executemany.call_args
    assert args[1] is seq, "list input must be forwarded without copy"
