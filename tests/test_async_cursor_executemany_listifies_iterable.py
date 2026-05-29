"""``executemany`` materialises one-shot iterables to a list before forwarding.

Otherwise SA's engine-retry path re-issues executemany against the exhausted
iterator: a silent zero-row execute then COMMIT, a data-loss class. The list
fast path stays no-copy (pinned via ``id()``)."""

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
    cur, inner = _make_cursor_with_underlying()
    _patch_await(monkeypatch)

    cur.executemany("INSERT INTO t VALUES (?)", iter([(1,), (2,), (3,)]))

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
    """SA always hands a ``list``; pin the no-copy fast path via ``id()``."""
    cur, inner = _make_cursor_with_underlying()
    _patch_await(monkeypatch)

    seq = [(1,), (2,), (3,)]
    cur.executemany("INSERT INTO t VALUES (?)", seq)

    args, _ = inner.executemany.call_args
    assert args[1] is seq, "list input must be forwarded without copy"
