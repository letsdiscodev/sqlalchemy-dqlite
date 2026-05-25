"""Pin the consolidated ``_TRANSPORT_CLASS_EXCEPTIONS`` tuple and the
``_BARE_DBE_DISCONNECT_CODES`` frozenset shared between the sync and
async dialects.

Both constants live at module scope in ``base.py`` so a future addition
or removal updates one place. ``do_begin``'s post-BEGIN cursor-close
arm and the async adapter's ``close()`` / ``terminate()`` cleanup arms
all reference the same tuple — pin that they remain in sync, and pin
the narrowing of ``do_begin``'s tuple from the older ``DatabaseError``
umbrella to the narrower transport-class set (an ``IntegrityError``
from a buggy audit trigger inside ``cursor.close()`` should propagate,
not be silently swallowed).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dqliteclient.exceptions as _client_exc
import dqlitedbapi.exceptions as _dbapi_exc
from sqlalchemydqlite.base import (
    _BARE_DBE_DISCONNECT_CODES,
    _TRANSPORT_CLASS_EXCEPTIONS,
    DqliteDialect,
)


class TestTransportClassExceptionsTuple:
    def test_tuple_is_immutable(self) -> None:
        """Module-level constant is a tuple — immutable so a
        misbehaving caller can't mutate the set under the dialect's
        feet."""
        assert isinstance(_TRANSPORT_CLASS_EXCEPTIONS, tuple)

    def test_tuple_membership(self) -> None:
        """Pin the exact membership: OperationalError + InterfaceError
        from the dbapi, DqliteConnectionError from the client, and
        OSError for stdlib transport faults (covers
        ConnectionError/BrokenPipeError/TimeoutError as subclasses)."""
        assert set(_TRANSPORT_CLASS_EXCEPTIONS) == {
            _dbapi_exc.OperationalError,
            _dbapi_exc.InterfaceError,
            _client_exc.DqliteConnectionError,
            OSError,
        }

    def test_excludes_database_error_umbrella(self) -> None:
        """``DatabaseError`` (the PEP 249 parent of Integrity / Data /
        Internal / Operational / NotSupported) must NOT be in the
        tuple — that would silently swallow constraint violations
        from a custom audit trigger inside ``cursor.close()`` and
        mask real programmer bugs."""
        assert _dbapi_exc.DatabaseError not in _TRANSPORT_CLASS_EXCEPTIONS

    def test_excludes_programming_error(self) -> None:
        """``ProgrammingError`` (cross-loop reuse) must NOT be in the
        cleanup tuple — it has its own remap path via
        ``_handle_exception`` and ``do_ping`` carries its own broader
        tuple. Including it on cleanup paths would mask the cross-
        loop fault that the remap relies on surfacing."""
        assert _dbapi_exc.ProgrammingError not in _TRANSPORT_CLASS_EXCEPTIONS


class TestBareDbeDisconnectCodes:
    def test_set_is_frozen(self) -> None:
        assert isinstance(_BARE_DBE_DISCONNECT_CODES, frozenset)

    def test_set_membership(self) -> None:
        """SQLITE_CORRUPT (11), SQLITE_FORMAT (24), SQLITE_NOTADB (26)
        — the codes ``_classify_operational`` routes to bare
        ``DatabaseError``, all of which the SA dialect treats as
        slot-fatal."""
        assert frozenset({11, 24, 26}) == _BARE_DBE_DISCONNECT_CODES

    def test_set_imported_from_wire(self) -> None:
        """The set references the wire-layer constants, not magic
        numbers. A future rename in the wire layer would surface as
        an import error rather than a silent drift."""
        from dqlitewire import SQLITE_CORRUPT, SQLITE_FORMAT, SQLITE_NOTADB

        assert SQLITE_CORRUPT in _BARE_DBE_DISCONNECT_CODES
        assert SQLITE_FORMAT in _BARE_DBE_DISCONNECT_CODES
        assert SQLITE_NOTADB in _BARE_DBE_DISCONNECT_CODES

    def test_set_is_wire_layer_ssot(self) -> None:
        """The SA set IS the wire-layer ``BARE_DATABASE_ERROR_CODES``.
        Identity equality means the SA dialect can not silently drift
        from the wire-layer truth: a future wire-layer addition
        propagates here automatically. Pin the cross-package SSOT
        relationship."""
        from dqlitewire import BARE_DATABASE_ERROR_CODES

        assert _BARE_DBE_DISCONNECT_CODES is BARE_DATABASE_ERROR_CODES


class TestDoBeginCloseNarrowing:
    """Pin that ``do_begin``'s post-BEGIN cursor-close arm uses the
    narrow transport-class tuple — an ``IntegrityError`` from
    ``cursor.close()`` (e.g. a custom audit trigger fires) propagates
    rather than being silently swallowed."""

    @pytest.mark.parametrize(
        "transport_exc",
        [
            _dbapi_exc.OperationalError("leader flip", code=None),
            _dbapi_exc.InterfaceError("Connection is closed"),
            _client_exc.DqliteConnectionError("peer rst"),
            OSError("broken pipe"),
        ],
    )
    def test_swallows_transport_class_close_failures(self, transport_exc: BaseException) -> None:
        """Each transport-class exception from ``cursor.close()`` after
        a successful BEGIN must be debug-logged and absorbed (so the
        BEGIN succeeded and the engine sees a clean return)."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = transport_exc
        mock_conn.cursor.return_value = mock_cursor

        # Must not raise.
        dialect.do_begin(mock_conn)
        mock_cursor.execute.assert_called_once_with("BEGIN")
        mock_cursor.close.assert_called_once_with()

    def test_does_not_swallow_integrity_error_on_close(self) -> None:
        """An ``IntegrityError`` from ``cursor.close()`` is implausible
        in practice (close doesn't fire constraints) but if some custom
        audit trigger / extension produces one, it must propagate so
        the bug is visible — not be silently swallowed under the
        previous ``DatabaseError``-umbrella catch."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = _dbapi_exc.IntegrityError("audit trigger violated", code=19)
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(_dbapi_exc.IntegrityError, match="audit trigger violated"):
            dialect.do_begin(mock_conn)

    def test_does_not_swallow_data_error_on_close(self) -> None:
        """``DataError`` from ``cursor.close()`` similarly propagates."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = _dbapi_exc.DataError("value too large", code=18)
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(_dbapi_exc.DataError, match="value too large"):
            dialect.do_begin(mock_conn)

    def test_does_not_swallow_attribute_error(self) -> None:
        """An ``AttributeError`` from ``cursor.close()`` (not in the
        ``_FORCE_CLOSE_TAIL_EXCEPTIONS`` tuple) propagates —
        programmer-bug shapes must stay visible. ``RuntimeError`` and
        ``ReferenceError`` ARE in the wider tuple (cross-loop dispose /
        dead-proxy shapes that mask the BEGIN exception) and are
        swallowed; the rationale is symmetric with ``do_close``."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = AttributeError("programmer bug")
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(AttributeError, match="programmer bug"):
            dialect.do_begin(mock_conn)


class TestHandleExceptionConsolidation:
    """Pin the consolidated ``_handle_exception`` shape: a single
    ``isinstance(error, (RuntimeError, ProgrammingError))`` check
    feeds a single substring scan against ``"different loop"``,
    matching both Python's ``"attached to a different loop"``
    wording (RuntimeError) and dqlitedbapi's
    ``"different event loop"`` wording (ProgrammingError)."""

    @pytest.fixture
    def adapter(self) -> object:
        """Bare adapter shell with only the methods the test exercises;
        avoids needing a real connect/close cycle."""
        from sqlalchemydqlite.aio import AsyncAdaptedConnection

        return AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)

    @pytest.mark.parametrize(
        ("exc_cls", "msg", "expected_substring"),
        [
            (
                RuntimeError,
                "got Future ... attached to a different loop",
                "different loop",
            ),
            (
                RuntimeError,
                "Task <Task> attached to a different loop",
                "different loop",
            ),
            (
                _dbapi_exc.ProgrammingError,
                "AsyncConnection used from a different event loop",
                "different event loop",
            ),
        ],
    )
    def test_remaps_loop_mismatch_to_operational_error(
        self,
        adapter: object,
        exc_cls: type[BaseException],
        msg: str,
        expected_substring: str,
    ) -> None:
        """Both RuntimeError and ProgrammingError variants of the
        cross-loop fault remap to OperationalError(code=None) so the
        SA dialect's substring branch picks them up and the pool
        invalidates the slot. RuntimeError carries Python's
        ``"different loop"`` wording; ProgrammingError carries
        dqlitedbapi's distinct ``"different event loop"`` wording —
        the dialect's substring list contains both."""
        with pytest.raises(_dbapi_exc.OperationalError) as exc_info:
            adapter._handle_exception(exc_cls(msg))  # type: ignore[attr-defined]
        assert exc_info.value.code is None
        assert expected_substring in str(exc_info.value)

    def test_passes_through_unrelated_runtime_error(self, adapter: object) -> None:
        """A RuntimeError without ``"different loop"`` propagates
        unchanged — programmer-bug shapes (Event loop is closed,
        unawaited coroutine) must stay visible."""
        with pytest.raises(RuntimeError, match="something else entirely"):
            adapter._handle_exception(RuntimeError("something else entirely"))  # type: ignore[attr-defined]

    def test_passes_through_unrelated_programming_error(self, adapter: object) -> None:
        """A ProgrammingError without ``"different loop"`` propagates
        unchanged — bind-count mismatches and similar bugs must stay
        visible."""
        with pytest.raises(_dbapi_exc.ProgrammingError, match="bad bind count"):
            adapter._handle_exception(_dbapi_exc.ProgrammingError("bad bind count"))  # type: ignore[attr-defined]

    def test_dbapi_programming_error_remap_classified_as_disconnect(self, adapter: object) -> None:
        """End-to-end pin: the ProgrammingError remap reaches the SA
        dialect's ``is_disconnect`` substring branch and returns True.
        The dialect's substring list includes ``"different event loop"``
        — without it the cross-loop fault would survive in the pool slot."""
        try:
            adapter._handle_exception(  # type: ignore[attr-defined]
                _dbapi_exc.ProgrammingError("AsyncConnection used from a different event loop")
            )
        except _dbapi_exc.OperationalError as remapped:
            assert DqliteDialect().is_disconnect(remapped, None, None) is True
        else:  # pragma: no cover
            raise AssertionError("expected OperationalError remap")
