"""Pin the shared _TRANSPORT_CLASS_EXCEPTIONS tuple and _BARE_DBE_DISCONNECT_CODES frozenset.
do_begin's cursor-close arm uses the narrow transport set, not the old DatabaseError umbrella,
so an IntegrityError from a buggy audit trigger in cursor.close() propagates rather than hides."""

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
        assert isinstance(_TRANSPORT_CLASS_EXCEPTIONS, tuple)

    def test_tuple_membership(self) -> None:
        """OSError covers ConnectionError/BrokenPipeError/TimeoutError as subclasses."""
        assert set(_TRANSPORT_CLASS_EXCEPTIONS) == {
            _dbapi_exc.OperationalError,
            _dbapi_exc.InterfaceError,
            _client_exc.DqliteConnectionError,
            OSError,
        }

    def test_excludes_database_error_umbrella(self) -> None:
        """DatabaseError (PEP 249 parent) excluded: it would swallow constraint violations."""
        assert _dbapi_exc.DatabaseError not in _TRANSPORT_CLASS_EXCEPTIONS

    def test_excludes_programming_error(self) -> None:
        """ProgrammingError excluded: it has its own remap path via _handle_exception;
        swallowing it on cleanup would mask the cross-loop fault the remap relies on."""
        assert _dbapi_exc.ProgrammingError not in _TRANSPORT_CLASS_EXCEPTIONS


class TestBareDbeDisconnectCodes:
    def test_set_is_frozen(self) -> None:
        assert isinstance(_BARE_DBE_DISCONNECT_CODES, frozenset)

    def test_set_membership(self) -> None:
        """Codes _classify_operational routes to bare DatabaseError; all slot-fatal here."""
        assert frozenset({11, 24, 26}) == _BARE_DBE_DISCONNECT_CODES

    def test_set_imported_from_wire(self) -> None:
        from dqlitewire import SQLITE_CORRUPT, SQLITE_FORMAT, SQLITE_NOTADB

        assert SQLITE_CORRUPT in _BARE_DBE_DISCONNECT_CODES
        assert SQLITE_FORMAT in _BARE_DBE_DISCONNECT_CODES
        assert SQLITE_NOTADB in _BARE_DBE_DISCONNECT_CODES

    def test_set_is_wire_layer_ssot(self) -> None:
        """Identity with the wire-layer set: a wire addition propagates here automatically."""
        from dqlitewire import BARE_DATABASE_ERROR_CODES

        assert _BARE_DBE_DISCONNECT_CODES is BARE_DATABASE_ERROR_CODES


class TestDoBeginCloseNarrowing:
    """do_begin's cursor-close arm uses the narrow transport tuple, so a non-transport
    exception from cursor.close() propagates rather than being swallowed."""

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
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = transport_exc
        mock_conn.cursor.return_value = mock_cursor

        dialect.do_begin(mock_conn)
        mock_cursor.execute.assert_called_once_with("BEGIN")
        mock_cursor.close.assert_called_once_with()

    def test_does_not_swallow_integrity_error_on_close(self) -> None:
        """An IntegrityError from cursor.close() must propagate, not be swallowed."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = _dbapi_exc.IntegrityError("audit trigger violated", code=19)
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(_dbapi_exc.IntegrityError, match="audit trigger violated"):
            dialect.do_begin(mock_conn)

    def test_does_not_swallow_data_error_on_close(self) -> None:
        """DataError from cursor.close() similarly propagates."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = _dbapi_exc.DataError("value too large", code=18)
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(_dbapi_exc.DataError, match="value too large"):
            dialect.do_begin(mock_conn)

    def test_does_not_swallow_attribute_error(self) -> None:
        """AttributeError propagates (programmer bug); RuntimeError/ReferenceError are in
        _FORCE_CLOSE_TAIL_EXCEPTIONS (cross-loop/dead-proxy shapes) and are swallowed."""
        dialect = DqliteDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.close.side_effect = AttributeError("programmer bug")
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(AttributeError, match="programmer bug"):
            dialect.do_begin(mock_conn)


class TestHandleExceptionConsolidation:
    """_handle_exception scans both RuntimeError and ProgrammingError for "different loop":
    Python's "attached to a different loop" and dqlitedbapi's "different event loop"."""

    @pytest.fixture
    def adapter(self) -> object:
        """Bare adapter shell, avoiding a real connect/close cycle."""
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
        """Both cross-loop variants remap to OperationalError(code=None) so the dialect's
        substring branch invalidates the slot."""
        with pytest.raises(_dbapi_exc.OperationalError) as exc_info:
            adapter._handle_exception(exc_cls(msg))  # type: ignore[attr-defined]
        assert exc_info.value.code is None
        assert expected_substring in str(exc_info.value)

    def test_passes_through_unrelated_runtime_error(self, adapter: object) -> None:
        """A RuntimeError without "different loop" propagates unchanged."""
        with pytest.raises(RuntimeError, match="something else entirely"):
            adapter._handle_exception(RuntimeError("something else entirely"))  # type: ignore[attr-defined]

    def test_passes_through_unrelated_programming_error(self, adapter: object) -> None:
        """A ProgrammingError without "different loop" propagates unchanged."""
        with pytest.raises(_dbapi_exc.ProgrammingError, match="bad bind count"):
            adapter._handle_exception(_dbapi_exc.ProgrammingError("bad bind count"))  # type: ignore[attr-defined]

    def test_dbapi_programming_error_remap_classified_as_disconnect(self, adapter: object) -> None:
        """End-to-end: the remapped error reaches is_disconnect and returns True, since the
        dialect's substring list includes "different event loop"."""
        try:
            adapter._handle_exception(  # type: ignore[attr-defined]
                _dbapi_exc.ProgrammingError("AsyncConnection used from a different event loop")
            )
        except _dbapi_exc.OperationalError as remapped:
            assert DqliteDialect().is_disconnect(remapped, None, None) is True
        else:  # pragma: no cover
            raise AssertionError("expected OperationalError remap")
