"""is_disconnect's cause-walk has an explicit bare ``client.ProtocolError`` arm for
middleware that bypasses the dbapi wrap and surfaces a bare ProtocolError in the chain."""

from __future__ import annotations

import dqliteclient.exceptions as _client_exc
from sqlalchemydqlite.base import DqliteDialect


class TestIsDisconnectProtocolErrorArm:
    def test_protocol_error_via_cause_chain(self) -> None:
        """A RuntimeError wrapper with __cause__ ProtocolError classifies only via the arm."""
        dialect = DqliteDialect()
        inner = _client_exc.ProtocolError("malformed frame")
        try:
            raise RuntimeError("middleware wrapper") from inner
        except RuntimeError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_protocol_error_via_context_chain(self) -> None:
        """Implicit __context__ chaining must be walked too."""
        dialect = DqliteDialect()
        try:
            try:
                raise _client_exc.ProtocolError("malformed frame")
            except _client_exc.ProtocolError:
                raise RuntimeError("middleware wrapper")  # noqa: B904
        except RuntimeError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_protocol_error_two_hop_via_cause_chain(self) -> None:
        """The walk descends two wrapper layers until it hits the arm."""
        dialect = DqliteDialect()
        leaf = _client_exc.ProtocolError("malformed frame")
        try:
            try:
                raise ValueError("intermediate") from leaf
            except ValueError as middle:
                raise RuntimeError("outermost wrapper") from middle
        except RuntimeError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True
