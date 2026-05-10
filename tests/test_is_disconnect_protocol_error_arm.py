"""is_disconnect's bare ``client.ProtocolError`` arm.

The cause-chain walk in ``DqliteDialect.is_disconnect`` has an
explicit ``isinstance(cause, _client_exc.ProtocolError): return True``
arm (base.py:1833). It exists for third-party callers / middleware
that bypass the dbapi wrap and surface a bare ``ProtocolError`` deeper
in the chain — without the arm, the wire-decode failure walks past
every type-check and only the substring scan on the
``OperationalError``-wrapped form would fire.

Pin both shapes the arm exists to handle:

- a bare ``ProtocolError`` deeper in the ``__cause__`` chain (the walk
  reaches the arm).
- a bare ``ProtocolError`` deeper in the ``__context__`` chain (walk
  follows both per the cause-walk's contract).

Without these pins, a future refactor that consolidates the
cause-walk arms could silently delete the branch and the only signal
would be a production disconnect-classification miss → SA pool fails
to recycle the slot → subsequent queries fail mysteriously.
"""

from __future__ import annotations

import dqliteclient.exceptions as _client_exc
from sqlalchemydqlite.base import DqliteDialect


class TestIsDisconnectProtocolErrorArm:
    def test_protocol_error_via_cause_chain(self) -> None:
        """A wrapper exception with ``__cause__ = ProtocolError(...)``
        forces the cause-walk to descend; the bare-``e`` entry-level
        check on the wrapper does NOT match (RuntimeError is not in
        the disconnect-class set), so the walk's ProtocolError arm is
        the only path that classifies."""
        dialect = DqliteDialect()
        inner = _client_exc.ProtocolError("malformed frame")
        try:
            raise RuntimeError("middleware wrapper") from inner
        except RuntimeError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_protocol_error_via_context_chain(self) -> None:
        """``__context__`` (implicit chaining via ``raise X`` while
        ``Y`` is in flight) must be walked too; the cause-walk follows
        both per its contract."""
        dialect = DqliteDialect()
        try:
            try:
                raise _client_exc.ProtocolError("malformed frame")
            except _client_exc.ProtocolError:
                raise RuntimeError("middleware wrapper")  # noqa: B904
        except RuntimeError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True

    def test_protocol_error_two_hop_via_cause_chain(self) -> None:
        """Two layers of middleware — outer wrapper, intermediate
        wrapper, inner ProtocolError. The walk descends until it hits
        the arm."""
        dialect = DqliteDialect()
        leaf = _client_exc.ProtocolError("malformed frame")
        try:
            try:
                raise ValueError("intermediate") from leaf
            except ValueError as middle:
                raise RuntimeError("outermost wrapper") from middle
        except RuntimeError as wrapped:
            assert dialect.is_disconnect(wrapped, None, None) is True
