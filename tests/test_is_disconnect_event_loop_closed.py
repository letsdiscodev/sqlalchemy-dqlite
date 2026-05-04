"""Pin: ``is_disconnect`` classifies an ``OperationalError`` carrying
the ``"event loop closed"`` substring as a disconnect.

The async dialect's ``_handle_exception`` (in ``aio.py``) remaps
``RuntimeError("Event loop is closed")`` to
``OperationalError(f"event loop closed: {msg}")`` when the asyncio
machinery has been torn down between checkout and the operation. The
substring scanned by ``_dqlite_disconnect_messages`` must include
``"event loop closed"`` so the SA pool invalidates the slot — without
this entry the next checkout would hit the same dead loop.
"""

from __future__ import annotations

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.base import DqliteDialect


def test_event_loop_closed_substring_classifies_as_disconnect() -> None:
    e = OperationalError(
        "event loop closed: Event loop is closed",
        code=None,
    )
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_event_loop_closed_case_insensitive() -> None:
    e = OperationalError("Event Loop Closed: Event loop is closed", code=None)
    assert DqliteDialect().is_disconnect(e, None, None) is True


def test_unrelated_event_loop_message_not_classified() -> None:
    """Negative pin: a message that mentions ``"event loop"`` but not
    the canonical ``"event loop closed"`` substring must not match."""
    e = OperationalError("event loop is happy and healthy", code=None)
    assert DqliteDialect().is_disconnect(e, None, None) is False


def test_event_loop_closed_via_cause_walk() -> None:
    """The substring scan walks ``__cause__`` and ``__context__``, so a
    SA wrapper around the remapped error must still classify."""
    inner = OperationalError("event loop closed: Event loop is closed", code=None)

    class _SAWrapperError(Exception):
        pass

    outer = _SAWrapperError("statement execution failed")
    outer.__cause__ = inner
    assert DqliteDialect().is_disconnect(outer, None, None) is True
