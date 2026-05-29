"""``AsyncAdaptedConnection.await_`` is exposed as a staticmethod (SA convention) so
instrumentation and the cursor can reuse the greenlet hop."""

from sqlalchemy.util import await_only

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def test_async_adapter_exposes_await_class_attribute() -> None:
    """``await_`` lives on the class itself, readable via class introspection."""
    assert "await_" in AsyncAdaptedConnection.__dict__


def test_async_adapter_await_is_await_only() -> None:
    """The staticmethod points at SA's ``await_only``, not our own wrapper."""
    assert AsyncAdaptedConnection.await_ is await_only


def test_async_adapter_await_is_callable_on_class() -> None:
    """The staticmethod descriptor unwraps to a callable on class access."""
    assert callable(AsyncAdaptedConnection.await_)
