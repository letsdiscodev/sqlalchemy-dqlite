"""``AsyncAdaptedConnection._handle_exception`` MUST raise.

The hook is annotated ``-> NoReturn`` and the base implementation is
``raise error``. Subclasses are expected to remap or re-raise; the
NoReturn contract makes "return without raising" a typing error.
Existing tests via the route-through-hook pin (other test files
named ``test_adapter_handle_exception_hook.py`` etc.) verify
exceptions reach this hook. None of them assert the contract that
the hook itself ALWAYS raises.

Pin the base contract directly so a refactor that accidentally
returns instead of raising is caught loudly. Subclass behaviour
(swallowing exceptions and corrupting cursor state) is out of scope
— typing already defends against the "return without raising"
mistake at edit time, and this test catches a runtime regression
of the base path.
"""

from __future__ import annotations

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    """Construct an AsyncAdaptedConnection with a None backing
    connection — _handle_exception does not touch ``_connection`` so
    the placeholder is harmless. Avoids needing a live AsyncConnection."""
    # AsyncAdaptedConnection.__init__ takes one positional argument.
    return AsyncAdaptedConnection(None)  # type: ignore[arg-type]


class TestBaseHandleExceptionAlwaysRaises:
    def test_re_raises_value_error_unchanged(self) -> None:
        adapter = _make_adapter()
        original = ValueError("boom")
        with pytest.raises(ValueError) as exc_info:
            adapter._handle_exception(original)
        assert exc_info.value is original

    def test_re_raises_type_error_unchanged(self) -> None:
        adapter = _make_adapter()
        original = TypeError("type fail")
        with pytest.raises(TypeError) as exc_info:
            adapter._handle_exception(original)
        assert exc_info.value is original

    def test_re_raises_base_exception_too(self) -> None:
        """KeyboardInterrupt / SystemExit are BaseException-only; the
        ``_handle_exception`` signature accepts BaseException so the
        contract holds for them too. The asyncio.CancelledError path
        in commit / rollback / close depends on this."""
        adapter = _make_adapter()
        original = KeyboardInterrupt()
        with pytest.raises(KeyboardInterrupt):
            adapter._handle_exception(original)
