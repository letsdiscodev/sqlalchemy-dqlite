"""Pin: AsyncAdaptedConnection._handle_exception (annotated -> NoReturn) always raises."""

from __future__ import annotations

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    """None backing is harmless: _handle_exception never touches _connection."""
    return AsyncAdaptedConnection(None)


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
        """Contract holds for BaseException-only types; the CancelledError path depends on it."""
        adapter = _make_adapter()
        original = KeyboardInterrupt()
        with pytest.raises(KeyboardInterrupt):
            adapter._handle_exception(original)
