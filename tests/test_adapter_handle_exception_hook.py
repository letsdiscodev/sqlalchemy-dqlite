"""``AsyncAdaptedConnection._handle_exception`` matches SA's
aiosqlite normalisation hook.

SA's reference dialect routes every adapter-level exception through
``_handle_exception`` so a subclass can remap driver-layer quirks in
one place. Our adapter previously had no such hook, forcing any
future renormalisation to touch every try-block. Default behaviour is
identity re-raise; the test pins that contract and the subclass-remap
extension point.
"""

from __future__ import annotations

from typing import NoReturn
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import OperationalError
from sqlalchemydqlite.aio import AsyncAdaptedConnection


def _make_adapter() -> AsyncAdaptedConnection:
    # Build an AsyncAdaptedConnection skeleton without running the
    # constructor (the real constructor takes an AsyncConnection).
    adapter = AsyncAdaptedConnection.__new__(AsyncAdaptedConnection)
    adapter._connection = MagicMock()
    return adapter


def test_handle_exception_default_is_identity() -> None:
    adapter = _make_adapter()
    err = RuntimeError("boom")
    with pytest.raises(RuntimeError) as exc_info:
        adapter._handle_exception(err)
    assert exc_info.value is err


def test_handle_exception_can_be_overridden_to_remap() -> None:
    class Remapping(AsyncAdaptedConnection):
        def _handle_exception(self, error: BaseException) -> NoReturn:
            if isinstance(error, RuntimeError):
                raise OperationalError(str(error)) from error
            raise error

    adapter = Remapping.__new__(Remapping)
    adapter._connection = MagicMock()
    with pytest.raises(OperationalError, match="boom"):
        adapter._handle_exception(RuntimeError("boom"))
