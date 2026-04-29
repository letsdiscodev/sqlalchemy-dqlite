"""Pin: SA-adapter classes ``AsyncAdaptedConnection`` and
``AsyncAdaptedCursor`` raise a clear driver-level ``TypeError`` on
pickle / copy / deepcopy. The error message must name the
SA-adapter class specifically — without an explicit
``__reduce__``, the underlying dbapi class name leaks through and
the diagnostic is a wrong-layer pointer for SA users.
"""

from __future__ import annotations

import copy
import pickle
from unittest.mock import MagicMock

import pytest

from sqlalchemydqlite.aio import AsyncAdaptedConnection, AsyncAdaptedCursor


class TestAsyncAdaptedConnectionPickleGuard:
    def test_pickle_names_adapter_class(self) -> None:
        raw = MagicMock()
        adapted = AsyncAdaptedConnection(raw)
        with pytest.raises(TypeError, match="AsyncAdaptedConnection"):
            pickle.dumps(adapted)

    def test_copy_copy_raises(self) -> None:
        raw = MagicMock()
        adapted = AsyncAdaptedConnection(raw)
        with pytest.raises(TypeError, match="AsyncAdaptedConnection"):
            copy.copy(adapted)

    def test_copy_deepcopy_raises(self) -> None:
        raw = MagicMock()
        adapted = AsyncAdaptedConnection(raw)
        with pytest.raises(TypeError, match="AsyncAdaptedConnection"):
            copy.deepcopy(adapted)


class TestAsyncAdaptedCursorPickleGuard:
    def test_pickle_names_adapter_class(self) -> None:
        raw = MagicMock()
        adapted_conn = AsyncAdaptedConnection(raw)
        cur = AsyncAdaptedCursor(adapted_conn)
        with pytest.raises(TypeError, match="AsyncAdaptedCursor"):
            pickle.dumps(cur)

    def test_copy_copy_raises(self) -> None:
        raw = MagicMock()
        adapted_conn = AsyncAdaptedConnection(raw)
        cur = AsyncAdaptedCursor(adapted_conn)
        with pytest.raises(TypeError, match="AsyncAdaptedCursor"):
            copy.copy(cur)

    def test_copy_deepcopy_raises(self) -> None:
        raw = MagicMock()
        adapted_conn = AsyncAdaptedConnection(raw)
        cur = AsyncAdaptedCursor(adapted_conn)
        with pytest.raises(TypeError, match="AsyncAdaptedCursor"):
            copy.deepcopy(cur)
