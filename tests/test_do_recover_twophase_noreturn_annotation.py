"""Pin: ``DqliteDialect.do_recover_twophase`` is annotated ``NoReturn`` (raise-only stub)."""

from __future__ import annotations

import typing
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import NotSupportedError
from sqlalchemydqlite.base import DqliteDialect


def test_do_recover_twophase_return_annotation_is_noreturn() -> None:
    hints = typing.get_type_hints(DqliteDialect.do_recover_twophase)
    assert hints.get("return") is typing.NoReturn, (
        f"do_recover_twophase return annotation must be NoReturn; got {hints.get('return')!r}"
    )


def test_do_recover_twophase_still_raises_not_supported_error() -> None:
    dialect = DqliteDialect.__new__(DqliteDialect)
    with pytest.raises(NotSupportedError, match="two-phase"):
        dialect.do_recover_twophase(MagicMock())
