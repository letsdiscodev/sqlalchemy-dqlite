"""Pin: ``DqliteDialect.do_recover_twophase`` is annotated ``NoReturn``.

The body unconditionally raises ``NotSupportedError``; the previous
``list[Any]`` annotation misled mypy / pyright / IDE auto-complete
readers into expecting a successful list response. ``NoReturn`` is
the SA-style annotation for raise-only stubs (matches the
``NoReturn`` pins on ``aio.py``'s ``callproc`` / ``nextset`` /
``scroll`` stubs) so type-checkers flag any code following a call
site as unreachable.

Runtime behaviour (raising NotSupportedError) is unchanged.
"""

from __future__ import annotations

import typing
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.exceptions import NotSupportedError
from sqlalchemydqlite.base import DqliteDialect


def test_do_recover_twophase_return_annotation_is_noreturn() -> None:
    hints = typing.get_type_hints(DqliteDialect.do_recover_twophase)
    # ``NoReturn`` is canonically the ``typing.NoReturn`` sentinel; the
    # exact identity check is the right pin against accidental
    # changes to ``None`` (which the four sibling twophase stubs use
    # by SA-parent contract).
    assert hints.get("return") is typing.NoReturn, (
        f"do_recover_twophase return annotation must be NoReturn; got {hints.get('return')!r}"
    )


def test_do_recover_twophase_still_raises_not_supported_error() -> None:
    """Runtime behaviour pin — the annotation change must not affect
    the raise."""
    dialect = DqliteDialect.__new__(DqliteDialect)
    with pytest.raises(NotSupportedError, match="two-phase"):
        dialect.do_recover_twophase(MagicMock())
