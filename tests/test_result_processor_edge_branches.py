"""Pin the defensive fallthrough branches and the None-handling in the
``_DqliteDateTime`` / ``_DqliteDate`` / ``_DqliteTime`` result-processor
closures.

``pytest --cov`` reports four lines uncovered in ``base.py``:

- L155 (``_DqliteDateTime.process``): fallthrough ``return value``
  for unexpected types (defensive pass-through).
- L189 (``_DqliteDate.process``): the ``if value is None: return None``
  early-return — meaning no test exercises NULL DATE columns.
- L203 (``_DqliteDate.process``): fallthrough for unexpected types.
- L249 (``_DqliteTime.process``): fallthrough for unexpected types.

A refactor that adds preprocessing before the None-check or that
tightens the unexpected-type fallthrough into a TypeError would
silently change user-visible behaviour without these pins.
"""

from __future__ import annotations

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime, _DqliteTime


class TestDateTimeResultProcessor:
    def test_processor_passes_through_unexpected_type_unchanged(self) -> None:
        """If the wire returned an int / bytes / arbitrary object for
        a DateTime column (only possible via raw query or column-type
        drift), the processor passes it through unchanged. Pin the
        defensive contract."""
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        sentinel = 42
        assert proc(sentinel) == sentinel


class TestDateResultProcessor:
    def test_processor_returns_none_for_none(self) -> None:
        """NULL DATE column → process(None) returns None. Pin the
        early-return — without it, a refactor that adds a
        preprocessing step before the None-check could silently
        mishandle NULL DATE values."""
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        assert proc(None) is None

    def test_processor_passes_through_unexpected_type_unchanged(self) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        sentinel = 42
        assert proc(sentinel) == sentinel


class TestTimeResultProcessor:
    def test_processor_passes_through_unexpected_type_unchanged(self) -> None:
        proc = _DqliteTime().result_processor(None, None)
        assert proc is not None
        sentinel = 42
        assert proc(sentinel) == sentinel
