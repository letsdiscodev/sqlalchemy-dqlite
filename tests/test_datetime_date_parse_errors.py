"""DateTime / Date processors log a warning on unparseable ISO8601 strings.

The column-type contract promises ``datetime.datetime`` / ``datetime.date``
objects. A silent pass-through of a malformed ``str`` hides real
data-integrity problems from every downstream consumer (ORM filters,
``func.max``, serialisation, ordering). The processors keep forgiving
the bad row (so a stray legacy value doesn't abort a full read) but
emit a WARNING so operators can see and repair it.
"""

from __future__ import annotations

import logging

from sqlalchemydqlite.base import _DqliteDate, _DqliteDateTime


class TestDqliteDateTimeParseError:
    def test_unparseable_iso_string_logs_warning_and_returns_raw(self, caplog: object) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):  # type: ignore[attr-defined]
            result = proc("not an iso date")
        assert result == "not an iso date"
        records = [r for r in caplog.records if r.levelno == logging.WARNING]  # type: ignore[attr-defined]
        assert any("DateTime processor" in r.message for r in records), (
            f"expected a WARNING about the unparseable datetime string; got {records!r}"
        )

    def test_parseable_iso_string_no_warning(self, caplog: object) -> None:
        proc = _DqliteDateTime(timezone=False).result_processor(None, None)
        assert proc is not None
        with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):  # type: ignore[attr-defined]
            result = proc("2024-01-02T03:04:05")
        # A valid ISO value parses to datetime.
        import datetime as _dt

        assert isinstance(result, _dt.datetime)
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]  # type: ignore[attr-defined]
        assert warnings == []


class TestDqliteDateParseError:
    def test_unparseable_iso_string_logs_warning_and_returns_raw(self, caplog: object) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):  # type: ignore[attr-defined]
            result = proc("garbage")
        assert result == "garbage"
        records = [r for r in caplog.records if r.levelno == logging.WARNING]  # type: ignore[attr-defined]
        assert any("Date processor" in r.message for r in records), (
            f"expected a WARNING about the unparseable date string; got {records!r}"
        )

    def test_parseable_iso_string_no_warning(self, caplog: object) -> None:
        proc = _DqliteDate().result_processor(None, None)
        assert proc is not None
        with caplog.at_level(logging.WARNING, logger="sqlalchemydqlite.base"):  # type: ignore[attr-defined]
            result = proc("2024-01-02")
        import datetime as _dt

        assert isinstance(result, _dt.date)
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]  # type: ignore[attr-defined]
        assert warnings == []
