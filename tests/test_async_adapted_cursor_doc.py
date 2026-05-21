"""Pin: ``AsyncAdaptedCursor``'s PEP 249 fetch trio (``fetchone`` /
``fetchmany`` / ``fetchall``) and ``close`` carry non-empty
``__doc__`` strings that cite PEP 249.

The SA-adapter cursor IS the user-facing surface SA-using consumers
reach via ``engine.execute()`` / ``conn.execute()``; ``help()`` and
``inspect.getdoc`` against these four methods previously returned
``None`` (only inline ``#``-comments existed). Same-class neighbours
(``connection`` property, ``rownumber`` property, ``arraysize``
setter) carry docstrings, as does the underlying
``dqlitedbapi.Cursor.fetchone`` / ``fetchmany`` / ``fetchall``. Lift
the rationale from the existing inline comments where applicable;
keep the closed-cursor + no-wire-I/O invariants explicit.
"""

from __future__ import annotations

import inspect

from sqlalchemydqlite.aio import AsyncAdaptedCursor


def _doc(name: str) -> str:
    method = getattr(AsyncAdaptedCursor, name)
    text = inspect.getdoc(method)
    assert text is not None, f"AsyncAdaptedCursor.{name} has no docstring"
    assert text.strip(), f"AsyncAdaptedCursor.{name} docstring is empty"
    return text


def test_fetchone_docstring_present_and_cites_pep_249() -> None:
    text = _doc("fetchone")
    assert "PEP 249" in text


def test_fetchmany_docstring_present_and_cites_pep_249() -> None:
    text = _doc("fetchmany")
    assert "PEP 249" in text
    # The negative-size = fetchall parity is the load-bearing
    # divergence from a naive PEP 249 reading; keep it documented.
    assert "Negative" in text or "negative" in text


def test_fetchall_docstring_present_and_cites_pep_249() -> None:
    text = _doc("fetchall")
    assert "PEP 249" in text


def test_close_docstring_present_and_cites_pep_249() -> None:
    text = _doc("close")
    assert "PEP 249" in text
    # The idempotent-close contract is the public-facing invariant
    # the 30-line inline comment encodes; surface it in the docstring.
    assert "Idempotent" in text or "idempotent" in text


def test_no_server_side_prefetch_invariant_documented() -> None:
    """The ``arraysize.setter`` already documents the deque-only
    semantic (dqlite's wire delivers the full result set up-front, no
    server-side prefetch). The fetch trio should reference the same
    invariant so a reader of ``help(cursor.fetchone)`` learns that
    arraysize tuning does not affect wire round-trips.
    """
    for name in ("fetchone", "fetchmany", "fetchall"):
        text = _doc(name)
        assert "wire" in text.lower() or "prefetch" in text.lower(), (
            f"AsyncAdaptedCursor.{name} docstring should reference the "
            "no-server-side-prefetch / single-RTT invariant"
        )
