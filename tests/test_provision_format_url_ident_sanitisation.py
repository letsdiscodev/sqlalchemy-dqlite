"""Pin: ``_format_url``'s ident sanitisation honours the narrative
comment's promise to scrub ``@`` / path-separators / control chars.

Before round 8 the implementation only replaced ``/`` and ``@`` while
the surrounding comment promised "control chars" handling — a
docstring-vs-implementation divergence. The fix widens the scrub to
match the documented intent: ``@``, ``/``, ``\\``, C0 control
characters (``\\x00``..``\\x1f``), and U+2028 / U+2029 (LF-equivalent
in many log pipelines / journald). Other characters (alphanumerics,
dots, dashes, underscores) survive verbatim — this is not a strict
``[A-Za-z0-9_]`` allowlist; legitimate idents with version suffixes
or hyphens stay readable.

``ident`` is normally pytest-xdist's ``gw0`` / ``gw1`` shape, so the
live exposure is bounded; the pin is defense-in-depth and a
docstring-vs-implementation reconciliation.
"""

from __future__ import annotations

from sqlalchemy.engine import url as sa_url

from sqlalchemydqlite.provision import _format_url


def _base() -> sa_url.URL:
    return sa_url.make_url("dqlite://127.0.0.1:9001/test")


def test_ident_with_lf_replaced() -> None:
    """LF in ``ident`` must not survive into the database name; an LF
    in a URL echoed to a structured log splits the record."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\nINJECT")
    assert out.database is not None
    assert "\n" not in out.database
    assert "INJECT" in out.database  # body preserved, LF replaced


def test_ident_with_cr_replaced() -> None:
    """CR is the other half of CRLF log-record splitting."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\rINJECT")
    assert out.database is not None
    assert "\r" not in out.database


def test_ident_with_tab_replaced() -> None:
    """TAB is a C0 control character; structured loggers using TSV
    encode it as a column separator."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\tx")
    assert out.database is not None
    assert "\t" not in out.database


def test_ident_with_nul_replaced() -> None:
    """NUL passes through to the wire layer where ``encode_text``
    late-rejects it with an obscure error; reject it at the source."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x00x")
    assert out.database is not None
    assert "\x00" not in out.database


def test_ident_with_backslash_replaced() -> None:
    """The comment promises "path-separators" plural; the unix-only
    forward-slash strip was incomplete."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\\evil")
    assert out.database is not None
    assert "\\" not in out.database


def test_ident_with_u2028_replaced() -> None:
    """U+2028 is LINE SEPARATOR — LF-equivalent in journald and many
    JSON log encoders."""
    out = _format_url(_base(), "dqlitedbapi", "gw0 x")
    assert out.database is not None
    assert " " not in out.database


def test_ident_with_u2029_replaced() -> None:
    """U+2029 is PARAGRAPH SEPARATOR — same hazard as U+2028."""
    out = _format_url(_base(), "dqlitedbapi", "gw0 x")
    assert out.database is not None
    assert " " not in out.database


def test_ident_with_at_replaced() -> None:
    """Regression pin for the original ``@`` strip."""
    out = _format_url(_base(), "dqlitedbapi", "gw0@host")
    assert out.database is not None
    assert "@" not in out.database


def test_ident_with_slash_replaced() -> None:
    """Regression pin for the original ``/`` strip."""
    out = _format_url(_base(), "dqlitedbapi", "gw0/foo")
    assert out.database is not None
    assert "/" not in out.database


def test_ident_ordinary_alphanumeric_unchanged() -> None:
    """``gw0`` — the pytest-xdist convention — survives verbatim."""
    out = _format_url(_base(), "dqlitedbapi", "gw0")
    assert out.database is not None
    assert "_gw0" in out.database


def test_ident_with_dot_preserved() -> None:
    """The scrub is not a strict alphanumeric allowlist — legitimate
    idents with version suffixes (``gw0.1``) or hyphens stay
    readable. The architect's note (c) explicitly warns against
    over-tightening to ``[A-Za-z0-9_]``."""
    out = _format_url(_base(), "dqlitedbapi", "gw0.1")
    assert out.database is not None
    assert "gw0.1" in out.database


def test_ident_with_hyphen_preserved() -> None:
    """Hyphens in idents (custom fixture configs) survive."""
    out = _format_url(_base(), "dqlitedbapi", "worker-a")
    assert out.database is not None
    assert "worker-a" in out.database


def test_ident_already_suffixed_path_also_sanitised() -> None:
    """The already-suffixed branch (when ``_SESSION_TOKEN`` is already
    present in the database name) must apply the same scrub. Before
    round 8 it used the same incomplete two-character strip."""
    from sqlalchemydqlite.provision import _SESSION_TOKEN

    # Build a URL whose database name already carries the session
    # token, so the already-suffixed branch fires.
    pre_suffixed = sa_url.make_url(f"dqlite://127.0.0.1:9001/test_{_SESSION_TOKEN}")
    out = _format_url(pre_suffixed, "dqlitedbapi", "gw0\nx")
    assert out.database is not None
    assert "\n" not in out.database
