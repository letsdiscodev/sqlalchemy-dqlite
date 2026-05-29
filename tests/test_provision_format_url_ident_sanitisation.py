"""Pin: ``_format_url`` scrubs ``@`` / path-separators / control chars from
``ident`` (not a strict ``[A-Za-z0-9_]`` allowlist — dots/hyphens survive)."""

from __future__ import annotations

from sqlalchemy.engine import url as sa_url

from sqlalchemydqlite.provision import _format_url


def _base() -> sa_url.URL:
    return sa_url.make_url("dqlite://127.0.0.1:9001/test")


def test_ident_with_lf_replaced() -> None:
    """LF must not survive into the database name; in a logged URL it splits
    the record."""
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
    """Not a strict alphanumeric allowlist — version suffixes like ``gw0.1`` survive."""
    out = _format_url(_base(), "dqlitedbapi", "gw0.1")
    assert out.database is not None
    assert "gw0.1" in out.database


def test_ident_with_hyphen_preserved() -> None:
    """Hyphens in idents (custom fixture configs) survive."""
    out = _format_url(_base(), "dqlitedbapi", "worker-a")
    assert out.database is not None
    assert "worker-a" in out.database


def test_ident_with_del_replaced() -> None:
    """DEL (``\\x7f``) is a ``Cc`` control char and shares the C0 TTY hazards."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x7finject")
    assert out.database is not None
    assert "\x7f" not in out.database


def test_ident_with_nel_replaced() -> None:
    """NEL (``\\x85``) is a line terminator for ``str.splitlines`` and log forwarders."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x85inject")
    assert out.database is not None
    assert "\x85" not in out.database


def test_ident_with_c1_low_replaced() -> None:
    """C1 lower boundary (``\\x80``); the scrub covers the full ``Cc`` category."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x80inject")
    assert out.database is not None
    assert "\x80" not in out.database


def test_ident_with_c1_high_replaced() -> None:
    """C1 upper boundary (``\\x9f``): last byte of the extended control range."""
    out = _format_url(_base(), "dqlitedbapi", "gw0\x9finject")
    assert out.database is not None
    assert "\x9f" not in out.database


def test_ident_already_suffixed_path_also_sanitised() -> None:
    """The already-suffixed branch (``_SESSION_TOKEN`` present) applies the same scrub."""
    from sqlalchemydqlite.provision import _SESSION_TOKEN

    pre_suffixed = sa_url.make_url(f"dqlite://127.0.0.1:9001/test_{_SESSION_TOKEN}")
    out = _format_url(pre_suffixed, "dqlitedbapi", "gw0\nx")
    assert out.database is not None
    assert "\n" not in out.database
