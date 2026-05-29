"""Pin: ``_dqlite_generate_driver_url`` driver-dispatch branches — unknown
driver yields ``None`` (SA skips), known drivers yield canonical URLs."""

from __future__ import annotations

from sqlalchemy.engine import url as sa_url

from sqlalchemydqlite.provision import _DRIVERNAMES, _dqlite_generate_driver_url


def test_unknown_driver_returns_none() -> None:
    url = sa_url.make_url("dqlite://h:9001/db")
    assert _dqlite_generate_driver_url(url, "totally_unknown", None) is None


def test_known_drivers_each_return_rewritten_url() -> None:
    url = sa_url.make_url("dqlite://h:9001/db")
    for driver in _DRIVERNAMES:
        out = _dqlite_generate_driver_url(url, driver, None)
        assert out is not None, f"driver {driver!r} returned None"


def test_dqlitedbapi_maps_to_bare_dqlite_drivername() -> None:
    """``_format_url`` collapses ``dqlitedbapi`` to the bare ``dqlite`` drivername."""
    url = sa_url.make_url("dqlite://h:9001/db")
    out = _dqlite_generate_driver_url(url, "dqlitedbapi", None)
    assert out is not None
    assert out.drivername == "dqlite"


def test_aio_driver_yields_dqlite_aio_drivername() -> None:
    """``aio`` resolves to the explicit ``dqlite+aio`` drivername."""
    url = sa_url.make_url("dqlite://h:9001/db")
    out = _dqlite_generate_driver_url(url, "aio", None)
    assert out is not None
    assert out.drivername == "dqlite+aio"


def test_bare_dqlite_alias_rejected_to_preserve_fail_fast() -> None:
    """``driver="dqlite"`` is not allowlisted, so a ``dqlite+dqlite://`` typo
    fails fast instead of silently routing as the bare form."""
    url = sa_url.make_url("dqlite+dqlite://h:9001/db")
    assert _dqlite_generate_driver_url(url, "dqlite", None) is None


def test_drivernames_only_contains_sa_invoked_values() -> None:
    """The allowlist is exactly the two drivernames SA returns for this dialect."""
    assert frozenset({"dqlitedbapi", "aio"}) == _DRIVERNAMES
