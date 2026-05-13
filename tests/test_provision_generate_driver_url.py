"""Pin: ``_dqlite_generate_driver_url`` driver-dispatch branches.

The SA provisioning hook fans out one base URL to one URL per
registered driver. Three branches matter and had no fast-feedback
test:

1. unknown driver -> ``None`` so SA's dispatch skips this dialect
2. each driver in ``_DRIVERNAMES`` returns a non-None rewritten URL
3. the rewritten URL carries the canonical drivername that
   ``_format_url`` produces

A regression that argued "let SA decide" on unknown drivers would
silently route foreign drivers into this dialect; without these
pins the unenforced comment at the hook is the only contract.
"""

from __future__ import annotations

from sqlalchemy.engine import url as sa_url

from sqlalchemydqlite.provision import _DRIVERNAMES, _dqlite_generate_driver_url


def test_unknown_driver_returns_none() -> None:
    """Unknown driver -> ``None``; SA's dispatch skips."""
    url = sa_url.make_url("dqlite://h:9001/db")
    assert _dqlite_generate_driver_url(url, "totally_unknown", None) is None


def test_known_drivers_each_return_rewritten_url() -> None:
    """Every member of ``_DRIVERNAMES`` resolves to a non-None URL."""
    url = sa_url.make_url("dqlite://h:9001/db")
    for driver in _DRIVERNAMES:
        out = _dqlite_generate_driver_url(url, driver, None)
        assert out is not None, f"driver {driver!r} returned None"


def test_dqlitedbapi_maps_to_bare_dqlite_drivername() -> None:
    """``_format_url`` collapses both ``dqlite`` and ``dqlitedbapi``
    to the bare ``dqlite`` drivername for the rewritten URL.
    """
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
