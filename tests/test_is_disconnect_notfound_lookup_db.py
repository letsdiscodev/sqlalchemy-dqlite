"""Pin: SA's ``DqliteDialect.is_disconnect`` classifies
``OperationalError(code=SQLITE_NOTFOUND, raw_message="no database
opened ...")`` as a disconnect (Go-parity, gateway.c LOOKUP_DB arm).

The orthogonal ``LOOKUP_STMT`` arm of upstream gateway.c emits the
same primary code (12) but with a different message ("no statement
with the given id ..."). That arm is a server-side state bug
rather than a transport flip and must NOT participate in pool
invalidation — keep the existing InternalError classification from
done/ISSUE-862 / ISSUE-954 intact.

The discriminator is therefore substring-based via the wire-side
``LEADER_LOST_DB_LOOKUP_SUBSTRING`` constant. Mirrors the parallel
arm in ``dqliteclient.connection._run_protocol``.
"""

from __future__ import annotations

import pytest

from dqliteclient.exceptions import OperationalError as _ClientOperationalError
from dqlitedbapi.exceptions import OperationalError as _DBAPIOperationalError
from dqlitewire import LEADER_LOST_DB_LOOKUP_SUBSTRING, SQLITE_NOTFOUND
from sqlalchemydqlite.aio import DqliteDialect_aio


@pytest.fixture
def dialect() -> DqliteDialect_aio:
    return DqliteDialect_aio()


@pytest.mark.parametrize(
    "err_class",
    [
        _DBAPIOperationalError,
        _ClientOperationalError,
    ],
)
def test_is_disconnect_classifies_notfound_lookup_db(
    dialect: DqliteDialect_aio,
    err_class: type[Exception],
) -> None:
    """``code=SQLITE_NOTFOUND`` AND the canonical leader-flip prefix
    in ``raw_message`` → disconnect (pool slot is invalidated and a
    fresh connection is dialled). Mirrors Go's
    ``driverError::errNotFound → ErrBadConn`` arm."""
    e = err_class(  # type: ignore[call-arg]
        f"{LEADER_LOST_DB_LOOKUP_SUBSTRING} (db_id=7)",
        code=SQLITE_NOTFOUND,
        raw_message=f"{LEADER_LOST_DB_LOOKUP_SUBSTRING} (db_id=7)",
    )
    assert dialect.is_disconnect(e, None, None) is True


@pytest.mark.parametrize(
    "err_class",
    [
        _DBAPIOperationalError,
        _ClientOperationalError,
    ],
)
def test_is_disconnect_does_not_classify_notfound_lookup_stmt(
    dialect: DqliteDialect_aio,
    err_class: type[Exception],
) -> None:
    """``code=SQLITE_NOTFOUND`` with the orthogonal ``LOOKUP_STMT``
    wording is a server-side state bug, NOT a transport flip. Must
    NOT classify as disconnect — preserves the existing
    ``InternalError`` PEP 249 mapping settled in
    done/ISSUE-862 / ISSUE-954."""
    e = err_class(  # type: ignore[call-arg]
        "no statement with the given id 7",
        code=SQLITE_NOTFOUND,
        raw_message="no statement with the given id 7",
    )
    assert dialect.is_disconnect(e, None, None) is False
