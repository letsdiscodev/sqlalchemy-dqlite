"""is_disconnect classifies SQLITE_NOTFOUND + "no database opened" as a disconnect
(gateway.c LOOKUP_DB arm). The orthogonal LOOKUP_STMT arm shares code 12 but is a
server-side state bug, so the discriminator is the LEADER_LOST_DB_LOOKUP_SUBSTRING."""

from __future__ import annotations

import pytest

from dqliteclient.exceptions import OperationalError as _ClientOperationalError
from dqlitedbapi.exceptions import InternalError as _DBAPIInternalError
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
    """code=SQLITE_NOTFOUND plus the leader-flip prefix in raw_message classifies."""
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
    """code=SQLITE_NOTFOUND with LOOKUP_STMT wording is a state bug and must NOT classify."""
    e = err_class(  # type: ignore[call-arg]
        "no statement with the given id 7",
        code=SQLITE_NOTFOUND,
        raw_message="no statement with the given id 7",
    )
    assert dialect.is_disconnect(e, None, None) is False


def test_is_disconnect_classifies_notfound_lookup_db_via_internalerror_with_chained_cause(
    dialect: DqliteDialect_aio,
) -> None:
    """Production shape: dbapi maps NOTFOUND to InternalError chaining the client
    OperationalError as __cause__, so SA must classify via the cause-chain walker."""
    cause = _ClientOperationalError(
        f"{LEADER_LOST_DB_LOOKUP_SUBSTRING} (db_id=7)",
        SQLITE_NOTFOUND,
        raw_message=f"{LEADER_LOST_DB_LOOKUP_SUBSTRING} (db_id=7)",
    )
    try:
        raise _DBAPIInternalError(
            f"{LEADER_LOST_DB_LOOKUP_SUBSTRING} (db_id=7)",
            code=SQLITE_NOTFOUND,
            raw_message=f"{LEADER_LOST_DB_LOOKUP_SUBSTRING} (db_id=7)",
        ) from cause
    except _DBAPIInternalError as e:
        assert dialect.is_disconnect(e, None, None) is True


def test_is_disconnect_does_not_classify_notfound_lookup_stmt_via_internalerror_with_chained_cause(
    dialect: DqliteDialect_aio,
) -> None:
    """Negative twin: LOOKUP_STMT wording must NOT classify even in the chained-cause shape."""
    cause = _ClientOperationalError(
        "no statement with the given id 7",
        SQLITE_NOTFOUND,
        raw_message="no statement with the given id 7",
    )
    try:
        raise _DBAPIInternalError(
            "no statement with the given id 7",
            code=SQLITE_NOTFOUND,
            raw_message="no statement with the given id 7",
        ) from cause
    except _DBAPIInternalError as e:
        assert dialect.is_disconnect(e, None, None) is False
