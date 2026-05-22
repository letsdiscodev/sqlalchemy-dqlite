"""Pin: ``is_disconnect`` recognises the dbapi's
``InterfaceError("Connection used after fork; ...")`` raise so the SA
pool invalidates the fork-inherited slot.

Threat model: fork-based deployments (gunicorn ``preload``, Celery
prefork, ``multiprocessing.Pool``) that inherit a SA engine from the
parent. The dbapi's cached creator-pid no longer matches the child
process; every method raises
``InterfaceError("Connection used after fork; reconstruct from
configuration in the target process. ...")`` (or the aio twin
``AsyncConnection used after fork; ...``).

Without recognition in ``is_disconnect``, the SA pool keeps the
fork-poisoned slot and the next checkout returns the same dead
inner connection. With it, the pool invalidates the slot and the
next acquire rebuilds.

The substring ``"used after fork"`` is the dbapi's canonical phrase
across all raise sites in ``dqlitedbapi.connection`` and
``dqlitedbapi.aio.connection``. It is specific enough that
user-raised ``InterfaceError("forked workflow not supported by this
trigger")`` does NOT trip disconnect classification (the bare token
``"fork"`` would, ``"used after fork"`` does not).
"""

from __future__ import annotations

from dqlitedbapi.exceptions import InterfaceError
from sqlalchemydqlite.base import DqliteDialect


def _make_dialect() -> DqliteDialect:
    return DqliteDialect()


def test_used_after_fork_interfaceerror_recognised_as_disconnect() -> None:
    dialect = _make_dialect()
    e = InterfaceError(
        "Connection used after fork; reconstruct from configuration "
        "in the target process. (created in pid 1234, current pid 5678)"
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_async_used_after_fork_interfaceerror_recognised_as_disconnect() -> None:
    dialect = _make_dialect()
    e = InterfaceError(
        "AsyncConnection used after fork; reconstruct from "
        "configuration in the target process. (created in pid 1234, "
        "current pid 5678)"
    )
    assert dialect.is_disconnect(e, None, None) is True


def test_unrelated_fork_word_not_classified_as_disconnect() -> None:
    """False-positive guard: an ``InterfaceError`` that contains
    ``"fork"`` somewhere unrelated must NOT trip disconnect
    classification. The substring is specifically ``"used after fork"``,
    not the bare token ``"fork"``.
    """
    dialect = _make_dialect()
    e = InterfaceError("forked workflow not supported by this trigger")
    assert dialect.is_disconnect(e, None, None) is False
