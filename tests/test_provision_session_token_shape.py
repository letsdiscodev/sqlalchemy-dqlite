"""Pin ``_SESSION_TOKEN``'s shape/origin and that the module docstring keeps
the fork-inheritance caveat (no ``register_at_fork``, which would break the
cross-process URL idempotence check)."""

from __future__ import annotations

import os
import re

from sqlalchemydqlite import provision as p


def test_session_token_is_module_level_constant() -> None:
    """Captured once at import; a function-local would break the
    ``_SESSION_TOKEN in database`` idempotence check in ``_format_url``."""
    assert isinstance(p._SESSION_TOKEN, str)
    assert p._SESSION_TOKEN == p._SESSION_TOKEN


def test_session_token_shape_is_sa_pid_monotonicns() -> None:
    """Token shape ``sa_<pid>_<monotonic-ns>``: safe across dbapi/URL/wire layers."""
    assert re.match(r"^sa_\d+_\d+$", p._SESSION_TOKEN), (
        f"token shape must be 'sa_<pid>_<monotonic-ns>'; got {p._SESSION_TOKEN!r}"
    )


def test_session_token_embeds_importing_process_pid() -> None:
    """In the importing process the token's pid matches ``os.getpid()``
    (under fork-based xdist a worker's token still names the controller's pid)."""
    pid_component = int(p._SESSION_TOKEN.split("_")[1])
    assert pid_component == os.getpid()
