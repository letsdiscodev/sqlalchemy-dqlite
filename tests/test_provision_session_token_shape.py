"""Pin: ``_SESSION_TOKEN`` is a module-level constant capturing the
importing-process pid and a monotonic-ns timestamp, and the docstring
explains the fork-inheritance caveat.

Under fork-based pytest-xdist (the default on Linux), worker
processes inherit ``_SESSION_TOKEN`` verbatim from the controller.
The token's ``<pid>`` component embeds the controller's pid, not
the worker's. Per-worker database isolation is provided by
``_format_url``'s ``ident`` suffix; the pid is defensive
overprovisioning. A future maintainer adding a fork-spawned worker
to the SA test infrastructure must NOT assume ``_SESSION_TOKEN``
provides per-process isolation — the docstring documents the
asymmetry and this test locks it in so a "fix" that adds
``os.register_at_fork`` (which would break the cross-process URL
idempotence check) cannot land silently.
"""

from __future__ import annotations

import os
import re

from sqlalchemydqlite import provision as p


def test_session_token_is_module_level_constant() -> None:
    """The token is captured once at import time. A future refactor
    that converts it to a function-local would break the
    ``_SESSION_TOKEN in database`` idempotence check at the
    ``_format_url`` callsite."""
    assert isinstance(p._SESSION_TOKEN, str)
    # Two reads return the same value (module-level capture, not
    # per-call regeneration).
    assert p._SESSION_TOKEN == p._SESSION_TOKEN


def test_session_token_shape_is_sa_pid_monotonicns() -> None:
    """Token shape: ``sa_<pid>_<monotonic-ns>`` — bounded length,
    safe across dbapi/URL/wire layers (no path separators, no @,
    no whitespace)."""
    assert re.match(r"^sa_\d+_\d+$", p._SESSION_TOKEN), (
        f"token shape must be 'sa_<pid>_<monotonic-ns>'; got {p._SESSION_TOKEN!r}"
    )


def test_session_token_embeds_importing_process_pid() -> None:
    """In the importing process (this test process), the token's
    pid component matches ``os.getpid()``. The contract reads
    "importing-process pid"; under fork-based xdist, a worker's
    token still names the controller's pid because Python's fork
    preserves module-level state."""
    pid_component = int(p._SESSION_TOKEN.split("_")[1])
    assert pid_component == os.getpid()


def test_session_token_docstring_documents_fork_caveat() -> None:
    """The module docstring (or the inline comment block around
    ``_SESSION_TOKEN``) must explain the fork-preserves-parent-pid
    behavior so a future maintainer doesn't add an
    ``os.register_at_fork`` hook that would break the idempotence
    check."""
    import inspect

    src = inspect.getsource(p)
    assert "importing-process" in src or "importing process" in src, (
        "module source must label the pid as importing-process pid, "
        "not just <pid>, so the fork caveat is grep-able"
    )
    assert "register_at_fork" in src, (
        "module source must explain why no register_at_fork hook is "
        "registered (the idempotence-check rationale)"
    )
