"""Conftest scoping the SA testing plugin to this directory.

The plugin replaces pytest's default collection (filtering to
``TestBase`` subclasses), so it must not leak into the rest of the suite.
"""

from typing import Any

import pytest
from sqlalchemy.testing.plugin import pytestplugin as _sa_pytestplugin
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: F401, F403

# Capture SA's requirement-based collection hook before redefining it; we
# delegate to it first, then run our own dqlite skip pass on top.
_sa_modify_items = _sa_pytestplugin.pytest_collection_modifyitems


# Suite tests whose ``use_schema=True`` parametrize variant is ungated by
# ``requires.schemas``, so closing ``schemas`` in Requirements doesn't skip
# them; the True variants query ``test_schema.*`` which dqlite lacks (no ATTACH).
_SCHEMA_USING_PARAMETRIZE_SKIPS: tuple[str, ...] = (
    "test_metadata[False-_exclusions_01-True]",
    "test_metadata[True-_exclusions_00-True]",
    "test_check_constraint_no_constraint[True]",
    "test_check_constraint_standalone[True-my_ck_const]",
    "test_check_constraint_standalone[True-MyCkConst]",
    "test_check_constraint_standalone[True-None]",
    "test_check_constraint_inline[True-my_inline]",
    "test_check_constraint_inline[True-MyInline]",
    "test_check_constraint_inline[True-None]",
    "test_check_constraint_mixed[True]",
)


def pytest_collection_modifyitems(  # type: ignore[no-redef]
    session: pytest.Session, config: pytest.Config, items: list[Any]
) -> None:
    """Delegate to SA's hook, then mark ungated schema-using variants skip.

    Snapshots the pre-delegation nodeids and fails loud if a skip fragment
    matches nothing — SA test IDs are a private contract, so this catches
    parametrize drift on the first SA bump.
    """
    # ``"" in nodeid`` is always True, so an empty entry would skip everything.
    for fragment in _SCHEMA_USING_PARAMETRIZE_SKIPS:
        if not fragment:
            raise pytest.UsageError(
                "Empty fragment in _SCHEMA_USING_PARAMETRIZE_SKIPS would "
                "match every collected nodeid; refusing to run."
            )

    # Snapshot before delegation: SA's hook deselects requirement-gated tests,
    # which would falsely flag our fragments as stale.
    pre_nodeids = [item.nodeid for item in items]

    _sa_modify_items(session, config, items)

    matched = {fragment: False for fragment in _SCHEMA_USING_PARAMETRIZE_SKIPS}
    for nodeid in pre_nodeids:
        for fragment in _SCHEMA_USING_PARAMETRIZE_SKIPS:
            if fragment in nodeid:
                matched[fragment] = True

    stale = [fragment for fragment, hit in matched.items() if not hit]
    if stale:
        raise pytest.UsageError(
            f"_SCHEMA_USING_PARAMETRIZE_SKIPS fragment(s) matched no "
            f"collected SA test: {stale!r}. Likely SA renamed or "
            f"recombined the parametrize axes, or added a requirement "
            f"gate that now handles the test. Update or remove the "
            f"stale entries before running the compliance suite."
        )

    skip = pytest.mark.skip(
        reason="dqlite has no ATTACH-DATABASE schema; see "
        "Requirements.schemas for the closed declaration. This "
        "parametrize variant is not reachable through the standard "
        "requirement gate."
    )
    for item in items:
        for fragment in _SCHEMA_USING_PARAMETRIZE_SKIPS:
            if fragment in item.nodeid:
                item.add_marker(skip)
                break
