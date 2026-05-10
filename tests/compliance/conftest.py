"""Conftest scoped to the SQLAlchemy compliance suite.

Loading the SA testing pytest plugin replaces pytest's default test
collection (it filters to ``sqlalchemy.testing.fixtures.TestBase``
subclasses only). To avoid hijacking the project's other tests, scope
the plugin to this directory by giving the suite its own ``conftest``;
running ``pytest tests/`` discovers regular tests without the SA
plugin, while ``pytest tests/compliance/`` loads SA's plugin and runs
the compliance suite.
"""

from typing import Any

import pytest
from sqlalchemy.testing.plugin import pytestplugin as _sa_pytestplugin
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: F401, F403

# The SA plugin's own ``pytest_collection_modifyitems`` does the
# requirement-based exclusion / inclusion of suite tests. Capture it
# here before redefining the hook below; we delegate first, then run
# our own dqlite-specific skip pass on top.
_sa_modify_items = _sa_pytestplugin.pytest_collection_modifyitems


# Compliance-suite tests we cannot reach via the standard
# ``Requirements`` gating. Each entry below corresponds to a test
# whose parametrize tuple includes ``use_schema=True`` but the test
# itself does not check ``requires.schemas`` — so closing
# ``schemas`` in our ``Requirements`` class doesn't skip these
# branches. They unconditionally try to query ``test_schema.*`` which
# does not exist on dqlite (no ATTACH-DATABASE workaround).
#
# pysqlite-style sidecar ATTACH DATABASE is the canonical SA fix; until
# that lands for dqlite, skip these specific parametrize variants.
_SCHEMA_USING_PARAMETRIZE_SKIPS: tuple[str, ...] = (
    # ``ComponentReflectionTest::test_metadata`` parametrises
    # use_schema=True/False without a requires.schemas gate.
    "test_metadata[False-_exclusions_01-True]",
    "test_metadata[True-_exclusions_00-True]",
)


def pytest_collection_modifyitems(  # type: ignore[no-redef]
    session: pytest.Session, config: pytest.Config, items: list[Any]
) -> None:
    """Delegate to SA's hook, then mark ungated schema-using
    parametrize variants as ``skip``.

    SA's ``pytest_collection_modifyitems`` runs the requirement-based
    exclusion that turns ``Requirements.<name>.enabled == False`` into
    pytest skips. Re-bind it before our own pass so neither is dropped.

    Fail-loud staleness pin: snapshot the pre-delegation nodeid set so
    a fragment in ``_SCHEMA_USING_PARAMETRIZE_SKIPS`` that no longer
    matches any collected SA test (because SA renamed the axes,
    recombined the parametrize, or added a requirement gate that now
    handles the test) raises ``pytest.UsageError`` at collection
    time instead of silently no-op'ing. SA test IDs are a private
    contract, so this guard catches drift on the first SA bump rather
    than weeks later when a test we thought was skipped starts
    running and passing for the wrong reason.
    """
    # Empty-fragment foot-gun: ``"" in nodeid`` is always True, so a
    # stray empty entry would mark everything skipped. Cheap eager
    # rejection.
    for fragment in _SCHEMA_USING_PARAMETRIZE_SKIPS:
        if not fragment:
            raise pytest.UsageError(
                "Empty fragment in _SCHEMA_USING_PARAMETRIZE_SKIPS would "
                "match every collected nodeid; refusing to run."
            )

    # Snapshot BEFORE delegation: SA's hook may deselect (remove from
    # ``items``) requirement-gated tests, which would falsely flag our
    # fragments as stale. The pre-snapshot is the authoritative
    # collected set for staleness purposes.
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
