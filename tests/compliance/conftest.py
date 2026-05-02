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
    """
    _sa_modify_items(session, config, items)

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
