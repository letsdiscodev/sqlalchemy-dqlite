"""SQLAlchemy compliance test suite entry-point.

The import triggers SA's plugin to collect every ``sqlalchemy.testing.suite``
test against the dqlite cluster; ``Requirements`` gates which ones apply.
"""

from sqlalchemy.testing.suite import *  # noqa: F401, F403
