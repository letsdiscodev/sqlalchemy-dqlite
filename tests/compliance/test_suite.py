"""SQLAlchemy compliance test suite entry-point.

This single import triggers SA's pytest plugin to discover every test
class under ``sqlalchemy.testing.suite`` and run them against the
configured dqlite cluster. The dialect's :class:`~sqlalchemydqlite.requirements.Requirements`
gates which suite tests apply to dqlite — declarations marked
``exclusions.closed()`` skip with a clear "not supported by this
dialect" reason; ``exclusions.open()`` lets the suite test run.

See also:

- ``setup.cfg`` ``[sqla_testing]`` and ``[db]`` sections, which the SA
  plugin reads at session start to locate the requirements class and
  dburi.
- ``sqlalchemydqlite.provision`` — provision hooks the suite uses for
  per-follower database setup / teardown.
"""

from sqlalchemy.testing.suite import *  # noqa: F401, F403
