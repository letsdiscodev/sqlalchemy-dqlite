"""Tests for dialect requirements."""

from sqlalchemydqlite.requirements import Requirements


class TestRequirements:
    def test_properties_return_exclusion_objects(self) -> None:
        """All requirement properties must return exclusion objects, not bare booleans."""
        req = Requirements()
        properties = [
            "datetime_literals",
            "time_microseconds",
            "datetime_historic",
            "unicode_ddl",
            "savepoints",
            "two_phase_transactions",
            "temp_table_reflection",
        ]
        for prop_name in properties:
            value = getattr(req, prop_name)
            assert not isinstance(value, bool), (
                f"Requirements.{prop_name} returns a bare bool; "
                f"should return exclusions.open() or exclusions.closed()"
            )
            # Should have the enabled_for_config method used by the test runner
            assert hasattr(value, "enabled_for_config"), (
                f"Requirements.{prop_name} return value lacks enabled_for_config method"
            )
