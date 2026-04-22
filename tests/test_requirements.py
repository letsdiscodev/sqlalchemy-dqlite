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
            "sane_rowcount",
            "sane_multi_rowcount",
            "emulated_lastrowid",
            "empty_inserts",
            "regexp_match",
            "ctes",
            "window_functions",
            "insert_returning",
            "update_returning",
            "delete_returning",
            "insert_from_select",
            "on_update_cascade",
            "self_referential_foreign_keys",
            "unique_constraint_reflection",
            "primary_key_constraint_reflection",
            "foreign_key_constraint_reflection",
            "index_reflection",
            "temporary_tables",
            "table_ddl_if_exists",
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

    def test_override_names_exist_on_sa_base(self) -> None:
        """Every override must name a real attribute on SA's
        ``SuiteRequirements``; otherwise the suite's decorators never
        consult it and the dqlite contract silently falls through to
        the SA default.
        """
        from sqlalchemy.testing.requirements import SuiteRequirements

        override_names = [
            "ctes",
            "insert_returning",
            "update_returning",
            "delete_returning",
            "on_update_cascade",
            "empty_inserts",
        ]
        for name in override_names:
            assert hasattr(SuiteRequirements, name), (
                f"Requirements.{name} overrides a name not present on "
                f"sqlalchemy.testing.requirements.SuiteRequirements — the "
                f"override is dead code"
            )

    def test_regexp_match_is_closed(self) -> None:
        """dqlite has no server-side REGEXP function and no
        ``create_function`` hook on the DBAPI, so the compliance suite
        must skip ``regexp_match`` cases rather than run them and fail.
        """
        req = Requirements()
        assert req.regexp_match.enabled is False


class TestRequirementsReturnAnnotations:
    """Static pin: every property on the Requirements class advertises
    its actual return type as ``compound``. If a future maintainer adds
    a property returning ``Any`` (or a helper that yields a bare bool),
    this test catches it before the return-type drift makes it into a
    release."""

    def test_every_property_annotates_compound_return(self) -> None:
        import typing

        from sqlalchemy.testing.exclusions import compound

        from sqlalchemydqlite.requirements import Requirements

        skipped = {"_sa_instance_state"}
        missing: list[str] = []
        for name in vars(Requirements):
            if name.startswith("_") or name in skipped:
                continue
            attr = vars(Requirements)[name]
            if not isinstance(attr, property):
                continue
            fget = attr.fget
            assert fget is not None
            hints = typing.get_type_hints(fget)
            if hints.get("return") is not compound:
                missing.append(name)
        assert not missing, (
            f"Requirements properties must annotate ``-> compound``; missing on: {sorted(missing)}"
        )
