"""Lock in reflected column type-class identity; a pysqlite ischema_names
change could silently land a reflected column in NULLTYPE. This is the canary.
"""

from collections.abc import Generator

import pytest
from sqlalchemy import (
    BLOB,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    Time,
    create_engine,
    inspect,
    types,
)
from sqlalchemy.engine import Engine

BOOLEAN_LIKE = (types.Boolean, types.Numeric, types.SmallInteger, types.Integer)


@pytest.mark.integration
class TestReflectColumnTypes:
    @pytest.fixture
    def engine(self, engine_url: str) -> Generator[Engine]:
        engine = create_engine(engine_url)
        md = MetaData()
        Table(
            "reflect_types_test",
            md,
            Column("id", Integer, primary_key=True),
            Column("name", String(32)),
            Column("body", Text),
            Column("blob_data", BLOB),
            Column("price", Numeric(10, 2)),
            Column("ratio", Float),
            Column("created", DateTime),
            Column("birthday", Date),
            Column("start_time", Time),
            Column("flag", Boolean),
        )
        md.create_all(engine)
        try:
            yield engine
        finally:
            md.drop_all(engine)
            engine.dispose()

    def test_reflects_standard_types(self, engine: Engine) -> None:
        insp = inspect(engine)
        cols = {c["name"]: c for c in insp.get_columns("reflect_types_test")}

        assert isinstance(cols["id"]["type"], types.Integer)
        assert isinstance(cols["name"]["type"], types.String)
        assert isinstance(cols["body"]["type"], types.Text | types.String)
        assert isinstance(cols["blob_data"]["type"], types.LargeBinary | types.BLOB)
        assert isinstance(cols["price"]["type"], types.Numeric | types.Float | types.Integer)
        assert isinstance(cols["ratio"]["type"], types.Float | types.Numeric)
        assert isinstance(cols["created"]["type"], types.DateTime)
        assert isinstance(cols["birthday"]["type"], types.Date)
        # TIME reflects to Time/String/DateTime depending on pysqlite version.
        assert isinstance(cols["start_time"]["type"], types.Time | types.String | types.DateTime)
        assert isinstance(cols["flag"]["type"], BOOLEAN_LIKE)

        for name, c in cols.items():
            assert not isinstance(c["type"], types.NullType), f"column {name} reflected to NullType"
