"""Integration tests for ORM operations."""

import datetime
from collections.abc import Generator

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    String,
    Text,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()


class User(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))


class UnicodeTest(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "unicode_test"

    id = Column(Integer, primary_key=True)
    content = Column(Text)


class BlobTest(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "blob_test"

    id = Column(Integer, primary_key=True)
    data = Column(LargeBinary)


class NumericTest(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "numeric_test"

    id = Column(Integer, primary_key=True)
    int_val = Column(Integer)
    bigint_val = Column(BigInteger)
    float_val = Column(Float)
    bool_val = Column(Boolean)


class DateTimeTest(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "datetime_test"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime, nullable=True)


class DateOnlyTest(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "date_only_test"

    id = Column(Integer, primary_key=True)
    d = Column(Date)


@pytest.mark.integration
class TestORMOperations:
    @pytest.fixture
    def engine(self, engine_url: str) -> Generator[Engine]:
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_create_engine(self, engine_url: str) -> None:
        engine = create_engine(engine_url)
        assert engine is not None
        engine.dispose()

    def test_raw_sql(self, engine_url: str) -> None:
        engine = create_engine(engine_url)

        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            row = result.fetchone()
            assert row is not None
            assert row[0] == 1

        engine.dispose()

    def test_create_table_and_insert(self, engine: Engine) -> None:
        with Session(engine) as session:
            user = User(name="Alice", email="alice@example.com")
            session.add(user)
            session.commit()

            users = session.query(User).filter_by(name="Alice").all()
            assert len(users) == 1
            assert users[0].email == "alice@example.com"

    def test_unicode_text(self, engine: Engine) -> None:
        unicode_values = [
            "Hello \U0001f389 World",
            "\U0001f389\U0001f38a\U0001f381\U0001f382",
            "\u4e2d\u6587\u6d4b\u8bd5",
            "\u65e5\u672c\u8a9e\u30c6\u30b9\u30c8",
            "\ud55c\uad6d\uc5b4 \ud14c\uc2a4\ud2b8",
            "\u0627\u0644\u0639\u0631\u0628\u064a\u0629",
            "\u05e2\u05d1\u05e8\u05d9\u05ea",
            "Hello \u4e16\u754c \U0001f30d",
            "caf\u00e9 r\u00e9sum\u00e9 na\u00efve",
        ]

        with Session(engine) as session:
            for val in unicode_values:
                record = UnicodeTest(content=val)
                session.add(record)
                session.commit()

                result = session.query(UnicodeTest).filter_by(content=val).first()
                assert result is not None, f"Failed to find: {repr(val)}"
                assert result.content == val, f"Mismatch for: {repr(val)}"

                session.delete(result)
                session.commit()

    def test_binary_blob(self, engine: Engine) -> None:
        blob_values = [
            b"simple",
            b"\x00\x01\x02\x03",
            b"\xff\xfe\xfd",
            bytes(range(256)),
        ]

        with Session(engine) as session:
            for val in blob_values:
                record = BlobTest(data=val)
                session.add(record)
                session.commit()

                result = session.query(BlobTest).order_by(BlobTest.id.desc()).first()
                assert result is not None
                assert result.data == val, f"Mismatch for blob: {repr(val)}"

    def test_numeric_types(self, engine: Engine) -> None:
        test_cases = [
            (0, 0, 0.0, False),
            (1, 1, 1.0, True),
            (-1, -1, -1.0, False),
            (2147483647, 9223372036854775807, 3.14159265358979, True),
            (-2147483648, -9223372036854775808, -3.14159265358979, False),
        ]

        with Session(engine) as session:
            for int_val, bigint_val, float_val, bool_val in test_cases:
                record = NumericTest(
                    int_val=int_val,
                    bigint_val=bigint_val,
                    float_val=float_val,
                    bool_val=bool_val,
                )
                session.add(record)
                session.commit()

                result = session.query(NumericTest).order_by(NumericTest.id.desc()).first()
                assert result is not None
                assert result.int_val == int_val
                assert result.bigint_val == bigint_val
                if float_val is not None:
                    assert abs(result.float_val - float_val) < 1e-9
                assert result.bool_val == bool_val

    def test_datetime_types(self, engine: Engine) -> None:
        test_dates = [
            datetime.datetime(2024, 1, 15, 10, 30, 45),
            datetime.datetime(1970, 1, 1, 0, 0, 0),
            datetime.datetime(2038, 1, 19, 3, 14, 7),
            datetime.datetime.now().replace(microsecond=0),
        ]

        with Session(engine) as session:
            for dt in test_dates:
                record = DateTimeTest(created_at=dt, updated_at=dt)
                session.add(record)
                session.commit()

                result = session.query(DateTimeTest).order_by(DateTimeTest.id.desc()).first()
                assert result is not None

                assert isinstance(result.created_at, datetime.datetime)
                # Default DateTime is timezone=False: naive in, naive out.
                assert result.created_at.tzinfo is None
                assert result.created_at == dt

    def test_datetime_null_roundtrip(self, engine: Engine) -> None:
        with Session(engine) as session:
            record = DateTimeTest(
                created_at=datetime.datetime(2024, 1, 1, 0, 0, 0),
                updated_at=None,
            )
            session.add(record)
            session.commit()

            result = session.query(DateTimeTest).order_by(DateTimeTest.id.desc()).first()
            assert result is not None
            assert result.updated_at is None

    def test_date_column_returns_date(self, engine: Engine) -> None:
        """Column(Date) returns datetime.date: _DqliteDate.result_processor narrows
        the DBAPI's datetime (ISO8601-tagged columns) to date on read."""
        d = datetime.date(2024, 3, 14)
        with Session(engine) as session:
            session.add(DateOnlyTest(d=d))
            session.commit()

            result = session.query(DateOnlyTest).order_by(DateOnlyTest.id.desc()).first()
            assert result is not None
            assert type(result.d) is datetime.date
            assert result.d == d

    def test_null_handling(self, engine: Engine) -> None:
        with Session(engine) as session:
            record = NumericTest(
                int_val=None,
                bigint_val=None,
                float_val=None,
                bool_val=None,
            )
            session.add(record)
            session.commit()

            result = session.query(NumericTest).first()
            assert result is not None
            assert result.int_val is None
            assert result.bigint_val is None
            assert result.float_val is None
            assert result.bool_val is None
