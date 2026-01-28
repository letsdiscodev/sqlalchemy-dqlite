"""Integration tests for ORM operations."""

import datetime

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    String,
    Text,
    create_engine,
    text,
)
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


@pytest.mark.integration
class TestORMOperations:
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

    def test_create_table_and_insert(self, engine_url: str) -> None:
        engine = create_engine(engine_url)

        # Create tables
        Base.metadata.create_all(engine)

        # Insert data
        with Session(engine) as session:
            user = User(name="Alice", email="alice@example.com")
            session.add(user)
            session.commit()

            # Query data
            users = session.query(User).filter_by(name="Alice").all()
            assert len(users) == 1
            assert users[0].email == "alice@example.com"

        # Cleanup
        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_unicode_text(self, engine_url: str) -> None:
        """Test Unicode text handling including emojis, CJK, RTL."""
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)

        unicode_values = [
            # Emojis (4-byte UTF-8)
            "Hello 🎉 World",
            "🎉🎊🎁🎂",
            # CJK characters
            "中文测试",
            "日本語テスト",
            "한국어 테스트",
            # RTL languages
            "العربية",
            "עברית",
            # Mixed scripts
            "Hello 世界 🌍",
            # Combining characters
            "café résumé naïve",
        ]

        with Session(engine) as session:
            for val in unicode_values:
                # Insert
                record = UnicodeTest(content=val)
                session.add(record)
                session.commit()

                # Query and verify
                result = session.query(UnicodeTest).filter_by(content=val).first()
                assert result is not None, f"Failed to find: {repr(val)}"
                assert result.content == val, f"Mismatch for: {repr(val)}"

                # Cleanup
                session.delete(result)
                session.commit()

        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_binary_blob(self, engine_url: str) -> None:
        """Test binary blob handling including null bytes."""
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)

        blob_values = [
            b"simple",
            b"\x00\x01\x02\x03",  # Null bytes
            b"\xff\xfe\xfd",  # High bytes
            bytes(range(256)),  # All byte values
        ]

        with Session(engine) as session:
            for val in blob_values:
                # Insert
                record = BlobTest(data=val)
                session.add(record)
                session.commit()

                # Query and verify
                result = session.query(BlobTest).order_by(BlobTest.id.desc()).first()
                assert result is not None
                assert result.data == val, f"Mismatch for blob: {repr(val)}"

        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_numeric_types(self, engine_url: str) -> None:
        """Test integer, bigint, float, and boolean types.

        Note: dqlite has a known limitation where BOOLEAN NULL values are
        returned as False (type BOOLEAN with value 0) instead of NULL.
        This is because dqlite returns the column's declared type even for
        NULL values, and 0 is indistinguishable from NULL for BOOLEAN columns.
        """
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)

        test_cases = [
            # (int, bigint, float, bool)
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

                # Query and verify
                result = session.query(NumericTest).order_by(NumericTest.id.desc()).first()
                assert result is not None
                assert result.int_val == int_val
                assert result.bigint_val == bigint_val
                if float_val is not None:
                    assert abs(result.float_val - float_val) < 1e-9
                assert result.bool_val == bool_val

        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_datetime_types(self, engine_url: str) -> None:
        """Test DateTime column type.

        Note: dqlite has a known limitation where DATETIME NULL values are
        returned as empty string instead of NULL. This causes SQLAlchemy's
        DateTime processor to fail when parsing. Avoid using NULL datetime
        values with dqlite - use a sentinel value if needed.
        """
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)

        test_dates = [
            datetime.datetime(2024, 1, 15, 10, 30, 45),
            datetime.datetime(1970, 1, 1, 0, 0, 0),  # Unix epoch
            datetime.datetime(2038, 1, 19, 3, 14, 7),  # Near Y2038
            datetime.datetime.now().replace(microsecond=0),
        ]

        with Session(engine) as session:
            for dt in test_dates:
                # Note: We set updated_at to a value, not NULL, due to dqlite limitation
                record = DateTimeTest(created_at=dt, updated_at=dt)
                session.add(record)
                session.commit()

                # Query and verify
                result = session.query(DateTimeTest).order_by(DateTimeTest.id.desc()).first()
                assert result is not None

                # SQLite stores datetime as text, compare with second precision
                assert result.created_at.year == dt.year
                assert result.created_at.month == dt.month
                assert result.created_at.day == dt.day
                assert result.created_at.hour == dt.hour
                assert result.created_at.minute == dt.minute
                assert result.created_at.second == dt.second

        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_null_handling(self, engine_url: str) -> None:
        """Test NULL values across different column types."""
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            # Insert record with all nullable fields as NULL
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

        Base.metadata.drop_all(engine)
        engine.dispose()
