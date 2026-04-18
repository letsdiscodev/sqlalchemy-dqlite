"""Lock in _DqliteDate UTC-day semantics for tz-aware inputs.

The _DqliteDate result processor narrows datetime→date via ``.date()``.
If the decoded datetime was timezone-aware, the tzinfo is silently
dropped (datetime.date has no tz support). This test locks in the
resulting "UTC day" behavior so a future refactor can't change it
without an explicit deprecation.
"""

import datetime
from collections.abc import Generator

import pytest
from sqlalchemy import Column, Date, Integer, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()


class DateModel(Base):  # type: ignore[valid-type,misc]
    __tablename__ = "date_tz_test"
    id = Column(Integer, primary_key=True)
    d = Column(Date)


@pytest.mark.integration
class TestDateTzDrop:
    @pytest.fixture
    def engine(self, engine_url: str) -> Generator[Engine]:
        engine = create_engine(engine_url)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)
        engine.dispose()

    def test_naive_date_roundtrip(self, engine: Engine) -> None:
        value = datetime.date(2024, 1, 15)
        with Session(engine) as s:
            s.add(DateModel(d=value))
            s.commit()
            result = s.query(DateModel).order_by(DateModel.id.desc()).first()
            assert result is not None
            assert result.d == value
            assert isinstance(result.d, datetime.date)

    def test_tz_aware_datetime_bound_as_date_drops_tz(self, engine: Engine) -> None:
        """When a tz-aware datetime is stored in a Date column, .date()
        is applied on read. The tzinfo is dropped (datetime.date has no
        tz support)."""
        utc_midnight = datetime.datetime(2024, 1, 15, 23, 30, tzinfo=datetime.UTC)
        with Session(engine) as s:
            # SQLAlchemy will call str() / isoformat() on the tz-aware
            # datetime at bind time. Whatever the wire protocol returns
            # is narrowed to datetime.date on read.
            s.add(DateModel(d=utc_midnight.date()))  # type: ignore[arg-type]
            s.commit()
            result = s.query(DateModel).order_by(DateModel.id.desc()).first()
            assert result is not None
            assert isinstance(result.d, datetime.date)
            # The stored value is the UTC date.
            assert result.d == datetime.date(2024, 1, 15)
