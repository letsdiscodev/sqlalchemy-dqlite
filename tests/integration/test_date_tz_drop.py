"""Lock in _DqliteDate UTC-day semantics: tz-aware datetime narrowed via
.date() silently drops tzinfo (datetime.date has no tz support)."""

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
        utc_midnight = datetime.datetime(2024, 1, 15, 23, 30, tzinfo=datetime.UTC)
        with Session(engine) as s:
            s.add(DateModel(d=utc_midnight.date()))
            s.commit()
            result = s.query(DateModel).order_by(DateModel.id.desc()).first()
            assert result is not None
            assert isinstance(result.d, datetime.date)
            assert result.d == datetime.date(2024, 1, 15)
