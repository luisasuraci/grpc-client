from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import DateTime, BigInteger, Integer, MetaData, String, create_engine, select, func, Float
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData()


class SignalRecord(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    tag: Mapped[str] = mapped_column(String(255), index=True)
    quality: Mapped[str] = mapped_column(String(64))
    timestamp_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    unit: Mapped[str | None] = mapped_column(String(64), nullable=True)
    value_type: Mapped[str] = mapped_column(String(16))
    value_text: Mapped[str] = mapped_column(String(255))
    payload_size_bytes: Mapped[int] = mapped_column(Integer)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class SubscriptionTag(Base):
    __tablename__ = "subscription_tags"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    tag: Mapped[str] = mapped_column(String(255), index=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


@dataclass(frozen=True)
class DbConfig:
    backend: str
    host: str
    port: int
    database: str
    username: str
    password: str


def create_db_engine(config: DbConfig) -> Engine:
    backend = config.backend.lower()
    if backend == "postgresql":
        driver = "postgresql+psycopg2"
    elif backend in {"mariadb", "mysql"}:
        driver = "mysql+pymysql"
    else:
        raise ValueError("backend deve essere 'postgresql' oppure 'mariadb'")

    uri = f"{driver}://{config.username}:{config.password}@{config.host}:{config.port}/{config.database}"
    return create_engine(uri, pool_pre_ping=True, pool_recycle=1800)


def init_schema(engine: Engine) -> None:
    Base.metadata.create_all(engine)


def save_subscription_tags(engine: Engine, run_id: str, tags: Iterable[str]) -> None:
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        session.add_all([SubscriptionTag(run_id=run_id, tag=t, captured_at=now) for t in tags])
        session.commit()


def rate_per_minute(session: Session, start_ms: int | None, end_ms: int | None) -> float:
    q = select(func.count(SignalRecord.id), func.min(SignalRecord.timestamp_ms), func.max(SignalRecord.timestamp_ms))
    if start_ms is not None:
        q = q.where(SignalRecord.timestamp_ms >= start_ms)
    if end_ms is not None:
        q = q.where(SignalRecord.timestamp_ms <= end_ms)

    count, min_ts, max_ts = session.execute(q).one()
    if not count or min_ts is None or max_ts is None or max_ts == min_ts:
        return 0.0
    span_minutes = (max_ts - min_ts) / 1000 / 60
    return float(count) / span_minutes if span_minutes > 0 else 0.0


def throughput_bytes_per_second(session: Session, start_ms: int | None, end_ms: int | None) -> float:
    q = select(func.coalesce(func.sum(SignalRecord.payload_size_bytes), 0), func.min(SignalRecord.timestamp_ms), func.max(SignalRecord.timestamp_ms))
    if start_ms is not None:
        q = q.where(SignalRecord.timestamp_ms >= start_ms)
    if end_ms is not None:
        q = q.where(SignalRecord.timestamp_ms <= end_ms)
    total, min_ts, max_ts = session.execute(q).one()
    if min_ts is None or max_ts is None or max_ts == min_ts:
        return 0.0
    span_seconds = (max_ts - min_ts) / 1000
    return float(total) / span_seconds if span_seconds > 0 else 0.0
