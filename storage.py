from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PersistedSignal:
    tag: str
    timestamp: int
    quality: str
    value: str
    unit: str | None
    payload_bytes: int


class SignalStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                quality TEXT NOT NULL,
                value TEXT NOT NULL,
                unit TEXT,
                payload_bytes INTEGER NOT NULL,
                inserted_at INTEGER NOT NULL DEFAULT (unixepoch())
            );

            CREATE INDEX IF NOT EXISTS idx_signals_tag_time ON signals(tag, timestamp);
            CREATE INDEX IF NOT EXISTS idx_signals_time ON signals(timestamp);

            CREATE TABLE IF NOT EXISTS per_second_stats (
                second_bucket INTEGER PRIMARY KEY,
                signal_count INTEGER NOT NULL,
                byte_count INTEGER NOT NULL
            );
            """
        )
        self.conn.commit()

    def insert_signal(self, signal: PersistedSignal) -> None:
        self.conn.execute(
            """
            INSERT INTO signals(tag, timestamp, quality, value, unit, payload_bytes)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (signal.tag, signal.timestamp, signal.quality, signal.value, signal.unit, signal.payload_bytes),
        )

        second_bucket = signal.timestamp // 1000
        self.conn.execute(
            """
            INSERT INTO per_second_stats(second_bucket, signal_count, byte_count)
            VALUES (?, 1, ?)
            ON CONFLICT(second_bucket)
            DO UPDATE SET
                signal_count = signal_count + 1,
                byte_count = byte_count + excluded.byte_count
            """,
            (second_bucket, signal.payload_bytes),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
