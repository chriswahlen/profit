from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


TABLE_SQL = """
CREATE TABLE IF NOT EXISTS seed_metadata (
    seeder TEXT PRIMARY KEY,
    last_run TEXT NOT NULL
)
"""


def ensure_seed_metadata(conn: sqlite3.Connection) -> None:
    conn.execute(TABLE_SQL)


def read_seed_metadata(conn: sqlite3.Connection, seeder: str) -> datetime | None:
    cur = conn.execute("SELECT last_run FROM seed_metadata WHERE seeder = ?", (seeder,))
    row = cur.fetchone()
    if row is None:
        return None
    return datetime.fromisoformat(row[0]).astimezone(timezone.utc)


def write_seed_metadata(conn: sqlite3.Connection, seeder: str, ts: datetime) -> None:
    conn.execute(
        """
        INSERT INTO seed_metadata (seeder, last_run)
        VALUES (?, ?)
        ON CONFLICT(seeder) DO UPDATE SET last_run = excluded.last_run
        """,
        (seeder, ts.isoformat()),
    )
