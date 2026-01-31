from __future__ import annotations

import gzip
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence


def _iso(ts: datetime | None = None) -> str:
    ts = ts or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


class EdgarDatabase:
    """
    Lightweight SQLite store for EDGAR submissions + accession metadata.
    """

    def __init__(self, db_path: Path, *, conn: sqlite3.Connection | None = None) -> None:
        self.db_path = Path(db_path)
        self._owns_conn = conn is None
        if conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.db_path, isolation_level=None)
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS edgar_submissions (
                cik TEXT PRIMARY KEY,
                entity_name TEXT,
                fetched_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS edgar_accession (
                cik TEXT NOT NULL,
                accession TEXT NOT NULL,
                base_url TEXT NOT NULL,
                file_count INTEGER NOT NULL,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (cik, accession)
            );

            CREATE TABLE IF NOT EXISTS edgar_accession_file (
                accession TEXT NOT NULL,
                file_name TEXT NOT NULL,
                fetched_at TEXT,
                compressed_payload BLOB,
                PRIMARY KEY (accession, file_name),
                FOREIGN KEY(accession) REFERENCES edgar_accession(accession)
            );
            """
        )

    def close(self) -> None:
        if self._owns_conn:
            self.conn.close()

    def record_submissions(self, cik: str, entity_name: str | None, payload: Mapping[str, object], *, fetched_at: datetime | None = None) -> None:
        ts = _iso(fetched_at)
        self.conn.execute(
            """
            INSERT INTO edgar_submissions (cik, entity_name, fetched_at, payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(cik) DO UPDATE SET
                entity_name=excluded.entity_name,
                fetched_at=excluded.fetched_at,
                payload=excluded.payload
            """,
            (cik, entity_name, ts, json.dumps(payload)),
        )
        self.conn.commit()

    def record_accession_index(
        self,
        cik: str,
        accession: str,
        base_url: str,
        files: Sequence[str],
        *,
        fetched_at: datetime | None = None,
    ) -> None:
        ts = _iso(fetched_at)
        self.conn.execute(
            """
            INSERT INTO edgar_accession (cik, accession, base_url, file_count, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cik, accession) DO UPDATE SET
                base_url=excluded.base_url,
                file_count=excluded.file_count,
                fetched_at=excluded.fetched_at
            """,
            (cik, accession, base_url, len(files), ts),
        )
        if files:
            existing = {row["file_name"] for row in self.conn.execute("SELECT file_name FROM edgar_accession_file WHERE accession = ?", (accession,)).fetchall()}
            rows = []
            for name in files:
                if not name:
                    continue
                if name in existing:
                    continue
                rows.append((accession, name, ts, None))
            if rows:
                self.conn.executemany(
                    "INSERT INTO edgar_accession_file (accession, file_name, fetched_at, compressed_payload) VALUES (?, ?, ?, ?)",
                    rows,
                )
        self.conn.commit()

    def _compress_payload(self, payload: bytes) -> bytes:
        return gzip.compress(payload)

    def _decompress_payload(self, payload: bytes) -> bytes:
        return gzip.decompress(payload)

    def has_file(self, accession: str, file_name: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM edgar_accession_file WHERE accession = ? AND file_name = ? AND compressed_payload IS NOT NULL",
            (accession, file_name),
        )
        return cur.fetchone() is not None

    def store_file(self, accession: str, file_name: str, payload: bytes, *, fetched_at: datetime | None = None) -> None:
        ts = _iso(fetched_at)
        compressed = self._compress_payload(payload)
        self.conn.execute(
            """
            INSERT INTO edgar_accession_file (accession, file_name, fetched_at, compressed_payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(accession, file_name) DO UPDATE SET
                fetched_at=excluded.fetched_at,
                compressed_payload=excluded.compressed_payload
            """,
            (accession, file_name, ts, compressed),
        )
        self.conn.commit()

    def get_file(self, accession: str, file_name: str) -> bytes | None:
        cur = self.conn.execute(
            "SELECT compressed_payload FROM edgar_accession_file WHERE accession = ? AND file_name = ?",
            (accession, file_name),
        )
        row = cur.fetchone()
        if row is None or row["compressed_payload"] is None:
            return None
        return self._decompress_payload(bytes(row["compressed_payload"]))

    def get_accession_files(self, accession: str) -> list[str]:
        cur = self.conn.execute(
            "SELECT file_name FROM edgar_accession_file WHERE accession = ? ORDER BY file_name",
            (accession,),
        )
        return [row["file_name"] for row in cur.fetchall()]
