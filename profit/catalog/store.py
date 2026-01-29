from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from profit.catalog.types import InstrumentRecord


def _dt_to_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _str_to_dt(val: str) -> datetime:
    return datetime.fromisoformat(val)


class CatalogStore:
    """
    Lightweight SQLite-backed catalog of instruments and identifier mappings.
    Writes are optional; Phase 2 focuses on read queries.
    """

    def __init__(self, db_path: Path, *, readonly: bool = False) -> None:
        self.db_path = Path(db_path)
        if not readonly:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        uri = f"file:{self.db_path.as_posix()}" + ("?mode=ro" if readonly else "")
        self.conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        if not readonly:
            self.ensure_schema()

    def ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS instrument_catalog (
                instrument_id   TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                provider        TEXT NOT NULL,
                provider_code   TEXT NOT NULL,
                mic             TEXT,
                currency        TEXT,
                active_from     TEXT NOT NULL,
                active_to       TEXT,
                last_seen       TEXT NOT NULL,
                attrs           TEXT,
                PRIMARY KEY (provider, provider_code)
            );

            CREATE TABLE IF NOT EXISTS identifier_map (
                instrument_id TEXT NOT NULL,
                scheme        TEXT NOT NULL,
                value         TEXT NOT NULL,
                active_from   TEXT NOT NULL,
                active_to     TEXT,
                source        TEXT,
                PRIMARY KEY (scheme, value, active_from)
            );

            CREATE INDEX IF NOT EXISTS idx_instrument_catalog_id ON instrument_catalog(instrument_id);
            CREATE INDEX IF NOT EXISTS idx_identifier_map_instr ON identifier_map(instrument_id);
            """
        )
        self.conn.commit()

    # --- Writes (optional for Phase 2) ---------------------------------
    def upsert_instruments(self, records: Iterable[InstrumentRecord], *, last_seen: Optional[datetime] = None) -> int:
        """
        Upsert instrument rows; returns count written.
        """
        now = last_seen or datetime.now(timezone.utc)
        rows = [
            (
                r.instrument_id,
                r.instrument_type,
                r.provider,
                r.provider_code,
                r.mic,
                r.currency,
                _dt_to_str(r.active_from),
                _dt_to_str(r.active_to) if r.active_to else None,
                _dt_to_str(now),
                json.dumps(r.attrs or {}),
            )
            for r in records
        ]
        if not rows:
            return 0
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO instrument_catalog (
                instrument_id, instrument_type, provider, provider_code,
                mic, currency, active_from, active_to, last_seen, attrs
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_code) DO UPDATE SET
                instrument_id=excluded.instrument_id,
                instrument_type=excluded.instrument_type,
                mic=excluded.mic,
                currency=excluded.currency,
                active_from=excluded.active_from,
                active_to=excluded.active_to,
                last_seen=excluded.last_seen,
                attrs=excluded.attrs;
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    # --- Reads ---------------------------------------------------------
    def search_instruments(
        self,
        *,
        query: str | None = None,
        provider: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[InstrumentRecord]:
        clauses = []
        params: list[str | int] = []
        if provider:
            clauses.append("provider = ?")
            params.append(provider)
        if query:
            like = f"%{query}%"
            clauses.append("(provider_code LIKE ? OR instrument_id LIKE ?)")
            params.extend([like, like])
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        sql = f"""
        SELECT instrument_id, instrument_type, provider, provider_code, mic, currency,
               active_from, active_to, attrs
        FROM instrument_catalog
        {where}
        ORDER BY provider, provider_code COLLATE NOCASE
        LIMIT ? OFFSET ?;
        """
        params.extend([limit, offset])
        cur = self.conn.execute(sql, params)
        return [self._row_to_record(row) for row in cur.fetchall()]

    def get_instrument(self, provider: str, provider_code: str) -> InstrumentRecord | None:
        cur = self.conn.execute(
            """
            SELECT instrument_id, instrument_type, provider, provider_code, mic, currency,
                   active_from, active_to, attrs
            FROM instrument_catalog
            WHERE provider = ? AND provider_code = ?
            """,
            (provider, provider_code),
        )
        row = cur.fetchone()
        return self._row_to_record(row) if row else None

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> InstrumentRecord:
        attrs = row["attrs"]
        parsed_attrs = json.loads(attrs) if attrs else {}
        return InstrumentRecord(
            instrument_id=row["instrument_id"],
            instrument_type=row["instrument_type"],
            provider=row["provider"],
            provider_code=row["provider_code"],
            mic=row["mic"],
            currency=row["currency"],
            active_from=_str_to_dt(row["active_from"]),
            active_to=_str_to_dt(row["active_to"]) if row["active_to"] else None,
            attrs=parsed_attrs,
        )
