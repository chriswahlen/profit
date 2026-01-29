from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
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

            CREATE TABLE IF NOT EXISTS catalog_meta (
                provider TEXT PRIMARY KEY,
                refreshed_at TEXT NOT NULL,
                source_version TEXT,
                row_count INTEGER
            );
            """
        )
        self.conn.commit()

    # --- Writes (optional for Phase 2) ---------------------------------
    def upsert_instruments(self, records: Iterable[InstrumentRecord], *, last_seen: Optional[datetime] = None) -> int:
        """
        Upsert instrument rows; returns count written.
        """
        now = last_seen or datetime.now(timezone.utc)
        rows = []
        for r in records:
            rows.append(
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
            )
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
                -- Preserve original active_from; only backfill if missing.
                active_from=COALESCE(instrument_catalog.active_from, excluded.active_from),
                -- If symbol reappears, clear tombstone; otherwise keep existing active_to.
                active_to=CASE
                    WHEN instrument_catalog.active_to IS NOT NULL AND excluded.active_to IS NULL THEN NULL
                    WHEN instrument_catalog.active_to IS NULL THEN excluded.active_to
                    ELSE instrument_catalog.active_to
                END,
                last_seen=excluded.last_seen,
                attrs=excluded.attrs;
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def mark_missing_as_inactive(self, *, provider: str, seen_at: datetime, grace: float = 0.0) -> int:
        """
        Mark symbols not seen in the latest snapshot as inactive by setting active_to.

        Args:
            provider: provider code (e.g., yfinance)
            seen_at: timestamp of the current snapshot
            grace: grace period in days before tombstoning
        """
        cutoff = seen_at - timedelta(days=grace)
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE instrument_catalog
            SET active_to = ?
            WHERE provider = ?
              AND active_to IS NULL
              AND last_seen < ?
            """,
            (_dt_to_str(seen_at), provider, _dt_to_str(cutoff)),
        )
        self.conn.commit()
        return cur.rowcount

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

    # Meta helpers -------------------------------------------------------
    def read_meta(self, provider: str) -> dict | None:
        cur = self.conn.execute(
            "SELECT provider, refreshed_at, source_version, row_count FROM catalog_meta WHERE provider = ?",
            (provider,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "provider": row["provider"],
            "refreshed_at": _str_to_dt(row["refreshed_at"]),
            "source_version": row["source_version"],
            "row_count": row["row_count"],
        }

    def write_meta(self, provider: str, refreshed_at: datetime, *, source_version: str | None, row_count: int) -> None:
        self.conn.execute(
            """
            INSERT INTO catalog_meta(provider, refreshed_at, source_version, row_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(provider) DO UPDATE SET
                refreshed_at=excluded.refreshed_at,
                source_version=excluded.source_version,
                row_count=excluded.row_count;
            """,
            (provider, _dt_to_str(refreshed_at), source_version, row_count),
        )
        self.conn.commit()
