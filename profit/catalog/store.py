from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, List, Optional

from profit.catalog.types import InstrumentRecord


def _dt_to_str(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _maybe_dt_to_str(dt: datetime | None) -> str | None:
    return _dt_to_str(dt) if dt is not None else None


def _str_to_dt(val: str) -> datetime:
    return datetime.fromisoformat(val)


class CatalogStore:
    """
    Lightweight SQLite-backed catalog of instruments and identifier mappings.
    Writes are optional; Phase 2 focuses on read queries.
    """

    def __init__(self, db_path: Path, *, readonly: bool = False, conn: sqlite3.Connection | None = None) -> None:
        self.db_path = Path(db_path)
        self._owns_conn = conn is None
        if conn is None:
            if not readonly:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            uri = f"file:{self.db_path.as_posix()}" + ("?mode=ro" if readonly else "")
            conn = sqlite3.connect(uri, uri=True, isolation_level=None)
            conn.execute("PRAGMA busy_timeout=5000")
            if not readonly:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        if not readonly:
            self.ensure_schema()

    def ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            -- Global instruments
            CREATE TABLE IF NOT EXISTS instrument (
                instrument_id   TEXT PRIMARY KEY,
                instrument_type TEXT NOT NULL,
                description     TEXT,
                mic_primary     TEXT,
                currency        TEXT,
                status          TEXT NOT NULL DEFAULT 'active',
                active_from     TEXT,
                active_to       TEXT
            );

            -- Provider mapping
            CREATE TABLE IF NOT EXISTS instrument_provider_map (
                provider      TEXT NOT NULL,
                provider_code TEXT NOT NULL,
                instrument_id TEXT NOT NULL REFERENCES instrument(instrument_id),
                active_from   TEXT,
                active_to     TEXT,
                last_seen     TEXT NOT NULL,
                attrs         TEXT,
                PRIMARY KEY (provider, provider_code)
            );
            CREATE INDEX IF NOT EXISTS idx_instr_prov_map_instr ON instrument_provider_map(instrument_id);

            -- Instrument/entity relations (multi-role)
            CREATE TABLE IF NOT EXISTS instrument_entity (
                instrument_id TEXT NOT NULL REFERENCES instrument(instrument_id),
                entity_id     TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                active_from   TEXT NOT NULL,
                active_to     TEXT,
                PRIMARY KEY (instrument_id, entity_id, relation_type, active_from)
            );

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
        records = list(records)
        if not records:
            return 0
        cur = self.conn.cursor()
        # Ensure instruments exist globally.
        instrument_rows = []
        provider_rows = []
        for r in records:
            instrument_rows.append(
                (
                    r.instrument_id,
                    r.instrument_type,
                    None,  # description
                    r.mic,
                    r.currency,
                    "active",
                    _maybe_dt_to_str(r.active_from),
                    _maybe_dt_to_str(r.active_to),
                )
            )
            provider_rows.append(
                (
                    r.instrument_id,
                    r.provider,
                    r.provider_code,
                    _maybe_dt_to_str(r.active_from),
                    _maybe_dt_to_str(r.active_to),
                    _dt_to_str(now),
                    json.dumps(r.attrs or {}),
                )
            )
        cur.executemany(
            """
            INSERT INTO instrument (
                instrument_id, instrument_type, description, mic_primary, currency,
                status, active_from, active_to
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id) DO UPDATE SET
                instrument_type=excluded.instrument_type,
                mic_primary=COALESCE(excluded.mic_primary, instrument.mic_primary),
                currency=COALESCE(excluded.currency, instrument.currency),
                active_from=COALESCE(excluded.active_from, instrument.active_from),
                active_to=CASE
                    WHEN instrument.active_to IS NOT NULL AND excluded.active_to IS NULL THEN instrument.active_to
                    ELSE COALESCE(excluded.active_to, instrument.active_to)
                END
            """,
            instrument_rows,
        )
        cur.executemany(
            """
            INSERT INTO instrument_provider_map (
                instrument_id, provider, provider_code,
                active_from, active_to, last_seen, attrs
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_code) DO UPDATE SET
                instrument_id=excluded.instrument_id,
                -- Preserve earliest known active_from (backfill if earlier is provided).
                active_from=MIN(instrument_provider_map.active_from, excluded.active_from),
                -- If symbol reappears, clear tombstone; otherwise keep existing active_to.
                active_to=CASE
                    WHEN instrument_provider_map.active_to IS NOT NULL AND excluded.active_to IS NULL THEN NULL
                    WHEN instrument_provider_map.active_to IS NULL THEN excluded.active_to
                    ELSE instrument_provider_map.active_to
                END,
                last_seen=excluded.last_seen,
                attrs=excluded.attrs;
            """,
            provider_rows,
        )
        self.conn.commit()
        return len(records)

    def upsert_provider_mapping(
        self,
        *,
        instrument_id: str,
        provider: str,
        provider_code: str,
        active_from: datetime | None = None,
        attrs: dict[str, Any] | None = None,
        last_seen: datetime | None = None,
    ) -> None:
        """
        Insert or refresh a provider mapping entry without re-upserting the instrument record.
        """
        now = last_seen or datetime.now(timezone.utc)
        active_from_ts = _dt_to_str(active_from or now)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO instrument_provider_map (
                instrument_id, provider, provider_code,
                active_from, active_to, last_seen, attrs
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_code) DO UPDATE SET
                instrument_id=excluded.instrument_id,
                active_from=MIN(
                    COALESCE(instrument_provider_map.active_from, excluded.active_from),
                    excluded.active_from
                ),
                active_to=CASE
                    WHEN instrument_provider_map.active_to IS NOT NULL AND excluded.active_to IS NULL THEN NULL
                    WHEN instrument_provider_map.active_to IS NULL THEN excluded.active_to
                    ELSE instrument_provider_map.active_to
                END,
                last_seen=excluded.last_seen,
                attrs=excluded.attrs;
            """,
            (
                instrument_id,
                provider,
                provider_code,
                active_from_ts,
                None,
                _dt_to_str(now),
                json.dumps(attrs or {}),
            ),
        )
        self.conn.commit()

    def remove_provider_mapping(self, *, provider: str, provider_code: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "DELETE FROM instrument_provider_map WHERE provider = ? AND provider_code = ?",
            (provider, provider_code),
        )
        self.conn.commit()

    def upsert_instrument_entities(
        self,
        records: Iterable[tuple[str, str, str, datetime, datetime | None]],
    ) -> int:
        """Insert or update instrument/entity relations.

        records: iterable of (instrument_id, entity_id, relation_type, active_from, active_to)
        """
        rows = []
        for instrument_id, entity_id, relation_type, active_from, active_to in records:
            rows.append(
                (
                    instrument_id,
                    entity_id,
                    relation_type,
                    _dt_to_str(active_from),
                    _maybe_dt_to_str(active_to),
                )
            )

        if not rows:
            return 0

        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO instrument_entity (
                instrument_id, entity_id, relation_type, active_from, active_to
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(instrument_id, entity_id, relation_type, active_from) DO UPDATE SET
                active_to=excluded.active_to
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
            UPDATE instrument_provider_map
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
        SELECT ipm.instrument_id, i.instrument_type, ipm.provider, ipm.provider_code, i.mic_primary AS mic, i.currency,
               ipm.active_from, ipm.active_to, ipm.attrs
        FROM instrument_provider_map ipm
        LEFT JOIN instrument i ON ipm.instrument_id = i.instrument_id
        {where}
        ORDER BY ipm.provider, ipm.provider_code COLLATE NOCASE
        LIMIT ? OFFSET ?;
        """
        params.extend([limit, offset])
        cur = self.conn.execute(sql, params)
        return [self._row_to_record(row) for row in cur.fetchall()]

    def get_instrument(self, provider: str, provider_code: str) -> InstrumentRecord | None:
        cur = self.conn.execute(
            """
            SELECT ipm.instrument_id, i.instrument_type, ipm.provider, ipm.provider_code, i.mic_primary AS mic, i.currency,
                   ipm.active_from, ipm.active_to, ipm.attrs
            FROM instrument_provider_map ipm
            LEFT JOIN instrument i ON ipm.instrument_id = i.instrument_id
            WHERE ipm.provider = ? AND ipm.provider_code = ?
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
            active_from=_str_to_dt(row["active_from"]) if row["active_from"] else None,
            active_to=_str_to_dt(row["active_to"]) if row["active_to"] else None,
            attrs=parsed_attrs,
        )

    def close(self) -> None:
        if getattr(self, "_owns_conn", False):
            self.conn.close()

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
