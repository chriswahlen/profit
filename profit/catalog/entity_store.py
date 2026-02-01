from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from profit.catalog.types import EntityIdentifierRecord, EntityRecord, FinanceFactRecord


def _dt_to_str(dt: datetime) -> str:
    """Normalize datetimes to UTC ISO strings."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def _date_to_str(dt: datetime) -> str:
    return dt.date().isoformat()

def _maybe_dt_to_str(dt: datetime | None) -> str | None:
    return _dt_to_str(dt) if dt is not None else None

def _maybe_date_to_str(dt: datetime | None) -> str | None:
    return _date_to_str(dt) if dt is not None else None


def _str_to_dt(val: str) -> datetime:
    return datetime.fromisoformat(val)

def _bool_to_int(val: bool | None) -> int | None:
    if val is None:
        return None
    return 1 if val else 0


_ENTITY_ID_RE = re.compile(r"^[a-z0-9:_/-]+$")


def validate_entity_id(entity_id: str) -> None:
    if not _ENTITY_ID_RE.match(entity_id):
        raise ValueError(
            "entity_id must be lowercase and may contain a-z, 0-9, underscore, colon, dash, or slash"
        )


class EntityStore:
    """
    SQLite-backed store for provider, entity, identifier, and company finance facts.
    Keeps a single authoritative row per finance fact key; newer asof overwrites older,
    older-asof conflicting writes are rejected to keep ingestion deterministic.
    """

    def __init__(self, db_path: Path, *, readonly: bool = False, conn: sqlite3.Connection | None = None) -> None:
        self.db_path = Path(db_path)
        self._owns_conn = conn is None
        if conn is None:
            if not readonly:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            uri = f"file:{self.db_path.as_posix()}" + ("?mode=ro" if readonly else "")
            conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        if not readonly:
            self.ensure_schema()

    def ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS provider (
                provider_id TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                description TEXT,
                homepage    TEXT,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS entity (
                entity_id    TEXT PRIMARY KEY,
                entity_type  TEXT NOT NULL,
                name         TEXT NOT NULL,
                country_iso2 TEXT,
                status       TEXT NOT NULL DEFAULT 'active',
                attrs        TEXT
            );

            CREATE TABLE IF NOT EXISTS entity_identifier (
                entity_id   TEXT NOT NULL REFERENCES entity(entity_id),
                scheme      TEXT NOT NULL,
                value       TEXT NOT NULL,
                provider_id TEXT REFERENCES provider(provider_id),
                active_from TEXT,
                active_to   TEXT,
                last_seen   TEXT NOT NULL,
                PRIMARY KEY (entity_id, scheme, value)
            );
            CREATE INDEX IF NOT EXISTS idx_identifier_scheme_value ON entity_identifier(scheme, value);
            CREATE INDEX IF NOT EXISTS idx_identifier_entity ON entity_identifier(entity_id);

            CREATE TABLE IF NOT EXISTS company_finance_fact (
                entity_id          TEXT NOT NULL REFERENCES entity(entity_id),
                provider_id        TEXT NOT NULL REFERENCES provider(provider_id),
                provider_entity_id TEXT NOT NULL,
                record_id          TEXT NOT NULL,
                report_id          TEXT NOT NULL,
                report_key         TEXT NOT NULL,
                period_start       TEXT,
                period_end         TEXT NOT NULL,
                decimals           INTEGER,
                dimensions_sig     TEXT,
                is_consolidated    INTEGER,
                amendment_flag     INTEGER,
                filed_at           TEXT,
                units              TEXT NOT NULL,
                value              REAL,
                asof               TEXT NOT NULL,
                attrs              TEXT,
                PRIMARY KEY (provider_id, provider_entity_id, record_id, report_id, report_key, period_end)
            );
            CREATE INDEX IF NOT EXISTS idx_finance_entity_period ON company_finance_fact(entity_id, period_end);
            CREATE INDEX IF NOT EXISTS idx_finance_provider_entity ON company_finance_fact(provider_id, provider_entity_id);
            CREATE INDEX IF NOT EXISTS idx_finance_report_lookup ON company_finance_fact(provider_id, provider_entity_id, report_id, report_key, period_end);
        """
        )
        self.conn.commit()

    def close(self) -> None:
        if getattr(self, "_owns_conn", False):
            self.conn.close()

    # --- Provider -----------------------------------------------------
    def upsert_providers(self, providers: Iterable[tuple[str, str | None, str | None]]) -> int:
        """
        providers: iterable of (provider_id, name, description)
        """
        rows = []
        for provider_id, name, description in providers:
            rows.append((provider_id, name or provider_id, description))
        if not rows:
            return 0
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO provider (provider_id, name, description)
            VALUES (?, ?, ?)
            ON CONFLICT(provider_id) DO UPDATE SET
                name=excluded.name,
                description=COALESCE(excluded.description, provider.description)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    # --- Entities -----------------------------------------------------
    def upsert_entities(self, records: Iterable[EntityRecord]) -> int:
        rows = []
        for r in records:
            validate_entity_id(r.entity_id)
            rows.append(
                (
                    r.entity_id,
                    r.entity_type,
                    r.name,
                    r.country_iso2,
                    r.status,
                    json.dumps(r.attrs or {}, sort_keys=True),
                )
            )
        if not rows:
            return 0
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO entity (entity_id, entity_type, name, country_iso2, status, attrs)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_id) DO UPDATE SET
                entity_type=excluded.entity_type,
                name=excluded.name,
                country_iso2=excluded.country_iso2,
                status=excluded.status,
                attrs=excluded.attrs
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    # --- Identifiers --------------------------------------------------
    def upsert_identifiers(self, records: Iterable[EntityIdentifierRecord], *, default_last_seen: Optional[datetime] = None) -> int:
        now = default_last_seen or datetime.now(timezone.utc)
        rows = []
        for r in records:
            validate_entity_id(r.entity_id)
            rows.append(
                (
                    r.entity_id,
                    r.scheme,
                    r.value,
                    r.provider_id,
                    _dt_to_str(r.active_from) if r.active_from else None,
                    _dt_to_str(r.active_to) if r.active_to else None,
                    _dt_to_str(r.last_seen or now),
                )
            )
        if not rows:
            return 0
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO entity_identifier (
                entity_id, scheme, value, provider_id, active_from, active_to, last_seen
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_id, scheme, value) DO UPDATE SET
                provider_id=excluded.provider_id,
                active_from=COALESCE(entity_identifier.active_from, excluded.active_from),
                active_to=excluded.active_to,
                last_seen=MAX(entity_identifier.last_seen, excluded.last_seen)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def find_entity_by_identifier(self, *, scheme: str, value: str) -> Optional[str]:
        cur = self.conn.execute(
            """
            SELECT entity_id FROM entity_identifier
            WHERE scheme = ? AND value = ?
            ORDER BY last_seen DESC
            LIMIT 1
            """,
            (scheme, value),
        )
        row = cur.fetchone()
        return row["entity_id"] if row else None

    # --- Finance facts ------------------------------------------------
    def upsert_finance_facts(self, records: Iterable[FinanceFactRecord]) -> int:
        cur = self.conn.cursor()
        written = 0
        for r in records:
            validate_entity_id(r.entity_id)
            key = (
                r.provider_id,
                r.provider_entity_id,
                r.record_id,
                r.report_id,
                r.report_key,
                _date_to_str(r.period_end),
            )
            cur.execute(
                """
                SELECT value, units, asof, attrs, decimals, period_start, dimensions_sig, is_consolidated, amendment_flag, filed_at
                FROM company_finance_fact
                WHERE provider_id = ? AND provider_entity_id = ? AND record_id = ? AND report_id = ?
                      AND report_key = ? AND period_end = ?
                """,
                key,
            )
            existing = cur.fetchone()
            new_asof = _dt_to_str(r.asof)
            attrs_json = json.dumps(r.attrs or {}, sort_keys=True)
            if existing:
                existing_asof = _str_to_dt(existing["asof"])
                if (
                    existing["value"] == r.value
                    and existing["units"] == r.units
                    and existing["attrs"] == attrs_json
                    and existing["decimals"] == r.decimals
                    and existing["period_start"] == _maybe_date_to_str(r.period_start)
                    and existing["dimensions_sig"] == r.dimensions_sig
                    and existing["is_consolidated"] == _bool_to_int(r.is_consolidated)
                    and existing["amendment_flag"] == _bool_to_int(r.amendment_flag)
                    and existing["filed_at"] == _maybe_dt_to_str(r.filed_at)
                ):
                    # identical payload; keep earliest asof
                    if r.asof > existing_asof:
                        cur.execute(
                            """
                            UPDATE company_finance_fact
                            SET asof = ?
                            WHERE provider_id = ? AND provider_entity_id = ? AND record_id = ? AND report_id = ?
                                  AND report_key = ? AND period_end = ?
                            """,
                            (new_asof, *key),
                        )
                    continue
                if r.asof < existing_asof:
                    raise ValueError(
                        "Refusing to overwrite finance fact with older asof for key "
                        f"{key[:-1]} period_end={key[-1]}"
                    )
                # overwrite with newer data
                cur.execute(
                    """
                    UPDATE company_finance_fact
                    SET entity_id=?, units=?, value=?, asof=?, attrs=?, decimals=?, period_start=?, dimensions_sig=?,
                        is_consolidated=?, amendment_flag=?, filed_at=?
                    WHERE provider_id = ? AND provider_entity_id = ? AND record_id = ? AND report_id = ?
                          AND report_key = ? AND period_end = ?
                    """,
                    (
                        r.entity_id,
                        r.units,
                        r.value,
                        new_asof,
                        attrs_json,
                        r.decimals,
                        _maybe_date_to_str(r.period_start),
                        r.dimensions_sig,
                        _bool_to_int(r.is_consolidated),
                        _bool_to_int(r.amendment_flag),
                        _maybe_dt_to_str(r.filed_at),
                        *key,
                    ),
                )
                written += cur.rowcount
            else:
                cur.execute(
                    """
                    INSERT INTO company_finance_fact (
                        entity_id, provider_id, provider_entity_id, record_id, report_id,
                        report_key, period_start, period_end, decimals, dimensions_sig,
                        is_consolidated, amendment_flag, filed_at, units, value, asof, attrs
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        r.entity_id,
                        r.provider_id,
                        r.provider_entity_id,
                        r.record_id,
                        r.report_id,
                        r.report_key,
                        _maybe_date_to_str(r.period_start),
                        _date_to_str(r.period_end),
                        r.decimals,
                        r.dimensions_sig,
                        _bool_to_int(r.is_consolidated),
                        _bool_to_int(r.amendment_flag),
                        _maybe_dt_to_str(r.filed_at),
                        r.units,
                        r.value,
                        new_asof,
                        attrs_json,
                    ),
                )
                written += 1
        self.conn.commit()
        return written
