from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from profit.catalog.types import EntityIdentifierRecord, EntityRecord


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
    SQLite-backed store for providers, entities, and identifiers.
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

    def resolve_entity_id(self, identifier: str) -> Optional[str]:
        """
        Return the canonical entity_id for the provided identifier string.
        """
        cur = self.conn.cursor()
        cur.execute("SELECT entity_id FROM entity WHERE entity_id = ?", (identifier,))
        row = cur.fetchone()
        if row:
            return row["entity_id"]
        cur.execute(
            "SELECT entity_id FROM entity_identifier WHERE value = ? COLLATE NOCASE",
            (identifier,),
        )
        row = cur.fetchone()
        if row:
            return row["entity_id"]
        cur.execute(
            "SELECT entity_id FROM entity_identifier WHERE value = ? COLLATE NOCASE",
            (identifier.upper(),),
        )
        row = cur.fetchone()
        if row:
            return row["entity_id"]
        return None

    def resolve_identifier(self, entity_id: str, scheme: str, *, provider_id: str | None = None) -> str | None:
        """
        Return the most recent identifier value for the given entity_id/scheme pair.
        """
        query = "SELECT value FROM entity_identifier WHERE entity_id = ? AND scheme = ?"
        params: list[str] = [entity_id, scheme]
        if provider_id:
            query += " AND provider_id = ?"
            params.append(provider_id)
        query += " ORDER BY last_seen DESC LIMIT 1"
        cur = self.conn.execute(query, params)
        row = cur.fetchone()
        return row["value"] if row else None
