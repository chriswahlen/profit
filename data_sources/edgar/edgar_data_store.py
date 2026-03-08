from __future__ import annotations

import gzip
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence
from xml.etree import ElementTree as ET

from config import Config
from data_sources.edgar.common import normalize_cik
from data_sources.edgar.xml_parser import parse_xbrl
from data_sources.edgar.xbrl_extract import (
    normalize_unit,
    parse_context_dimensions,
    parse_contexts,
    parse_units,
)
from data_sources.sqlite_data_store import SqliteDataStore

logger = logging.getLogger(__name__)


def _iso(ts: datetime | None = None) -> str:
    ts = ts or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _date_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.date().isoformat()


class EdgarDataStore(SqliteDataStore):
    """SQLite store for EDGAR submissions, accession documents, and parsed XBRL facts."""

    @dataclass(frozen=True)
    class XbrlContextRow:
        context_id: int
        accession: str
        context_ref: str
        entity_scheme: str | None
        entity_scheme_id: int | None
        entity_id: str | None
        period_type: str
        start_date: str | None
        end_date: str | None
        instant_date: str | None

    @dataclass(frozen=True)
    class ContextDimensionRow:
        context_id: int
        axis_id: int
        member_id: int | None
        typed_value: str | None

    @dataclass(frozen=True)
    class XbrlUnitRow:
        unit_id: int
        accession: str
        unit_ref: str
        measure: str | None

    @dataclass(frozen=True)
    class XbrlConceptRow:
        concept_id: int
        qname: str
        label: str | None
        data_type: str | None

    @dataclass(frozen=True)
    class XbrlFactRow:
        fact_id: int
        accession: str
        concept_id: int
        context_id: int
        unit_id: int | None
        decimals: int | None
        precision: int | None
        sign: int | None
        value_numeric: float | None
        value_text: str | None
        value_raw: str
        is_nil: int
        footnote_html: str | None

    def __init__(self, config: Config):
        super().__init__(db_name="edgar.sqlite", config=config, summary="SEC EDGAR submissions + XBRL facts")
        self._logger = logging.getLogger(__name__)
        self._ensure_schema()

    def describe_brief(self) -> str:
        return f"- edgar (sqlite://{self.db_path.name}): {self.summary}"

    def _ensure_conn(self) -> sqlite3.Connection:
        conn = super()._ensure_conn()
        # The `edgar.sqlite` schema is fixed to match an existing legacy design
        # that declares foreign keys referencing non-unique parent columns. With
        # FK enforcement enabled, SQLite can raise "foreign key mismatch" on
        # inserts. Keep enforcement disabled for this datastore.
        conn.execute("PRAGMA foreign_keys = OFF;")
        return conn

    # --- schema -------------------------------------------------------------
    def _ensure_schema(self) -> None:
        conn = self._ensure_conn()
        conn.executescript(
            """
            PRAGMA foreign_keys = OFF;

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
                source_url TEXT,
                PRIMARY KEY (accession, file_name),
                FOREIGN KEY(accession) REFERENCES edgar_accession(accession)
            );

            CREATE TABLE IF NOT EXISTS edgar_fact_extract (
                cik TEXT NOT NULL,
                accession TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                fact_count INTEGER,
                note TEXT,
                PRIMARY KEY (cik, accession)
            );

            CREATE TABLE IF NOT EXISTS xbrl_context (
                context_id INTEGER PRIMARY KEY,
                accession TEXT NOT NULL,
                context_ref TEXT NOT NULL,
                entity_scheme TEXT,
                entity_id TEXT,
                period_type TEXT NOT NULL CHECK (period_type IN ('instant','duration')),
                start_date TEXT,
                end_date TEXT,
                instant_date TEXT,
                entity_scheme_id INTEGER,
                FOREIGN KEY(accession) REFERENCES edgar_accession(accession),
                UNIQUE (accession, context_ref)
            );

            CREATE INDEX IF NOT EXISTS idx_context_accession ON xbrl_context(accession);
            CREATE INDEX IF NOT EXISTS idx_context_period ON xbrl_context(accession, period_type, start_date, end_date, instant_date);

            CREATE TABLE IF NOT EXISTS dimension_axis (
                axis_id INTEGER PRIMARY KEY,
                qname TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS dimension_member (
                member_id INTEGER PRIMARY KEY,
                qname TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS context_dimension (
                context_id INTEGER NOT NULL,
                axis_id INTEGER NOT NULL,
                member_id INTEGER,
                typed_value TEXT,
                PRIMARY KEY (context_id, axis_id),
                FOREIGN KEY(context_id) REFERENCES xbrl_context(context_id),
                FOREIGN KEY(axis_id) REFERENCES dimension_axis(axis_id),
                FOREIGN KEY(member_id) REFERENCES dimension_member(member_id)
            );

            CREATE INDEX IF NOT EXISTS idx_ctxdim_axis_member ON context_dimension(axis_id, member_id);

            CREATE TABLE IF NOT EXISTS xbrl_concept (
                concept_id INTEGER PRIMARY KEY,
                qname TEXT NOT NULL UNIQUE,
                label TEXT,
                data_type TEXT
            );

            CREATE TABLE IF NOT EXISTS xbrl_unit (
                unit_id INTEGER PRIMARY KEY,
                accession TEXT NOT NULL,
                unit_ref TEXT NOT NULL,
                measure TEXT,
                FOREIGN KEY(accession) REFERENCES edgar_accession(accession),
                UNIQUE(accession, unit_ref)
            );

            CREATE INDEX IF NOT EXISTS idx_unit_accession ON xbrl_unit(accession);

            CREATE TABLE IF NOT EXISTS xbrl_fact (
                fact_id INTEGER PRIMARY KEY,
                accession TEXT NOT NULL,
                concept_id INTEGER NOT NULL,
                context_id INTEGER NOT NULL,
                unit_id INTEGER,
                decimals INTEGER,
                precision INTEGER,
                sign INTEGER,
                value_numeric REAL,
                value_text TEXT,
                value_raw TEXT NOT NULL,
                is_nil INTEGER NOT NULL DEFAULT 0,
                footnote_html TEXT,
                FOREIGN KEY(accession) REFERENCES edgar_accession(accession),
                FOREIGN KEY(concept_id) REFERENCES xbrl_concept(concept_id),
                FOREIGN KEY(context_id) REFERENCES xbrl_context(context_id),
                FOREIGN KEY(unit_id) REFERENCES xbrl_unit(unit_id)
            );

            CREATE INDEX IF NOT EXISTS idx_fact_lookup ON xbrl_fact(accession, concept_id, context_id);
            CREATE INDEX IF NOT EXISTS idx_fact_concept ON xbrl_fact(concept_id);
            CREATE INDEX IF NOT EXISTS idx_fact_context ON xbrl_fact(context_id);

            CREATE TABLE IF NOT EXISTS entity_scheme (
                scheme_id INTEGER PRIMARY KEY,
                scheme TEXT NOT NULL UNIQUE
            );
            """
        )
        conn.commit()
        self._ensure_accession_file_url_column(conn)
        self._ensure_xbrl_context_columns(conn)
        self._create_fact_view(conn)

    @staticmethod
    def _ensure_accession_file_url_column(conn: sqlite3.Connection) -> None:
        cur = conn.execute("PRAGMA table_info(edgar_accession_file)")
        columns = {row[1] for row in cur.fetchall()}
        if "source_url" not in columns:
            conn.execute("ALTER TABLE edgar_accession_file ADD COLUMN source_url TEXT")
            conn.commit()

    @staticmethod
    def _ensure_xbrl_context_columns(conn: sqlite3.Connection) -> None:
        cur = conn.execute("PRAGMA table_info(xbrl_context)")
        columns = {row[1] for row in cur.fetchall()}
        # `edgar.sqlite` schema includes these. If a DB predates them, add in-place.
        if "entity_scheme" not in columns:
            conn.execute("ALTER TABLE xbrl_context ADD COLUMN entity_scheme TEXT")
        if "entity_scheme_id" not in columns:
            conn.execute("ALTER TABLE xbrl_context ADD COLUMN entity_scheme_id INTEGER")

    def _create_fact_view(self, conn: sqlite3.Connection) -> None:
        conn.execute("DROP VIEW IF EXISTS xbrl_fact_view;")
        conn.execute(
            """
            CREATE VIEW xbrl_fact_view AS
            SELECT
                f.fact_id,
                f.accession,
                f.context_id,
                c.qname AS concept_qname,
                c.label AS concept_label,
                c.data_type AS concept_data_type,
                ctx.context_ref,
                ctx.period_type,
                ctx.start_date,
                ctx.end_date,
                ctx.instant_date,
                f.unit_id,
                u.measure AS unit_measure,
                f.decimals,
                f.precision,
                f.sign,
                f.value_numeric,
                f.value_text,
                f.value_raw,
                f.is_nil,
                f.footnote_html
            FROM xbrl_fact f
            JOIN xbrl_concept c ON c.concept_id = f.concept_id
            LEFT JOIN xbrl_context ctx ON ctx.context_id = f.context_id
            LEFT JOIN xbrl_unit u ON u.unit_id = f.unit_id;
            """
        )
        conn.commit()

    # --- submissions --------------------------------------------------------
    def record_submissions(
        self,
        cik: str,
        entity_name: str | None,
        payload: Mapping[str, object],
        *,
        fetched_at: datetime | None = None,
    ) -> None:
        ts = _iso(fetched_at)
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO edgar_submissions (cik, entity_name, fetched_at, payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(cik) DO UPDATE SET
                entity_name=excluded.entity_name,
                fetched_at=excluded.fetched_at,
                payload=excluded.payload;
            """,
            (normalize_cik(cik), entity_name, ts, json.dumps(payload, ensure_ascii=True)),
        )
        conn.commit()

    def upsert_submissions_rows(self, rows: Iterable[tuple[str, str | None, datetime, str]]) -> int:
        data = list(rows)
        if not data:
            return 0
        conn = self._ensure_conn()
        conn.executemany(
            """
            INSERT INTO edgar_submissions (cik, entity_name, fetched_at, payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(cik) DO UPDATE SET
                entity_name=excluded.entity_name,
                fetched_at=excluded.fetched_at,
                payload=excluded.payload;
            """,
            [(normalize_cik(cik), name, _iso(ts), payload) for cik, name, ts, payload in data],
        )
        conn.commit()
        return len(data)

    # --- accessions + files -------------------------------------------------
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
        cik_norm = normalize_cik(cik)
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO edgar_accession (cik, accession, base_url, file_count, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cik, accession) DO UPDATE SET
                base_url=excluded.base_url,
                file_count=excluded.file_count,
                fetched_at=excluded.fetched_at;
            """,
            (cik_norm, accession, base_url, len(files), ts),
        )

        if files:
            existing = {
                row[0]
                for row in conn.execute(
                    "SELECT file_name FROM edgar_accession_file WHERE accession = ?",
                    (accession,),
                ).fetchall()
            }
            rows = []
            for name in files:
                if not name or name in existing:
                    continue
                source_url = f"{base_url}{name}" if base_url else None
                rows.append((accession, name, ts, None, source_url))
            if rows:
                conn.executemany(
                    """
                    INSERT INTO edgar_accession_file (accession, file_name, fetched_at, compressed_payload, source_url)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(accession, file_name) DO UPDATE SET source_url=excluded.source_url;
                    """,
                    rows,
                )
        conn.commit()

    @staticmethod
    def _compress_payload(payload: bytes) -> bytes:
        return gzip.compress(payload)

    @staticmethod
    def _decompress_payload(payload: bytes) -> bytes:
        return gzip.decompress(payload)

    def has_file(self, accession: str, file_name: str) -> bool:
        conn = self._ensure_conn()
        cur = conn.execute(
            "SELECT 1 FROM edgar_accession_file WHERE accession = ? AND file_name = ? AND compressed_payload IS NOT NULL",
            (accession, file_name),
        )
        return cur.fetchone() is not None

    def store_file(
        self,
        accession: str,
        file_name: str,
        payload: bytes,
        *,
        fetched_at: datetime | None = None,
        source_url: str | None = None,
    ) -> None:
        ts = _iso(fetched_at)
        compressed = self._compress_payload(payload)
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO edgar_accession_file (accession, file_name, fetched_at, compressed_payload, source_url)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(accession, file_name) DO UPDATE SET
                fetched_at=excluded.fetched_at,
                compressed_payload=excluded.compressed_payload,
                source_url=COALESCE(excluded.source_url, edgar_accession_file.source_url);
            """,
            (accession, file_name, ts, compressed, source_url),
        )
        conn.commit()

    def get_file(self, accession: str, file_name: str) -> bytes | None:
        conn = self._ensure_conn()
        cur = conn.execute(
            "SELECT compressed_payload FROM edgar_accession_file WHERE accession = ? AND file_name = ?",
            (accession, file_name),
        )
        row = cur.fetchone()
        if row is None or row[0] is None:
            return None
        return self._decompress_payload(bytes(row[0]))

    def get_accession_files(self, accession: str) -> list[str]:
        conn = self._ensure_conn()
        cur = conn.execute(
            "SELECT file_name FROM edgar_accession_file WHERE accession = ? ORDER BY file_name",
            (accession,),
        )
        return [row[0] for row in cur.fetchall()]

    def get_accession_files_info(self, accession: str) -> list[tuple[str, str | None]]:
        conn = self._ensure_conn()
        cur = conn.execute(
            "SELECT file_name, source_url FROM edgar_accession_file WHERE accession = ? ORDER BY file_name",
            (accession,),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]

    def known_accessions(self, cik: str) -> set[str]:
        conn = self._ensure_conn()
        cur = conn.execute("SELECT accession FROM edgar_accession WHERE cik = ?", (normalize_cik(cik),))
        return {row[0] for row in cur.fetchall()}

    def has_accession(self, accession: str, *, cik: str | None = None) -> bool:
        conn = self._ensure_conn()
        if cik is None:
            cur = conn.execute("SELECT 1 FROM edgar_accession WHERE accession = ? LIMIT 1", (accession,))
        else:
            cur = conn.execute(
                "SELECT 1 FROM edgar_accession WHERE cik = ? AND accession = ? LIMIT 1",
                (normalize_cik(cik), accession),
            )
        return cur.fetchone() is not None

    def get_accession_base_url(self, accession: str) -> str | None:
        conn = self._ensure_conn()
        row = conn.execute("SELECT base_url FROM edgar_accession WHERE accession = ? LIMIT 1", (accession,)).fetchone()
        return row[0] if row else None

    # --- XBRL ingestion -----------------------------------------------------
    def has_processed_xbrl_facts(self, cik: str, accession: str) -> bool:
        conn = self._ensure_conn()
        row = conn.execute(
            "SELECT 1 FROM edgar_fact_extract WHERE cik = ? AND accession = ? LIMIT 1",
            (normalize_cik(cik), accession),
        ).fetchone()
        return row is not None

    def mark_xbrl_facts_processed(self, cik: str, accession: str, fact_count: int, note: str | None) -> None:
        ts = _iso()
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO edgar_fact_extract (cik, accession, processed_at, fact_count, note)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cik, accession) DO UPDATE SET
                processed_at=excluded.processed_at,
                fact_count=excluded.fact_count,
                note=excluded.note;
            """,
            (normalize_cik(cik), accession, ts, fact_count, note),
        )
        conn.commit()

    def clear_xbrl_fact_marker(self, cik: str, accession: str) -> None:
        conn = self._ensure_conn()
        conn.execute("DELETE FROM edgar_fact_extract WHERE cik = ? AND accession = ?", (normalize_cik(cik), accession))
        conn.commit()

    def reset_xbrl_accession(self, accession: str) -> None:
        conn = self._ensure_conn()
        context_rows = conn.execute("SELECT context_id FROM xbrl_context WHERE accession = ?", (accession,)).fetchall()
        if context_rows:
            placeholders = ",".join("?" for _ in context_rows)
            ids = [row[0] for row in context_rows]
            conn.execute(f"DELETE FROM context_dimension WHERE context_id IN ({placeholders})", ids)
        conn.execute("DELETE FROM xbrl_fact WHERE accession = ?", (accession,))
        conn.execute("DELETE FROM xbrl_context WHERE accession = ?", (accession,))
        conn.execute("DELETE FROM xbrl_unit WHERE accession = ?", (accession,))
        conn.commit()

    def get_or_create_entity_scheme(self, scheme: str) -> int:
        conn = self._ensure_conn()
        row = conn.execute("SELECT scheme_id FROM entity_scheme WHERE scheme = ?", (scheme,)).fetchone()
        if row:
            return row[0]
        cur = conn.execute("INSERT INTO entity_scheme (scheme) VALUES (?)", (scheme,))
        conn.commit()
        return int(cur.lastrowid)

    def upsert_xbrl_context(
        self,
        accession: str,
        context_ref: str,
        *,
        entity_scheme: str | None,
        entity_scheme_id: int | None,
        entity_id: str | None,
        period_type: str,
        start_date: str | None,
        end_date: str | None,
        instant_date: str | None,
    ) -> int:
        conn = self._ensure_conn()
        cur = conn.execute(
            """
            INSERT INTO xbrl_context (
                accession,
                context_ref,
                entity_scheme,
                entity_id,
                period_type,
                start_date,
                end_date,
                instant_date,
                entity_scheme_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(accession, context_ref) DO UPDATE SET
                entity_scheme=excluded.entity_scheme,
                entity_id=excluded.entity_id,
                period_type=excluded.period_type,
                start_date=excluded.start_date,
                end_date=excluded.end_date,
                instant_date=excluded.instant_date,
                entity_scheme_id=excluded.entity_scheme_id;
            """,
            (
                accession,
                context_ref,
                entity_scheme,
                entity_id,
                period_type,
                start_date,
                end_date,
                instant_date,
                entity_scheme_id,
            ),
        )
        context_id = int(cur.lastrowid or 0)
        if not context_id:
            row = conn.execute(
                "SELECT context_id FROM xbrl_context WHERE accession = ? AND context_ref = ?",
                (accession, context_ref),
            ).fetchone()
            context_id = int(row[0]) if row else 0
        conn.commit()
        return context_id

    def get_or_create_dimension_axis(self, qname: str) -> int:
        conn = self._ensure_conn()
        row = conn.execute("SELECT axis_id FROM dimension_axis WHERE qname = ?", (qname,)).fetchone()
        if row:
            return row[0]
        cur = conn.execute("INSERT INTO dimension_axis (qname) VALUES (?)", (qname,))
        conn.commit()
        return int(cur.lastrowid)

    def get_or_create_dimension_member(self, qname: str) -> int:
        conn = self._ensure_conn()
        row = conn.execute("SELECT member_id FROM dimension_member WHERE qname = ?", (qname,)).fetchone()
        if row:
            return row[0]
        cur = conn.execute("INSERT INTO dimension_member (qname) VALUES (?)", (qname,))
        conn.commit()
        return int(cur.lastrowid)

    def upsert_context_dimension(
        self,
        context_id: int,
        axis_id: int,
        *,
        member_id: int | None = None,
        typed_value: str | None = None,
    ) -> None:
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO context_dimension (context_id, axis_id, member_id, typed_value)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(context_id, axis_id) DO UPDATE SET
                member_id=excluded.member_id,
                typed_value=excluded.typed_value;
            """,
            (context_id, axis_id, member_id, typed_value),
        )
        conn.commit()

    def get_or_create_xbrl_concept(self, qname: str, *, label: str | None = None, data_type: str | None = None) -> int:
        conn = self._ensure_conn()
        conn.execute(
            "INSERT OR IGNORE INTO xbrl_concept (qname, label, data_type) VALUES (?, ?, ?)",
            (qname, label, data_type),
        )
        if label is not None or data_type is not None:
            updates: list[str] = []
            params: list[object] = []
            if label is not None:
                updates.append("label = ?")
                params.append(label)
            if data_type is not None:
                updates.append("data_type = ?")
                params.append(data_type)
            params.append(qname)
            conn.execute(f"UPDATE xbrl_concept SET {', '.join(updates)} WHERE qname = ?", params)
        row = conn.execute("SELECT concept_id FROM xbrl_concept WHERE qname = ?", (qname,)).fetchone()
        conn.commit()
        return int(row[0])

    def get_or_create_xbrl_unit(self, accession: str, unit_ref: str, *, measure: str | None = None) -> int:
        conn = self._ensure_conn()
        conn.execute(
            "INSERT OR IGNORE INTO xbrl_unit (accession, unit_ref, measure) VALUES (?, ?, ?)",
            (accession, unit_ref, measure),
        )
        if measure is not None:
            conn.execute(
                """
                UPDATE xbrl_unit
                SET measure = ?
                WHERE accession = ? AND unit_ref = ? AND (measure IS NULL OR measure != ?);
                """,
                (measure, accession, unit_ref, measure),
            )
        row = conn.execute(
            "SELECT unit_id FROM xbrl_unit WHERE accession = ? AND unit_ref = ?",
            (accession, unit_ref),
        ).fetchone()
        conn.commit()
        return int(row[0])

    def insert_xbrl_fact(
        self,
        accession: str,
        concept_id: int,
        context_id: int,
        *,
        unit_id: int | None = None,
        decimals: int | None = None,
        precision: int | None = None,
        sign: int | None = None,
        value_numeric: float | None = None,
        value_text: str | None = None,
        value_raw: str,
        is_nil: int = 0,
        footnote_html: str | None = None,
    ) -> int:
        conn = self._ensure_conn()
        cur = conn.execute(
            """
            INSERT INTO xbrl_fact (
                accession,
                concept_id,
                context_id,
                unit_id,
                decimals,
                precision,
                sign,
                value_numeric,
                value_text,
                value_raw,
                is_nil,
                footnote_html
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                accession,
                concept_id,
                context_id,
                unit_id,
                decimals,
                precision,
                sign,
                value_numeric,
                value_text,
                value_raw,
                is_nil,
                footnote_html,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    def ingest_xbrl_facts(self, cik: str, accession: str, xml_bytes: bytes) -> int:
        cik_norm = normalize_cik(cik)
        if self.has_processed_xbrl_facts(cik_norm, accession):
            logger.debug("Skipping already processed XBRL facts cik=%s accession=%s", cik_norm, accession)
            return 0

        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            raise ValueError(f"invalid XML for accession={accession}: {exc}") from exc

        contexts = parse_contexts(root)
        if not contexts:
            raise ValueError(f"no valid contexts found in accession={accession}")
        dim_map = parse_context_dimensions(root)
        units = parse_units(root)
        parsed = parse_xbrl(xml_bytes, root=root)

        context_ids: dict[str, int] = {}
        conn = self._ensure_conn()
        for ctx in contexts.values():
            if ctx.period_type not in {"instant", "duration"}:
                continue
            start_date = _date_iso(ctx.period_start) if ctx.period_type == "duration" else None
            end_date = _date_iso(ctx.period_end) if ctx.period_type == "duration" else None
            instant_date = _date_iso(ctx.period_end) if ctx.period_type == "instant" else None
            scheme_id = self.get_or_create_entity_scheme(ctx.entity_scheme) if ctx.entity_scheme else None
            context_id = self.upsert_xbrl_context(
                accession,
                ctx.id,
                entity_scheme=ctx.entity_scheme,
                entity_scheme_id=scheme_id,
                entity_id=ctx.entity_id,
                period_type=ctx.period_type,
                start_date=start_date,
                end_date=end_date,
                instant_date=instant_date,
            )
            context_ids[ctx.id] = context_id
            conn.execute("DELETE FROM context_dimension WHERE context_id = ?", (context_id,))
            for dimension in dim_map.get(ctx.id) or []:
                axis_id = self.get_or_create_dimension_axis(dimension.axis)
                member_id = self.get_or_create_dimension_member(dimension.member) if dimension.member else None
                self.upsert_context_dimension(context_id, axis_id, member_id=member_id, typed_value=dimension.typed_value)

        unit_map: dict[str, int] = {}
        for unit_ref, parsed_unit in units.items():
            measure = normalize_unit(parsed_unit.measures) or (parsed_unit.measures[0] if parsed_unit.measures else None)
            unit_map[unit_ref] = self.get_or_create_xbrl_unit(accession, unit_ref, measure=measure)

        fact_count = 0
        for fact in parsed.facts:
            context_id = context_ids.get(fact.context_ref or "")
            if context_id is None:
                continue
            concept_id = self.get_or_create_xbrl_concept(fact.name)
            unit_id = unit_map.get(fact.unit_ref or "")
            decimals = int(fact.decimals) if fact.decimals and fact.decimals.strip("-").isdigit() else None
            raw_value = fact.lexical_value or str(fact.value)
            self.insert_xbrl_fact(
                accession,
                concept_id,
                context_id,
                unit_id=unit_id,
                decimals=decimals,
                precision=None,
                sign=None,
                value_numeric=fact.value,
                value_text=None,
                value_raw=raw_value,
                is_nil=fact.is_nil,
                footnote_html=None,
            )
            fact_count += 1

        self.mark_xbrl_facts_processed(cik_norm, accession, fact_count, f"inserted {fact_count} facts")
        return fact_count

    def accessions_for_cik(self, cik: str) -> list[str]:
        """
        Return accession identifiers for the requested normalized CIK, ordered by fetch date descending.
        """
        cik_norm = normalize_cik(cik)
        conn = self._ensure_conn()
        cur = conn.execute(
            "SELECT accession FROM edgar_accession WHERE cik = ? ORDER BY fetched_at DESC;",
            (cik_norm,),
        )
        return [row[0] for row in cur.fetchall()]

    def query_xbrl_facts(
        self,
        *,
        cik: str,
        concept_qnames: Sequence[str],
        accession: str | None = None,
        period_type: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = 100,
    ) -> list[dict[str, Any]]:
        """
        Fetch xbrl facts (contexts + values) for the provided concepts and company.
        """
        if not concept_qnames:
            raise ValueError("at least one concept qname is required")

        cik_norm = normalize_cik(cik)
        accessions = self.accessions_for_cik(cik_norm)
        if not accessions:
            return []

        conn = self._ensure_conn()
        placeholders = ",".join("?" for _ in accessions)
        sql_parts = [
            """
            SELECT
                f.fact_id,
                f.accession,
                c.qname AS concept,
                c.label,
                c.data_type,
                ctx.period_type,
                ctx.start_date,
                ctx.end_date,
                ctx.instant_date,
                f.value_numeric,
                f.value_raw,
                u.measure
            FROM xbrl_fact f
            JOIN xbrl_concept c ON c.concept_id = f.concept_id
            JOIN xbrl_context ctx ON ctx.context_id = f.context_id
            LEFT JOIN xbrl_unit u ON u.unit_id = f.unit_id
            WHERE f.accession IN (%s)
            """
            % placeholders
        ]
        params: list[Any] = list(accessions)

        if accession:
            sql_parts.append("AND f.accession = ?")
            params.append(accession)

        concept_placeholders = ",".join("?" for _ in concept_qnames)
        sql_parts.append(f"AND c.qname IN ({concept_placeholders})")
        params.extend(concept_qnames)

        if period_type:
            sql_parts.append("AND ctx.period_type = ?")
            params.append(period_type)

        date_expr = "COALESCE(ctx.end_date, ctx.instant_date, ctx.start_date)"
        if start_date:
            sql_parts.append(f"AND {date_expr} >= ?")
            params.append(start_date)
        if end_date:
            sql_parts.append(f"AND {date_expr} <= ?")
            params.append(end_date)

        sql_parts.append(f"ORDER BY {date_expr} DESC, f.accession DESC")
        if limit is not None:
            sql_parts.append("LIMIT ?")
            params.append(limit)

        sql = "\n".join(sql_parts)
        cur = conn.cursor()
        cur.execute(sql, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
