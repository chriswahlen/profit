from __future__ import annotations

import gzip
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from profit.sources.edgar.common import normalize_cik
from profit.edgar.xbrl_extract import (
    parse_context_dimensions,
    parse_contexts,
    parse_units,
    parse_xbrl,
    normalize_unit,
)
from xml.etree import ElementTree as ET


def _iso(ts: datetime | None = None) -> str:
    ts = ts or datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()


def _date_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.date().isoformat()


logger = logging.getLogger(__name__)


class EdgarDatabase:
    """
    Lightweight SQLite store for EDGAR submissions + accession metadata.
    """

    @dataclass(frozen=True)
    class EntitySchemeRow:
        scheme_id: int
        scheme: str

    @dataclass(frozen=True)
    class XbrlContextRow:
        context_id: int
        accession: str
        context_ref: str
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
    class DimensionAxisRow:
        axis_id: int
        qname: str

    @dataclass(frozen=True)
    class DimensionMemberRow:
        member_id: int
        qname: str

    @dataclass(frozen=True)
    class XbrlConceptRow:
        concept_id: int
        qname: str
        label: str | None
        data_type: str | None

    @dataclass(frozen=True)
    class XbrlUnitRow:
        unit_id: int
        accession: str
        unit_ref: str
        measure: str | None

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
        self._ensure_accession_file_url_column()
        self.ensure_fact_marker_table()

        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS entity_scheme (
                scheme_id INTEGER PRIMARY KEY,
                scheme TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS xbrl_context (
                context_id INTEGER PRIMARY KEY,
                accession TEXT NOT NULL,
                context_ref TEXT NOT NULL,
                entity_scheme_id INTEGER,
                entity_id TEXT,
                period_type TEXT NOT NULL CHECK (period_type IN ('instant','duration')),
                start_date TEXT,
                end_date TEXT,
                instant_date TEXT,
                FOREIGN KEY(accession) REFERENCES edgar_accession(accession),
                FOREIGN KEY(entity_scheme_id) REFERENCES entity_scheme(scheme_id),
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
            """
        )

    def _ensure_accession_file_url_column(self) -> None:
        cur = self.conn.execute("PRAGMA table_info(edgar_accession_file)")
        columns = {row["name"] for row in cur.fetchall()}
        if "source_url" not in columns:
            self.conn.execute("ALTER TABLE edgar_accession_file ADD COLUMN source_url TEXT")

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
            existing = {
                row["file_name"]
                for row in self.conn.execute(
                    "SELECT file_name FROM edgar_accession_file WHERE accession = ?", (accession,)
                ).fetchall()
            }
            rows = []
            for name in files:
                if not name:
                    continue
                if name in existing:
                    continue
                source_url = f"{base_url}{name}" if base_url else None
                rows.append((accession, name, ts, None, source_url))
            if rows:
                self.conn.executemany(
                    """
                    INSERT INTO edgar_accession_file (accession, file_name, fetched_at, compressed_payload, source_url)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(accession, file_name) DO UPDATE SET source_url=excluded.source_url
                    """,
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
        self.conn.execute(
            """
            INSERT INTO edgar_accession_file (accession, file_name, fetched_at, compressed_payload, source_url)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(accession, file_name) DO UPDATE SET
                fetched_at=excluded.fetched_at,
                compressed_payload=excluded.compressed_payload,
                source_url=COALESCE(excluded.source_url, edgar_accession_file.source_url)
            """,
            (accession, file_name, ts, compressed, source_url),
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

    def get_accession_files_info(self, accession: str) -> list[tuple[str, str | None]]:
        cur = self.conn.execute(
            "SELECT file_name, source_url FROM edgar_accession_file WHERE accession = ? ORDER BY file_name",
            (accession,),
        )
        return [(row["file_name"], row["source_url"]) for row in cur.fetchall()]

    def ensure_fact_marker_table(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS edgar_fact_extract (
                cik TEXT NOT NULL,
                accession TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                fact_count INTEGER,
                note TEXT,
                PRIMARY KEY (cik, accession)
            )
            """
        )
        self.conn.commit()

    def has_processed_xbrl_facts(self, cik: str, accession: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM edgar_fact_extract WHERE cik = ? AND accession = ? LIMIT 1",
            (normalize_cik(cik), accession),
        ).fetchone()
        return row is not None

    def mark_xbrl_facts_processed(self, cik: str, accession: str, fact_count: int, note: str | None) -> None:
        ts = _iso()
        self.conn.execute(
            """
            INSERT INTO edgar_fact_extract (cik, accession, processed_at, fact_count, note)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cik, accession) DO UPDATE SET
                processed_at=excluded.processed_at,
                fact_count=excluded.fact_count,
                note=excluded.note
            """,
            (normalize_cik(cik), accession, ts, fact_count, note),
        )
        self.conn.commit()

    def clear_xbrl_fact_marker(self, cik: str, accession: str) -> None:
        self.conn.execute(
            "DELETE FROM edgar_fact_extract WHERE cik = ? AND accession = ?",
            (normalize_cik(cik), accession),
        )
        self.conn.commit()

    def reset_xbrl_accession(self, accession: str) -> None:
        context_rows = self.conn.execute(
            "SELECT context_id FROM xbrl_context WHERE accession = ?", (accession,)
        ).fetchall()
        if context_rows:
            placeholders = ",".join("?" for _ in context_rows)
            ids = [row["context_id"] for row in context_rows]
            self.conn.execute(
                f"DELETE FROM context_dimension WHERE context_id IN ({placeholders})",
                ids,
            )
        self.conn.execute("DELETE FROM xbrl_fact WHERE accession = ?", (accession,))
        self.conn.execute("DELETE FROM xbrl_context WHERE accession = ?", (accession,))
        self.conn.execute("DELETE FROM xbrl_unit WHERE accession = ?", (accession,))
        self.conn.commit()

    # --- XBRL helpers -------------------------------------------------------
    def get_or_create_entity_scheme(self, scheme: str) -> int:
        row = self.conn.execute("SELECT scheme_id FROM entity_scheme WHERE scheme = ?", (scheme,)).fetchone()
        if row:
            return row["scheme_id"]
        cur = self.conn.execute("INSERT INTO entity_scheme (scheme) VALUES (?)", (scheme,))
        self.conn.commit()
        return cur.lastrowid

    def upsert_xbrl_context(
        self,
        accession: str,
        context_ref: str,
        *,
        entity_scheme_id: int | None = None,
        entity_id: str | None = None,
        period_type: str = "duration",
        start_date: str | None = None,
        end_date: str | None = None,
        instant_date: str | None = None,
    ) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO xbrl_context (
                accession,
                context_ref,
                entity_scheme_id,
                entity_id,
                period_type,
                start_date,
                end_date,
                instant_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(accession, context_ref) DO UPDATE SET
                entity_scheme_id=excluded.entity_scheme_id,
                entity_id=excluded.entity_id,
                period_type=excluded.period_type,
                start_date=excluded.start_date,
                end_date=excluded.end_date,
                instant_date=excluded.instant_date
            """,
            (
                accession,
                context_ref,
                entity_scheme_id,
                entity_id,
                period_type,
                start_date,
                end_date,
                instant_date,
            ),
        )
        context_id = cur.lastrowid
        if not context_id:
            row = self.conn.execute(
                "SELECT context_id FROM xbrl_context WHERE accession = ? AND context_ref = ?",
                (accession, context_ref),
            ).fetchone()
            context_id = row["context_id"] if row else 0
        self.conn.commit()
        return context_id

    def get_contexts_for_accession(self, accession: str) -> list[XbrlContextRow]:
        cur = self.conn.execute(
            """
            SELECT *
            FROM xbrl_context
            WHERE accession = ?
            ORDER BY context_ref
            """,
            (accession,),
        )
        return [self._row_to_xbrl_context(row) for row in cur.fetchall()]

    def get_context_dimensions(self, context_id: int) -> list[ContextDimensionRow]:
        cur = self.conn.execute(
            "SELECT context_id, axis_id, member_id, typed_value FROM context_dimension WHERE context_id = ? ORDER BY axis_id",
            (context_id,),
        )
        return [
            self.ContextDimensionRow(
                context_id=row["context_id"],
                axis_id=row["axis_id"],
                member_id=row["member_id"],
                typed_value=row["typed_value"],
            )
            for row in cur.fetchall()
        ]

    def get_or_create_dimension_axis(self, qname: str) -> int:
        axis = self.conn.execute("SELECT axis_id FROM dimension_axis WHERE qname = ?", (qname,)).fetchone()
        if axis:
            return axis["axis_id"]
        cur = self.conn.execute("INSERT INTO dimension_axis (qname) VALUES (?)", (qname,))
        self.conn.commit()
        return cur.lastrowid

    def get_or_create_dimension_member(self, qname: str) -> int:
        member = self.conn.execute("SELECT member_id FROM dimension_member WHERE qname = ?", (qname,)).fetchone()
        if member:
            return member["member_id"]
        cur = self.conn.execute("INSERT INTO dimension_member (qname) VALUES (?)", (qname,))
        self.conn.commit()
        return cur.lastrowid

    def upsert_context_dimension(
        self,
        context_id: int,
        axis_id: int,
        *,
        member_id: int | None = None,
        typed_value: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO context_dimension (context_id, axis_id, member_id, typed_value)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(context_id, axis_id) DO UPDATE SET
                member_id=excluded.member_id,
                typed_value=excluded.typed_value
            """,
            (context_id, axis_id, member_id, typed_value),
        )
        self.conn.commit()

    def get_or_create_xbrl_concept(
        self,
        qname: str,
        *,
        label: str | None = None,
        data_type: str | None = None,
    ) -> int:
        self.conn.execute(
            "INSERT OR IGNORE INTO xbrl_concept (qname, label, data_type) VALUES (?, ?, ?)",
            (qname, label, data_type),
        )
        if label is not None or data_type is not None:
            updates = []
            params = []
            if label is not None:
                updates.append("label = ?")
                params.append(label)
            if data_type is not None:
                updates.append("data_type = ?")
                params.append(data_type)
            params.append(qname)
            self.conn.execute(f"UPDATE xbrl_concept SET {', '.join(updates)} WHERE qname = ?", params)
        row = self.conn.execute("SELECT concept_id FROM xbrl_concept WHERE qname = ?", (qname,)).fetchone()
        self.conn.commit()
        return row["concept_id"]

    def get_concept(self, qname: str) -> XbrlConceptRow | None:
        row = self.conn.execute(
            "SELECT concept_id, qname, label, data_type FROM xbrl_concept WHERE qname = ?",
            (qname,),
        ).fetchone()
        return self._row_to_concept(row) if row else None

    def get_or_create_xbrl_unit(self, accession: str, unit_ref: str, *, measure: str | None = None) -> int:
        self.conn.execute(
            "INSERT OR IGNORE INTO xbrl_unit (accession, unit_ref, measure) VALUES (?, ?, ?)",
            (accession, unit_ref, measure),
        )
        if measure is not None:
            self.conn.execute(
                """
                UPDATE xbrl_unit
                SET measure = ?
                WHERE accession = ? AND unit_ref = ? AND (measure IS NULL OR measure != ?)
                """,
                (measure, accession, unit_ref, measure),
            )
        row = self.conn.execute(
            "SELECT unit_id FROM xbrl_unit WHERE accession = ? AND unit_ref = ?",
            (accession, unit_ref),
        ).fetchone()
        self.conn.commit()
        return row["unit_id"]

    def get_units_for_accession(self, accession: str) -> list[XbrlUnitRow]:
        cur = self.conn.execute(
            "SELECT unit_id, accession, unit_ref, measure FROM xbrl_unit WHERE accession = ? ORDER BY unit_ref",
            (accession,),
        )
        return [self._row_to_unit(row) for row in cur.fetchall()]

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
        cur = self.conn.execute(
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        self.conn.commit()
        return cur.lastrowid

    def get_facts_for_accession(self, accession: str) -> list[XbrlFactRow]:
        cur = self.conn.execute(
            """
            SELECT fact_id, accession, concept_id, context_id, unit_id, decimals, precision,
                   sign, value_numeric, value_text, value_raw, is_nil, footnote_html
            FROM xbrl_fact
            WHERE accession = ?
            ORDER BY fact_id
            """,
            (accession,),
        )
        return [self._row_to_fact(row) for row in cur.fetchall()]

    def _row_to_xbrl_context(self, row: sqlite3.Row) -> XbrlContextRow:
        return self.XbrlContextRow(
            context_id=row["context_id"],
            accession=row["accession"],
            context_ref=row["context_ref"],
            entity_scheme_id=row["entity_scheme_id"],
            entity_id=row["entity_id"],
            period_type=row["period_type"],
            start_date=row["start_date"],
            end_date=row["end_date"],
            instant_date=row["instant_date"],
        )

    def _row_to_concept(self, row: sqlite3.Row) -> XbrlConceptRow:
        return self.XbrlConceptRow(
            concept_id=row["concept_id"],
            qname=row["qname"],
            label=row["label"],
            data_type=row["data_type"],
        )

    def _row_to_unit(self, row: sqlite3.Row) -> XbrlUnitRow:
        return self.XbrlUnitRow(
            unit_id=row["unit_id"],
            accession=row["accession"],
            unit_ref=row["unit_ref"],
            measure=row["measure"],
        )

    def _row_to_fact(self, row: sqlite3.Row) -> XbrlFactRow:
        return self.XbrlFactRow(
            fact_id=row["fact_id"],
            accession=row["accession"],
            concept_id=row["concept_id"],
            context_id=row["context_id"],
            unit_id=row["unit_id"],
            decimals=row["decimals"],
            precision=row["precision"],
            sign=row["sign"],
            value_numeric=row["value_numeric"],
            value_text=row["value_text"],
            value_raw=row["value_raw"],
            is_nil=row["is_nil"],
            footnote_html=row["footnote_html"],
        )

    def ingest_xbrl_facts(self, cik: str, accession: str, xml_bytes: bytes) -> int:
        self.ensure_fact_marker_table()
        cik_norm = normalize_cik(cik)
        if self.has_processed_xbrl_facts(cik_norm, accession):
            logger.debug("skipping already processed accession=%s for cik=%s", accession, cik_norm)
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
        for ctx in contexts.values():
            if ctx.period_type not in {"instant", "duration"}:
                continue
            start_date = _date_iso(ctx.period_start) if ctx.period_type == "duration" else None
            end_date = _date_iso(ctx.period_end) if ctx.period_type == "duration" else None
            instant_date = _date_iso(ctx.period_end) if ctx.period_type == "instant" else None
            scheme_id = (
                self.get_or_create_entity_scheme(ctx.entity_scheme)
                if ctx.entity_scheme
                else None
            )
            context_id = self.upsert_xbrl_context(
                accession,
                ctx.id,
                entity_scheme_id=scheme_id,
                entity_id=ctx.entity_id,
                period_type=ctx.period_type,
                start_date=start_date,
                end_date=end_date,
                instant_date=instant_date,
            )
            context_ids[ctx.id] = context_id
            self.conn.execute("DELETE FROM context_dimension WHERE context_id = ?", (context_id,))
            dims = dim_map.get(ctx.id) or []
            for dimension in dims:
                axis_id = self.get_or_create_dimension_axis(dimension.axis)
                member_id = (
                    self.get_or_create_dimension_member(dimension.member)
                    if dimension.member
                    else None
                )
                self.upsert_context_dimension(
                    context_id, axis_id, member_id=member_id, typed_value=dimension.typed_value
                )

        unit_map: dict[str, int] = {}
        for unit_ref, parsed_unit in units.items():
            measure = normalize_unit(parsed_unit.measures) or (
                parsed_unit.measures[0] if parsed_unit.measures else None
            )
            unit_map[unit_ref] = self.get_or_create_xbrl_unit(
                accession, unit_ref, measure=measure
            )

        fact_count = 0
        for fact in parsed.facts:
            context_id = context_ids.get(fact.context_ref or "")
            if context_id is None:
                continue
            concept_id = self.get_or_create_xbrl_concept(fact.name)
            unit_id = unit_map.get(fact.unit_ref or "")
            decimals = None
            if fact.decimals and fact.decimals.strip("-").isdigit():
                decimals = int(fact.decimals)
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

        note = f"inserted {fact_count} facts"
        self.mark_xbrl_facts_processed(cik_norm, accession, fact_count, note)
        return fact_count

    def known_accessions(self, cik: str) -> set[str]:
        """Return the set of accession numbers already recorded for a CIK."""
        cur = self.conn.execute("SELECT accession FROM edgar_accession WHERE cik = ?", (cik,))
        return {row["accession"] for row in cur.fetchall()}

    def has_accession(self, accession: str, *, cik: str | None = None) -> bool:
        if cik is None:
            cur = self.conn.execute("SELECT 1 FROM edgar_accession WHERE accession = ? LIMIT 1", (accession,))
        else:
            cur = self.conn.execute(
                "SELECT 1 FROM edgar_accession WHERE cik = ? AND accession = ? LIMIT 1", (cik, accession)
            )
        return cur.fetchone() is not None

    def get_accession_base_url(self, accession: str) -> str | None:
        cur = self.conn.execute("SELECT base_url FROM edgar_accession WHERE accession = ? LIMIT 1", (accession,))
        row = cur.fetchone()
        return row["base_url"] if row else None
