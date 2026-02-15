from __future__ import annotations

import sqlite3
import logging
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple, Any
from uuid import uuid4
from datetime import datetime

from config import Config
from data_sources.data_store import DataSourceUpdateResults
from data_sources.sqlite_data_store import SqliteDataStore


@dataclass
class Region:
    region_id: str
    region_type: str
    name: str
    canonical_code: str
    country_iso2: str
    parent_region_id: Optional[str] = None
    population: Optional[int] = None
    timezone: Optional[str] = None
    metadata: Optional[str] = None


@dataclass
class RegionCodeMapEntry:
    region_id: str
    code_type: str
    code_value: str
    active_from: str  # ISO date
    active_to: Optional[str] = None


@dataclass
class RegionProviderMapEntry:
    provider: str
    provider_region_id: str
    region_id: str
    provider_name: Optional[str]
    active_from: str  # ISO date
    active_to: Optional[str]
    data_revision: int


@dataclass
class MarketMetric:
    region_id: str
    property_type_id: str
    period_start_date: str
    period_granularity: str
    data_revision: int
    source_provider: str
    median_sale_price: Optional[float] = None
    median_list_price: Optional[float] = None
    homes_sold: Optional[int] = None
    new_listings: Optional[int] = None
    inventory: Optional[int] = None
    median_dom: Optional[float] = None
    sale_to_list_ratio: Optional[float] = None
    price_drops_pct: Optional[float] = None
    pending_sales: Optional[int] = None
    months_supply: Optional[float] = None
    avg_ppsf: Optional[float] = None


@dataclass
class IngestionRun:
    run_id: str
    provider: str
    started_at: str
    status: str
    finished_at: Optional[str] = None
    source_url: Optional[str] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    row_count: Optional[int] = None
    data_revision: Optional[int] = None
    notes: Optional[str] = None


class RedfinDataStore(SqliteDataStore):
    """SQLite store for Redfin regional metrics."""

    def __init__(self, config: Config):
        super().__init__(
            db_name="redfin_metrics.sqlite",
            config=config,
            summary="Redfin regions, code maps, and market metrics",
        )
        self._ensure_schema()

    def describe_brief(self) -> str:
        return f"- redfin (sqlite://{self.db_path.name}): {self.summary}"

    def _ensure_schema(self) -> None:
        conn = self._ensure_conn()
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS regions (
                region_id TEXT PRIMARY KEY,
                region_type TEXT NOT NULL,
                name TEXT NOT NULL,
                canonical_code TEXT NOT NULL,
                country_iso2 TEXT NOT NULL,
                parent_region_id TEXT REFERENCES regions(region_id),
                population INTEGER,
                timezone TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS region_provider_map (
                provider TEXT NOT NULL,
                provider_region_id TEXT NOT NULL,
                region_id TEXT NOT NULL REFERENCES regions(region_id),
                provider_name TEXT,
                active_from TEXT NOT NULL DEFAULT (datetime('now')),
                active_to TEXT,
                data_revision INTEGER NOT NULL,
                PRIMARY KEY (provider, provider_region_id, active_from),
                UNIQUE (provider, provider_region_id)
            );

            DROP TABLE IF EXISTS property_types;
            CREATE TABLE property_types (
                provider TEXT NOT NULL,
                property_type_id TEXT NOT NULL,
                property_type_name TEXT NOT NULL,
                PRIMARY KEY (provider, property_type_id)
            );

            DROP TABLE IF EXISTS market_metrics;

            CREATE TABLE market_metrics (
                region_id TEXT NOT NULL REFERENCES regions(region_id),
                property_type_id TEXT NOT NULL,
                period_start_date TEXT NOT NULL,
                period_granularity TEXT NOT NULL,
                data_revision INTEGER NOT NULL,
                source_provider TEXT NOT NULL,
                median_sale_price REAL,
                median_list_price REAL,
                homes_sold INTEGER,
                new_listings INTEGER,
                inventory INTEGER,
                median_dom REAL,
                sale_to_list_ratio REAL,
                price_drops_pct REAL,
                pending_sales INTEGER,
                months_supply REAL,
                avg_ppsf REAL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (region_id, property_type_id, period_start_date, period_granularity)
            );

            CREATE INDEX idx_market_metrics_region_period ON
                market_metrics(region_id, property_type_id, period_start_date DESC);
            CREATE INDEX idx_market_metrics_period ON
                market_metrics(period_start_date);
            CREATE INDEX idx_market_metrics_data_revision ON
                market_metrics(data_revision);

            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                source_url TEXT,
                etag TEXT,
                last_modified TEXT,
                row_count INTEGER,
                data_revision INTEGER,
                notes TEXT
            );
            """
        )
        conn.commit()

    def describe_detailed(self, *, indent: str = "  ") -> str:
        return super().describe_detailed(indent=indent)

    # --- Helpers -----------------------------------------------------------------
    @staticmethod
    @staticmethod
    def _slugify(text: str) -> str:
        cleaned = []
        last_sep = False
        for ch in text.lower():
            if ch.isalnum():
                cleaned.append(ch)
                last_sep = False
            else:
                if not last_sep:
                    cleaned.append("_")
                    last_sep = True
        slug = "".join(cleaned).strip("_")
        return slug or "unknown"

    def canonical_region_code(self, region_type: str, region_name: str, state_code: Optional[str], city: Optional[str] = None) -> str:
        from data_sources.region import Region

        region = Region.from_fields(
            region_type=region_type,
            region_name=region_name,
            country_iso2="us",
            state_code=state_code,
            city=city,
        )
        return region.canonical_id

    # --- Upsert methods ----------------------------------------------------------
    def upsert_region(
        self,
        *,
        region_id: str,
        region_type: str,
        name: str,
        canonical_code: str,
        country_iso2: str,
        parent_region_id: Optional[str] = None,
        population: Optional[int] = None,
        timezone: Optional[str] = None,
        metadata: Optional[str] = None,
    ) -> None:
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO regions (
                region_id, region_type, name, canonical_code, country_iso2,
                parent_region_id, population, timezone, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(region_id) DO UPDATE SET
                region_type=excluded.region_type,
                name=excluded.name,
                canonical_code=excluded.canonical_code,
                country_iso2=excluded.country_iso2,
                parent_region_id=excluded.parent_region_id,
                population=excluded.population,
                timezone=excluded.timezone,
                metadata=excluded.metadata;
            """,
            (region_id, region_type, name, canonical_code, country_iso2, parent_region_id, population, timezone, metadata),
        )

    def upsert_region_provider_map(
        self,
        *,
        provider: str,
        provider_region_id: str,
        region_id: str,
        provider_name: Optional[str],
        active_from: str,
        active_to: Optional[str],
        data_revision: int,
    ) -> None:
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO region_provider_map (
                provider, provider_region_id, region_id, provider_name, active_from, active_to, data_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_region_id) DO UPDATE SET
                region_id=excluded.region_id,
                provider_name=excluded.provider_name,
                active_from=excluded.active_from,
                active_to=excluded.active_to,
                data_revision=excluded.data_revision;
            """,
            (provider, provider_region_id, region_id, provider_name, active_from, active_to, data_revision),
        )

    def upsert_property_type(self, *, provider: str, property_type_id: str, property_type_name: str) -> None:
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO property_types (provider, property_type_id, property_type_name)
            VALUES (?, ?, ?)
            ON CONFLICT(provider, property_type_id) DO UPDATE SET
              property_type_name=excluded.property_type_name;
            """,
            (provider, property_type_id, property_type_name),
        )

    def upsert_market_metrics(self, rows: Sequence[MarketMetric]) -> DataSourceUpdateResults:
        conn = self._ensure_conn()
        cur = conn.cursor()
        updated = failed = 0
        logger = logging.getLogger(__name__)
        batch_size = 5000
        try:
            insert_sql = """
                INSERT INTO market_metrics (
                    region_id, property_type_id,
                    period_start_date, period_granularity, data_revision, source_provider,
                    median_sale_price, median_list_price, homes_sold, new_listings, inventory,
                    median_dom, sale_to_list_ratio, price_drops_pct, pending_sales,
                    months_supply, avg_ppsf
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(region_id, property_type_id, period_start_date, period_granularity) DO UPDATE SET
                    data_revision=excluded.data_revision,
                    source_provider=excluded.source_provider,
                    median_sale_price=excluded.median_sale_price,
                    median_list_price=excluded.median_list_price,
                    homes_sold=excluded.homes_sold,
                    new_listings=excluded.new_listings,
                    inventory=excluded.inventory,
                    median_dom=excluded.median_dom,
                    sale_to_list_ratio=excluded.sale_to_list_ratio,
                    price_drops_pct=excluded.price_drops_pct,
                    pending_sales=excluded.pending_sales,
                    months_supply=excluded.months_supply,
                    avg_ppsf=excluded.avg_ppsf;
            """
            batch = []
            for r in rows:
                batch.append(
                    (
                        r.region_id,
                        r.property_type_id,
                        r.period_start_date,
                        r.period_granularity,
                        r.data_revision,
                        r.source_provider,
                        r.median_sale_price,
                        r.median_list_price,
                        r.homes_sold,
                        r.new_listings,
                        r.inventory,
                        r.median_dom,
                        r.sale_to_list_ratio,
                        r.price_drops_pct,
                        r.pending_sales,
                        r.months_supply,
                        r.avg_ppsf,
                    )
                )
                if len(batch) >= batch_size:
                    cur.executemany(insert_sql, batch)
                    updated += len(batch)
                    batch.clear()
            if batch:
                cur.executemany(insert_sql, batch)
                updated += len(batch)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            failed = len(rows)
            logger.exception("Failed bulk upsert of %d market_metrics rows", len(rows), exc_info=exc)
        return DataSourceUpdateResults(updated=updated, failed=failed)

    def start_ingestion_run(self, *, provider: str, source_url: str | None = None, data_revision: Optional[int] = None) -> str:
        run_id = str(uuid4())
        started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO ingestion_runs (run_id, provider, started_at, status, source_url, data_revision)
            VALUES (?, ?, ?, 'running', ?, ?);
            """,
            (run_id, provider, started_at, source_url, data_revision),
        )
        conn.commit()
        return run_id

    def finish_ingestion_run(
        self,
        *,
        run_id: str,
        status: str,
        row_count: int,
        notes: Optional[str] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> None:
        finished_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        conn = self._ensure_conn()
        conn.execute(
            """
            UPDATE ingestion_runs
            SET status=?, finished_at=?, row_count=?, notes=COALESCE(?, notes),
                etag=COALESCE(?, etag), last_modified=COALESCE(?, last_modified)
            WHERE run_id=?;
            """,
            (status, finished_at, row_count, notes, etag, last_modified, run_id),
        )
        conn.commit()

    def resolve_region_by_provider(self, provider: str, provider_region_id: str) -> Optional[str]:
        conn = self._ensure_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT region_id FROM region_provider_map
            WHERE provider = ? AND provider_region_id = ? AND (active_to IS NULL OR active_to >= date('now'))
            ORDER BY active_from DESC
            LIMIT 1;
            """,
            (provider, provider_region_id),
        )
        row = cur.fetchone()
        return row[0] if row else None
