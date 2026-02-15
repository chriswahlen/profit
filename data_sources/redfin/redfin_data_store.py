from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional

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

            CREATE TABLE IF NOT EXISTS region_code_map (
                region_id TEXT NOT NULL REFERENCES regions(region_id),
                code_type TEXT NOT NULL,
                code_value TEXT NOT NULL,
                active_from TEXT NOT NULL DEFAULT (datetime('now')),
                active_to TEXT,
                PRIMARY KEY (region_id, code_type, code_value, active_from)
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

            CREATE TABLE IF NOT EXISTS market_metrics (
                region_id TEXT NOT NULL REFERENCES regions(region_id),
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
                PRIMARY KEY (region_id, period_start_date, period_granularity)
            );

            CREATE INDEX IF NOT EXISTS idx_market_metrics_region_period ON
                market_metrics(region_id, period_start_date DESC);
            CREATE INDEX IF NOT EXISTS idx_market_metrics_period ON
                market_metrics(period_start_date);
            CREATE INDEX IF NOT EXISTS idx_market_metrics_data_revision ON
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
