from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import logging
import sqlite3

logger = logging.getLogger(__name__)


class RedfinStore:
    """
    Store for Redfin-derived tables.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        readonly: bool = False,
        conn: sqlite3.Connection | None = None,
        owns_conn: bool | None = None,
    ) -> None:
        self.db_path = Path(db_path)
        opened_conn = conn is None
        if opened_conn:
            if not readonly:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
            uri = f"file:{self.db_path.as_posix()}" + ("?mode=ro" if readonly else "")
            conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        self.conn = conn
        if owns_conn is None:
            self._owns_conn = opened_conn
        else:
            self._owns_conn = owns_conn
        self.conn.row_factory = sqlite3.Row
        if not readonly:
            self.ensure_schema()
        logger.info("redfin store opening %s (readonly=%s)", self.db_path, readonly)

    def ensure_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
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
        self.conn.commit()

    def close(self) -> None:
        if getattr(self, "_owns_conn", False):
            self.conn.close()

    @staticmethod
    def _coerce_date(value: str | datetime | date) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        return datetime.fromisoformat(value).date()

    def fetch_market_metrics(
        self,
        region_ids: Iterable[str],
        *,
        start_date: str | datetime | date,
        end_date: str | datetime | date,
    ) -> list[dict]:
        """
        Return market metrics rows for the provided regions and date window.
        """
        start = self._coerce_date(start_date)
        end = self._coerce_date(end_date)
        if start > end:
            raise ValueError("start_date must be <= end_date")
        region_ids = list(region_ids)
        if not region_ids:
            return []
        placeholders = ",".join("?" for _ in region_ids)
        cursor = self.conn.cursor()
        query = f"""
            SELECT region_id, period_start_date, period_granularity,
                   median_sale_price, median_list_price, homes_sold,
                   new_listings, inventory, median_dom, sale_to_list_ratio,
                   price_drops_pct, pending_sales, months_supply, avg_ppsf,
                   source_provider, data_revision
            FROM market_metrics
            WHERE region_id IN ({placeholders})
              AND period_start_date BETWEEN ? AND ?
            ORDER BY period_start_date ASC
            """
        params = (*region_ids, start.isoformat(), end.isoformat())
        logger.info("redfin query: %s params=%s", query.strip(), params)
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
