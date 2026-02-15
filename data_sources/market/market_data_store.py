from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional, Sequence

from data_sources.data_store import DataSourceUpdateResults
from data_sources.sqlite_data_store import SqliteDataStore


@dataclass(frozen=True)
class Candle:
    canonical_id: str
    instrument_type: str  # security, fund, index, crypto, forex, commodity
    interval: str  # e.g., 1d, 1h, 1m
    start_ts: str  # ISO-like string acceptable to SQLite julianday()
    adj_close: Optional[float] = None
    dividend: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    vwap: Optional[float] = None
    trades_count: Optional[int] = None
    provider: str = ""
    source_run_id: Optional[str] = None
    metadata: Optional[str] = None


class MarketDataStore(SqliteDataStore):
    """SQLite store for OHLCV market data with provider-aware best-price view."""

    def __init__(self, config):
        super().__init__(db_name="market_ohlcv.sqlite", config=config, summary="Market OHLCV bars (raw + best view)")
        self._logger = logging.getLogger(__name__)
        self._ensure_schema()

    def describe_brief(self) -> str:
        return f"- market (sqlite://{self.db_path.name}): {self.summary}"

    def _ensure_schema(self) -> None:
        conn = self._ensure_conn()
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS candles_raw (
                canonical_id TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                interval TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                adj_close REAL,
                dividend REAL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                vwap REAL,
                trades_count INTEGER,
                provider TEXT NOT NULL,
                ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
                source_run_id TEXT,
                metadata TEXT,
                PRIMARY KEY (canonical_id, interval, start_ts, provider)
            );

            CREATE INDEX IF NOT EXISTS idx_candles_raw_lookup
              ON candles_raw(canonical_id, interval, start_ts DESC);

            CREATE TABLE IF NOT EXISTS provider_priority (
                provider TEXT NOT NULL,
                instrument_type TEXT,
                priority INTEGER NOT NULL,
                staleness_days INTEGER,
                PRIMARY KEY (provider, instrument_type)
            );

            CREATE TABLE IF NOT EXISTS instrument_provider_rank (
                canonical_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                priority INTEGER NOT NULL,
                staleness_days INTEGER,
                PRIMARY KEY (canonical_id, provider)
            );
            """
        )
        conn.commit()
        self._create_best_view(conn)

    def _create_best_view(self, conn) -> None:
        conn.execute("DROP VIEW IF EXISTS candles_best;")
        conn.execute(
            """
            CREATE VIEW candles_best AS
            WITH ranked AS (
                SELECT
                    cr.*,
                    COALESCE(ipr.priority, pp.priority, 999) AS prio,
                    COALESCE(ipr.staleness_days, pp.staleness_days, 36500) AS stale_days,
                    julianday('now') - julianday(cr.start_ts) AS age_days,
                    ROW_NUMBER() OVER (
                        PARTITION BY cr.canonical_id, cr.interval, cr.start_ts
                        ORDER BY
                            COALESCE(ipr.priority, pp.priority, 999) ASC,
                            (julianday('now') - julianday(cr.start_ts) <= COALESCE(ipr.staleness_days, pp.staleness_days, 36500)) DESC,
                            cr.ingested_at DESC,
                            cr.provider ASC
                    ) AS rn
                FROM candles_raw cr
                LEFT JOIN instrument_provider_rank ipr
                  ON ipr.canonical_id = cr.canonical_id AND ipr.provider = cr.provider
                LEFT JOIN provider_priority pp
                  ON pp.provider = cr.provider AND (pp.instrument_type IS NULL OR pp.instrument_type = cr.instrument_type)
            )
            SELECT * FROM ranked WHERE rn = 1;
            """
        )
        conn.commit()

    # --- upserts -------------------------------------------------------------
    def upsert_provider_priority(self, *, provider: str, priority: int, instrument_type: str | None = None, staleness_days: int | None = None) -> None:
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO provider_priority (provider, instrument_type, priority, staleness_days)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(provider, instrument_type) DO UPDATE SET
              priority=excluded.priority,
              staleness_days=excluded.staleness_days;
            """,
            (provider, instrument_type, priority, staleness_days),
        )
        conn.commit()

    def upsert_instrument_provider_rank(self, *, canonical_id: str, provider: str, priority: int, staleness_days: int | None = None) -> None:
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO instrument_provider_rank (canonical_id, provider, priority, staleness_days)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(canonical_id, provider) DO UPDATE SET
              priority=excluded.priority,
              staleness_days=excluded.staleness_days;
            """,
            (canonical_id, provider, priority, staleness_days),
        )
        conn.commit()

    def upsert_candles_raw(self, rows: Sequence[Candle]) -> DataSourceUpdateResults:
        conn = self._ensure_conn()
        cur = conn.cursor()
        updated = failed = 0
        try:
            sql = """
                INSERT INTO candles_raw (
                    canonical_id, instrument_type, interval, start_ts, adj_close, dividend,
                    open, high, low, close,
                    volume, vwap, trades_count, provider, source_run_id, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_id, interval, start_ts, provider) DO UPDATE SET
                    adj_close=excluded.adj_close,
                    dividend=excluded.dividend,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    vwap=excluded.vwap,
                    trades_count=excluded.trades_count,
                    source_run_id=excluded.source_run_id,
                    metadata=excluded.metadata,
                    ingested_at=datetime('now');
            """
            params = [
                (
                    r.canonical_id,
                    r.instrument_type,
                    r.interval,
                    r.start_ts,
                    r.adj_close,
                    r.dividend,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.volume,
                    r.vwap,
                    r.trades_count,
                    r.provider,
                    r.source_run_id,
                    r.metadata,
                )
                for r in rows
            ]
            cur.executemany(sql, params)
            updated = len(rows)
            conn.commit()
        except Exception:
            conn.rollback()
            failed = len(rows)
            self._logger.exception("Failed to upsert %d candles", len(rows))
        return DataSourceUpdateResults(updated=updated, failed=failed)

    # --- queries -------------------------------------------------------------
    def last_start_ts(self, canonical_id: str, interval: str) -> Optional[str]:
        cur = self._ensure_conn().cursor()
        cur.execute(
            """SELECT start_ts FROM candles_raw
                WHERE canonical_id=? AND interval=?
                ORDER BY start_ts DESC LIMIT 1;""",
            (canonical_id, interval),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def describe_detailed(self, *, indent: str = "  ") -> str:
        return super().describe_detailed(indent=indent)
