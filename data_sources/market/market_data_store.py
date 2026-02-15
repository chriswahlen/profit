from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

from data_sources.data_store import DataSourceUpdateResults
from data_sources.sqlite_data_store import SqliteDataStore


@dataclass(frozen=True)
class Candle:
    canonical_id: str
    start_ts: str  # ISO date string YYYY-MM-DD (daily)
    adj_close: Optional[float] = None
    dividend: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    provider: str = ""  # provider code


class MarketDataStore(SqliteDataStore):
    """SQLite store for OHLCV candles with provider priority and best-view selection."""

    def __init__(self, config):
        super().__init__(db_name="market_ohlcv.sqlite", config=config, summary="Market OHLCV bars (raw + best view)")
        self._logger = logging.getLogger(__name__)
        self._provider_cache: dict[str, int] = {}
        self._ensure_schema()

    def describe_brief(self) -> str:
        return f"- market (sqlite://{self.db_path.name}): {self.summary}"

    # --- schema -------------------------------------------------------------
    def _ensure_schema(self) -> None:
        conn = self._ensure_conn()
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS providers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS candles_raw (
                canonical_id TEXT NOT NULL,
                start_ts DATE NOT NULL,
                adj_close REAL,
                dividend REAL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                provider_id INTEGER NOT NULL REFERENCES providers(id),
                PRIMARY KEY (canonical_id, start_ts, provider_id)
            );

            CREATE INDEX IF NOT EXISTS idx_candles_raw_lookup
              ON candles_raw(canonical_id, start_ts DESC);

            CREATE TABLE IF NOT EXISTS provider_priority (
                provider_id INTEGER NOT NULL REFERENCES providers(id),
                instrument_type TEXT,
                priority INTEGER NOT NULL,
                staleness_days INTEGER,
                PRIMARY KEY (provider_id, instrument_type)
            );

            CREATE TABLE IF NOT EXISTS instrument_provider_rank (
                canonical_id TEXT NOT NULL,
                provider_id INTEGER NOT NULL REFERENCES providers(id),
                priority INTEGER NOT NULL,
                staleness_days INTEGER,
                PRIMARY KEY (canonical_id, provider_id)
            );

            CREATE TABLE IF NOT EXISTS ingestion_runs (
                run_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                source TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                row_count INTEGER,
                notes TEXT
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
                    cr.*, p.code AS provider,
                    COALESCE(ipr.priority, pp.priority, 999) AS prio,
                    COALESCE(ipr.staleness_days, pp.staleness_days, 36500) AS stale_days,
                    julianday('now') - julianday(cr.start_ts) AS age_days,
                    ROW_NUMBER() OVER (
                        PARTITION BY cr.canonical_id, cr.start_ts
                        ORDER BY
                            COALESCE(ipr.priority, pp.priority, 999) ASC,
                            (julianday('now') - julianday(cr.start_ts) <= COALESCE(ipr.staleness_days, pp.staleness_days, 36500)) DESC,
                            p.code ASC
                    ) AS rn
                FROM candles_raw cr
                JOIN providers p ON p.id = cr.provider_id
                LEFT JOIN instrument_provider_rank ipr
                  ON ipr.canonical_id = cr.canonical_id AND ipr.provider_id = cr.provider_id
                LEFT JOIN provider_priority pp
                  ON pp.provider_id = cr.provider_id AND (pp.instrument_type IS NULL)
            )
            SELECT * FROM ranked WHERE rn = 1;
            """
        )
        conn.commit()

    # --- provider helpers ---------------------------------------------------
    def _provider_id(self, provider: str) -> int:
        if provider in self._provider_cache:
            return self._provider_cache[provider]
        conn = self._ensure_conn()
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO providers (code) VALUES (?);", (provider,))
        conn.commit()
        cur.execute("SELECT id FROM providers WHERE code=?;", (provider,))
        pid = cur.fetchone()[0]
        self._provider_cache[provider] = pid
        return pid

    # --- upserts -------------------------------------------------------------
    def upsert_provider_priority(self, *, provider: str, priority: int, instrument_type: str | None = None, staleness_days: int | None = None) -> None:
        pid = self._provider_id(provider)
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO provider_priority (provider_id, instrument_type, priority, staleness_days)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(provider_id, instrument_type) DO UPDATE SET
              priority=excluded.priority,
              staleness_days=excluded.staleness_days;
            """,
            (pid, instrument_type, priority, staleness_days),
        )
        conn.commit()

    def upsert_instrument_provider_rank(self, *, canonical_id: str, provider: str, priority: int, staleness_days: int | None = None) -> None:
        pid = self._provider_id(provider)
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO instrument_provider_rank (canonical_id, provider_id, priority, staleness_days)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(canonical_id, provider_id) DO UPDATE SET
              priority=excluded.priority,
              staleness_days=excluded.staleness_days;
            """,
            (canonical_id, pid, priority, staleness_days),
        )
        conn.commit()

    def upsert_candles_raw(self, rows: Iterable[Candle]) -> DataSourceUpdateResults:
        data = list(rows)
        if not data:
            return DataSourceUpdateResults()
        conn = self._ensure_conn()
        cur = conn.cursor()
        updated = failed = 0
        try:
            sql = """
                INSERT INTO candles_raw (
                    canonical_id, start_ts, adj_close, dividend,
                    open, high, low, close,
                    volume, provider_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(canonical_id, start_ts, provider_id) DO UPDATE SET
                    adj_close=excluded.adj_close,
                    dividend=excluded.dividend,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume;
            """
            params = [
                (
                    r.canonical_id,
                    r.start_ts,
                    r.adj_close,
                    r.dividend,
                    r.open,
                    r.high,
                    r.low,
                    r.close,
                    r.volume,
                    self._provider_id(r.provider),
                )
                for r in data
            ]
            cur.executemany(sql, params)
            updated = len(data)
            conn.commit()
        except Exception:
            conn.rollback()
            failed = len(data)
            self._logger.exception("Failed to upsert %d candles", len(data))
        return DataSourceUpdateResults(updated=updated, failed=failed)

    # --- queries -------------------------------------------------------------
    def last_start_ts(self, canonical_id: str) -> Optional[str]:
        cur = self._ensure_conn().cursor()
        cur.execute(
            """SELECT start_ts FROM candles_raw
                WHERE canonical_id=?
                ORDER BY start_ts DESC LIMIT 1;""",
            (canonical_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    # --- ingestion runs ------------------------------------------------------
    def start_ingestion_run(self, *, provider: str, source: str | None = None) -> str:
        import uuid

        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn = self._ensure_conn()
        conn.execute(
            """
            INSERT INTO ingestion_runs (run_id, provider, source, started_at, status)
            VALUES (?, ?, ?, ?, 'running');
            """,
            (run_id, provider, source, started_at),
        )
        conn.commit()
        return run_id

    def finish_ingestion_run(self, *, run_id: str, status: str, row_count: int = 0, notes: str | None = None) -> None:
        finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn = self._ensure_conn()
        conn.execute(
            """
            UPDATE ingestion_runs
            SET status=?, finished_at=?, row_count=?, notes=COALESCE(?, notes)
            WHERE run_id=?;
            """,
            (status, finished_at, row_count, notes, run_id),
        )
        conn.commit()
