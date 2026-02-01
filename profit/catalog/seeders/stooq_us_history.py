from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.seeders.stooq_common import iterate_stooq_rows
from profit.catalog.seeders.stooq_us_equities import StooqUsEquitySeeder
from profit.seed_metadata import ensure_seed_metadata, read_seed_metadata, write_seed_metadata


@dataclass(frozen=True)
class SeedResult:
    rows_written: int


class StooqUsHistorySeeder:
    """
    Load historical daily OHLCV for U.S. symbols from the Stooq `d_us_txt` dataset
    into the columnar store (`ColumnarSqliteStore`) as fixed-step series per field.
    """

    SEEDER_KEY = "stooq_us_history"
    STEP_US = 86_400_000_000  # daily
    WINDOW_POINTS = 1095      # 3 years per slice
    GRID_ORIGIN = datetime(1900, 1, 1, tzinfo=timezone.utc)
    PROVIDER = "stooq"

    def __init__(
        self,
        store: ColumnarSqliteStore,
        *,
        data_root: Path,
        force: bool = False,
        ttl: timedelta = timedelta(days=7),
    ) -> None:
        self.store = store
        self.data_root = data_root
        self.force = force
        self.ttl = ttl
        self._series_cache: dict[tuple[str, str], int] = {}
        self._series_high_water: dict[int, int | None] = {}

    def seed(self) -> SeedResult:
        base = self._find_base_path()
        if base is None:
            logging.warning("Stooq US history base path missing under %s", self.data_root)
            return SeedResult(rows_written=0)

        ensure_seed_metadata(self.store._conn)
        if not self.force and self._should_skip():
            age = self._last_run_age()
            remaining = max(self.ttl - age, timedelta(0))
            logging.info(
                "Stooq US history seeder skipped: last_run_age=%s ttl=%s next_refresh_in=%s",
                age,
                self.ttl,
                remaining,
            )
            return SeedResult(rows_written=0)

        written = self._ingest(base)
        self._bump_metadata()
        logging.info("Stooq US history wrote rows=%s", written)
        return SeedResult(rows_written=written)

    # ------------------------------------------------------------------
    def _ingest(self, base: Path) -> int:
        total_points = 0
        buffers: dict[int, list[tuple[datetime, float]]] = {}
        max_written: dict[int, int] = {}
        files = list(base.rglob("*.txt"))
        self._precreate_series(files, base)

        def flush() -> int:
            nonlocal total_points
            flushed = 0
            for series_id, pts in buffers.items():
                if not pts:
                    continue
                self.store.write(series_id, pts)
                flushed += len(pts)
            total_points += flushed
            buffers.clear()
            return flushed

        for txt in files:
            relative = txt.relative_to(base)
            venue = self._venue_from_parts(relative.parts)
            mic = StooqUsEquitySeeder.MIC_BY_FOLDER.get(venue, "")

            logging.info("Stooq US history reading file %s", txt)
            for record in iterate_stooq_rows(txt):
                ts = record["date"]
                ts_us = int(ts.timestamp() * 1_000_000)
                instrument_id = self._instrument_id(mic, record["ticker"])
                data = {
                    "open": record["open"],
                    "high": record["high"],
                    "low": record["low"],
                    "close": record["close"],
                    "volume": record["volume"],
                    "openint": record["openint"],
                }
                for field, value in data.items():
                    sid = self._series_id(instrument_id, field)
                    high = self._series_high_water.get(sid)
                    if high is not None and ts_us <= high:
                        continue
                    buffers.setdefault(sid, []).append((ts, value))
                    prev = max_written.get(sid)
                    if prev is None or ts_us > prev:
                        max_written[sid] = ts_us
                if sum(len(v) for v in buffers.values()) >= 50_000:
                    flush()

        flush()
        for sid, ts_us in max_written.items():
            self.store.bump_high_water_ts_us(sid, ts_us)
            self._series_high_water[sid] = ts_us
        return total_points

    def _venue_from_parts(self, parts: tuple[str, ...]) -> str:
        # parts like ("nyse stocks", "1", "aapl.us.txt") or with leading directories
        if len(parts) >= 1:
            tokens = parts[0].split()
            return tokens[0].lower()
        return ""

    def _series_id(self, instrument_id: str, field: str) -> int:
        key = (instrument_id, field)
        if key in self._series_cache:
            return self._series_cache[key]
        series_id = self.store.get_or_create_series(
            instrument_id=instrument_id,
            field=field,
            provider_id=self.PROVIDER,
            step_us=self.STEP_US,
            grid_origin_ts_us=int(self.GRID_ORIGIN.timestamp() * 1_000_000),
            window_points=self.WINDOW_POINTS,
            compression="zlib",
            offsets_enabled=False,
            checksum_enabled=True,
            sentinel_f64=float("nan"),
        )
        self._series_cache[key] = series_id
        if series_id not in self._series_high_water:
            self._series_high_water[series_id] = self.store.get_high_water_ts_us(series_id)
        return series_id

    def _instrument_id(self, mic: str, ticker: str) -> str:
        base_ticker = ticker.split(".", 1)[0]
        return f"{mic or 'STOOQ'}|{base_ticker}"

    def _precreate_series(self, files: list[Path], base: Path) -> None:
        fields = ("open", "high", "low", "close", "volume", "openint")
        for txt in files:
            relative = txt.relative_to(base)
            venue = self._venue_from_parts(relative.parts)
            mic = StooqUsEquitySeeder.MIC_BY_FOLDER.get(venue, "")
            instrument_id = self._instrument_id(mic, txt.stem.upper())
            for field in fields:
                self._series_id(instrument_id, field)

    def _find_base_path(self) -> Path | None:
        candidates = [
            self.data_root / "market" / "d_us_txt" / "data" / "daily" / "us",
            self.data_root / "datasets" / "market" / "d_us_txt" / "data" / "daily" / "us",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _should_skip(self) -> bool:
        last = read_seed_metadata(self.store._conn, self.SEEDER_KEY)
        if last is None:
            return False
        return datetime.now(timezone.utc) - last < self.ttl

    def _last_run_age(self) -> timedelta:
        last = read_seed_metadata(self.store._conn, self.SEEDER_KEY)
        if last is None:
            return timedelta.max
        return datetime.now(timezone.utc) - last

    def _bump_metadata(self) -> None:
        write_seed_metadata(self.store._conn, self.SEEDER_KEY, datetime.now(timezone.utc))
