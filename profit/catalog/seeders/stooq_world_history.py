from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.seeders.stooq_common import canonical_instrument_id, iterate_stooq_rows
from profit.seed_metadata import ensure_seed_metadata, read_seed_metadata, write_seed_metadata


@dataclass(frozen=True)
class SeedResult:
    rows_written: int


class StooqWorldHistorySeeder:
    """
    Load historical daily OHLCV for global Stooq symbols into the columnar store.
    """

    DATASET = "stooq_world_bar_ohlcv"
    SEEDER_KEY = "stooq_world_history"
    STEP_US = 86_400_000_000
    WINDOW_POINTS = 1095
    GRID_ORIGIN = datetime(1750, 1, 1, tzinfo=timezone.utc)

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

    def seed(self) -> SeedResult:
        base = self._find_base_path()
        if base is None:
            logging.warning("Stooq world history base path missing under %s", self.data_root)
            return SeedResult(rows_written=0)

        ensure_seed_metadata(self.store._conn)
        if not self.force and self._should_skip():
            age = self._last_run_age()
            remaining = max(self.ttl - age, timedelta(0))
            logging.info(
                "Stooq world history seeder skipped: last_run_age=%s ttl=%s next_refresh_in=%s",
                age,
                self.ttl,
                remaining,
            )
            return SeedResult(rows_written=0)

        written = self._ingest(base)
        self._bump_metadata()
        logging.info("Stooq world history wrote rows=%s", written)
        return SeedResult(rows_written=written)

    def _ingest(self, base: Path) -> int:
        total_points = 0
        buffers: dict[int, list[tuple[datetime, float]]] = {}
        files = list(base.rglob("*.txt"))
        self._precreate_series(files, base)

        def flush() -> None:
            nonlocal total_points
            for series_id, pts in list(buffers.items()):
                if not pts:
                    continue
                self.store.write(series_id, pts)
                total_points += len(pts)
            buffers.clear()

        for txt in files:
            logging.info("Stooq world history reading file %s", txt)
            relative = txt.relative_to(base)
            parts = [p.lower() for p in relative.parts[:-1]]
            for record in iterate_stooq_rows(txt):
                ts = record["date"]
                instrument_id = canonical_instrument_id(record["ticker"], parts)
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
                    buffers.setdefault(sid, []).append((ts, value))
                if sum(len(v) for v in buffers.values()) >= 50_000:
                    flush()

        flush()
        return total_points

    def _series_id(self, instrument_id: str, field: str) -> int:
        key = (instrument_id, field)
        if key in self._series_cache:
            return self._series_cache[key]
        series_id = self.store.get_or_create_series(
            instrument_id=instrument_id,
            dataset=self.DATASET,
            field=field,
            step_us=self.STEP_US,
            grid_origin_ts_us=int(self.GRID_ORIGIN.timestamp() * 1_000_000),
            window_points=self.WINDOW_POINTS,
            compression="zlib",
            offsets_enabled=False,
            checksum_enabled=True,
            sentinel_f64=float("nan"),
        )
        self._series_cache[key] = series_id
        return series_id

    def _find_base_path(self) -> Path | None:
        candidates = [
            self.data_root / "market" / "d_world_txt" / "data" / "daily" / "world",
            self.data_root
            / "datasets"
            / "market"
            / "d_world_txt"
            / "data"
            / "daily"
            / "world",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _precreate_series(self, files: list[Path], base: Path) -> None:
        fields = ("open", "high", "low", "close", "volume", "openint")
        for txt in files:
            relative = txt.relative_to(base)
            parts = [p.lower() for p in relative.parts[:-1]]
            ticker = txt.stem.upper()
            instrument_id = canonical_instrument_id(ticker, parts)
            for field in fields:
                self._series_id(instrument_id, field)

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
