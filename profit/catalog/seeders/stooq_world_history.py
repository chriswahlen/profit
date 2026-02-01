from __future__ import annotations

import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import TextIOWrapper
from pathlib import Path
from typing import Iterable

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.seeders.stooq_common import canonical_instrument_id, iterate_stooq_rows_file
from profit.seed_metadata import ensure_seed_metadata, read_seed_metadata, write_seed_metadata


@dataclass(frozen=True)
class SeedResult:
    rows_written: int


class StooqWorldHistorySeeder:
    """
    Load historical daily OHLCV for global Stooq symbols into the columnar store.
    """

    SEEDER_KEY = "stooq_world_history"
    STEP_US = 86_400_000_000
    WINDOW_POINTS = 1095
    GRID_ORIGIN = datetime(1750, 1, 1, tzinfo=timezone.utc)
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
        zip_path = self._find_zip_path()
        if zip_path is None:
            logging.warning("Stooq world history zip missing; expected one of: %s", ", ".join(self._zip_candidates()))
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

        written = self._ingest(zip_path)
        self._bump_metadata()
        logging.info("Stooq world history wrote rows=%s", written)
        return SeedResult(rows_written=written)

    def _ingest(self, zip_path: Path) -> int:
        total_points = 0
        buffers: dict[int, list[tuple[datetime, float]]] = {}
        max_written: dict[int, int] = {}
        with zipfile.ZipFile(zip_path) as zf:
            txt_members = [zi for zi in zf.infolist() if zi.filename.lower().endswith(".txt")]

            def flush() -> None:
                nonlocal total_points
                for series_id, pts in list(buffers.items()):
                    if not pts:
                        continue
                    self.store.write(series_id, pts)
                    total_points += len(pts)
                buffers.clear()

            for member in txt_members:
                logging.info("Stooq world history reading file %s", member.filename)
                relative = Path(member.filename).parts[3:]  # drop data/daily/world
                parts = [p.lower() for p in relative[:-1]]
                with zf.open(member, "r") as fh:
                    for record in iterate_stooq_rows_file(TextIOWrapper(fh, encoding="utf-8"), member.filename):
                        ts = record["date"]
                        ts_us = int(ts.timestamp() * 1_000_000)
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

    def _find_zip_path(self) -> Path | None:
        for candidate in self._zip_candidates():
            if candidate.exists():
                return candidate
        return None

    def _zip_candidates(self) -> list[Path]:
        return [
            self.data_root / "datasets" / "stooq" / "d_world_txt.zip",
            self.data_root / "stooq" / "d_world_txt.zip",
        ]

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
