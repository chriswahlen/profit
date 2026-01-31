from __future__ import annotations

import hashlib
import logging
import math
import os
import sqlite3
import struct
import sys
import zlib
from array import array
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from profit.config import ensure_profit_conf_loaded, ProfitConfig

logger = logging.getLogger(__name__)


class ColumnarStoreError(RuntimeError):
    """Base error for the columnar slice store."""


class SeriesNotFoundError(KeyError):
    """Raised when a series_id does not exist."""


class SliceCorruptionError(ColumnarStoreError):
    """Raised when slice payload verification fails."""


def _default_db_path() -> Path:
    ensure_profit_conf_loaded()
    # Prefer data root for persistent series storage; fall back to cache root.
    data_root = ProfitConfig.resolve_data_root()
    if data_root:
        return Path(data_root) / "columnar.sqlite3"
    return ProfitConfig.resolve_cache_root() / "columnar.sqlite3"


def _default_unfetched_bits(sentinel_bits: int) -> int:
    alt = 0x7FF8000000000001  # quiet NaN with payload
    if sentinel_bits != alt:
        return alt
    return 0x7FF8000000000002


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _dt_to_us(ts: datetime) -> int:
    ts = _to_utc(ts)
    return int(ts.timestamp() * 1_000_000)


def _us_to_dt(ts_us: int) -> datetime:
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc)


@dataclass(frozen=True)
class SeriesConfig:
    series_id: int
    instrument_id: str
    dataset: str
    field: str
    step_us: int
    grid_origin_ts_us: int
    window_points: int
    compression: str  # "none" | "zlib"
    offsets_enabled: bool
    checksum_enabled: bool
    sentinel_f64: float  # decoded from sentinel_f64_bits; NaN is supported
    sentinel_f64_bits: int
    sentinel_unfetched_f64: float
    sentinel_unfetched_f64_bits: int
    high_water_ts_us: int | None


class ColumnarSqliteStore:
    """
    Columnar-on-SQLite store for fixed-step f64 time series.

    Storage model:
    - Each series defines a fixed step, an origin timestamp, and a canonical
      window size (`window_points`).
    - Each slice row stores exactly one canonical window for one series:
      `values[]` is a packed float64 array of length `window_points`.
    - There are no NULLs. Missing points are represented by a sentinel value
      (recommended: NaN).
    - Writes are provided as timestamp/value pairs; the store loads the affected
      canonical slices, overlays updates, and rewrites each slice atomically
      (INSERT OR REPLACE).
    """

    _SLICE_INSERT_SQL = """
        INSERT OR REPLACE INTO __col_slice__ (series_id, start_index, values_blob, offsets_blob, checksum_blob, completeness)
        VALUES (?, ?, ?, ?, ?, ?)
        """

    _CREATE_SERIES_SQL = """
        INSERT INTO __col_series__ (
            instrument_id,
            dataset,
            field,
            step_us,
            grid_origin_ts_us,
            window_points,
            compression,
            offsets_enabled,
            checksum_enabled,
            sentinel_f64_bits,
            sentinel_unfetched_f64_bits,
            high_water_ts_us
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    _SELECT_SERIES_SQL = """
        SELECT
            series_id,
            instrument_id,
            dataset,
            field,
            step_us,
            grid_origin_ts_us,
            window_points,
            compression,
            offsets_enabled,
            checksum_enabled,
            sentinel_f64_bits,
            sentinel_unfetched_f64_bits,
            high_water_ts_us
        FROM __col_series__
        WHERE series_id = ?
        """

    _DELETE_SLICES_SQL = "DELETE FROM __col_slice__ WHERE series_id = ?"
    _DELETE_SERIES_SQL = "DELETE FROM __col_series__ WHERE series_id = ?"
    _SELECT_SLICE_SQL = """
        SELECT values_blob, offsets_blob, checksum_blob
        FROM __col_slice__
        WHERE series_id = ? AND start_index = ?
        """
    _SELECT_SERIES_ID_SQL = """
        SELECT series_id
        FROM __col_series__
        WHERE instrument_id = ? AND dataset = ? AND field = ? AND step_us = ?
        """
    _SELECT_ALL_SERIES_SQL = """
        SELECT
            series_id,
            instrument_id,
            dataset,
            field,
            step_us,
            grid_origin_ts_us,
            window_points,
            compression,
            offsets_enabled,
            checksum_enabled,
            sentinel_f64_bits,
            sentinel_unfetched_f64_bits,
            high_water_ts_us
        FROM __col_series__
        """

    DEFAULT_PENDING_LIMIT = 32

    def __init__(
        self,
        db_path: Optional[Path] = None,
        *,
        conn: sqlite3.Connection | None = None,
        pending_limit: int | None = None,
        dedupe_pending: bool = False,
    ) -> None:
        self._owns_conn = conn is None
        if conn is None:
            self.db_path = Path(db_path) if db_path else _default_db_path()
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.db_path, cached_statements=256)
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        else:
            self.db_path = Path(db_path) if db_path else self._extract_db_path(conn)
        self._conn = conn
        self._cursor_cache: dict[str, sqlite3.Cursor] = {}
        self._pending_slices: list[tuple[int, int, bytes, bytes, bytes, int]] = []
        self._pending_high_water: dict[int, int] = {}
        self._pending_limit = pending_limit if pending_limit is not None else self.DEFAULT_PENDING_LIMIT
        self._dedupe_pending = dedupe_pending
        self._series_cache: dict[int, SeriesConfig] = {}
        self._init_schema()
        self._preload_series_cache()

    def _cursor(self, key: str) -> sqlite3.Cursor:
        cur = self._cursor_cache.get(key)
        if cur is None:
            cur = self._conn.cursor()
            self._cursor_cache[key] = cur
        return cur

    # Series -----------------------------------------------------------
    def create_series(
        self,
        *,
        instrument_id: str,
        dataset: str,
        field: str,
        step_us: int,
        grid_origin_ts_us: int,
        window_points: int,
        compression: str = "none",
        offsets_enabled: bool = False,
        checksum_enabled: bool = True,
        sentinel_f64: float = float("nan"),
        sentinel_unfetched_f64: float | None = None,
        high_water_ts_us: int | None = None,
    ) -> int:
        if step_us <= 0:
            raise ValueError("step_us must be > 0")
        if window_points <= 0:
            raise ValueError("window_points must be > 0")
        if compression not in {"none", "zlib"}:
            raise ValueError("compression must be 'none' or 'zlib'")

        sentinel_bits = _f64_to_u64(sentinel_f64)
        if sentinel_unfetched_f64 is None:
            sentinel_unfetched_bits = _default_unfetched_bits(sentinel_bits)
            sentinel_unfetched_f64 = _u64_to_f64(sentinel_unfetched_bits)
        else:
            sentinel_unfetched_bits = _f64_to_u64(sentinel_unfetched_f64)
        logger.info(
            "create_series instrument_id=%s dataset=%s field=%s step_us=%s window_points=%s compression=%s offsets=%s checksum=%s",
            instrument_id,
            dataset,
            field,
            step_us,
            window_points,
            compression,
            offsets_enabled,
            checksum_enabled,
        )
        cur = self._cursor("create_series")
        cur.execute(
            self._CREATE_SERIES_SQL,
            (
                instrument_id,
                dataset,
                field,
                int(step_us),
                int(grid_origin_ts_us),
                int(window_points),
                compression,
                1 if offsets_enabled else 0,
                1 if checksum_enabled else 0,
                int(sentinel_bits),
                int(sentinel_unfetched_bits),
                None if high_water_ts_us is None else int(high_water_ts_us),
            ),
        )
        self._conn.commit()
        series_id = int(cur.lastrowid)
        cfg = SeriesConfig(
            series_id=series_id,
            instrument_id=instrument_id,
            dataset=dataset,
            field=field,
            step_us=int(step_us),
            grid_origin_ts_us=int(grid_origin_ts_us),
            window_points=int(window_points),
            compression=compression,
            offsets_enabled=bool(offsets_enabled),
            checksum_enabled=bool(checksum_enabled),
            sentinel_f64=float(sentinel_f64),
            sentinel_f64_bits=int(sentinel_bits),
            sentinel_unfetched_f64=float(sentinel_unfetched_f64),
            sentinel_unfetched_f64_bits=int(sentinel_unfetched_bits),
            high_water_ts_us=None if high_water_ts_us is None else int(high_water_ts_us),
        )
        self._series_cache[series_id] = cfg
        return series_id

    def get_series(self, series_id: int) -> SeriesConfig:
        if series_id in self._series_cache:
            return self._series_cache[series_id]
        cur = self._cursor("select_series")
        cur.execute(self._SELECT_SERIES_SQL, (int(series_id),))
        row = cur.fetchone()
        if row is None:
            raise SeriesNotFoundError(series_id)
        cfg = self._row_to_series_config(row)
        self._series_cache[series_id] = cfg
        return cfg

    def _row_to_series_config(self, row: sqlite3.Row) -> SeriesConfig:
        sentinel_bits = int(row[10])
        sentinel_unfetched_bits = int(row[11]) if len(row) > 11 else 0
        if sentinel_unfetched_bits == 0 or sentinel_unfetched_bits == sentinel_bits:
            sentinel_unfetched_bits = _default_unfetched_bits(sentinel_bits)
        high_water_ts_us = None
        if len(row) > 12 and row[12] is not None:
            high_water_ts_us = int(row[12])
        sentinel_f64 = _u64_to_f64(sentinel_bits)
        sentinel_unfetched_f64 = _u64_to_f64(sentinel_unfetched_bits)
        return SeriesConfig(
            series_id=int(row[0]),
            instrument_id=str(row[1]),
            dataset=str(row[2]),
            field=str(row[3]),
            step_us=int(row[4]),
            grid_origin_ts_us=int(row[5]),
            window_points=int(row[6]),
            compression=str(row[7]),
            offsets_enabled=bool(row[8]),
            checksum_enabled=bool(row[9]),
            sentinel_f64=float(sentinel_f64),
            sentinel_f64_bits=sentinel_bits,
            sentinel_unfetched_f64=float(sentinel_unfetched_f64),
            sentinel_unfetched_f64_bits=sentinel_unfetched_bits,
            high_water_ts_us=high_water_ts_us,
        )

    def drop_series(self, series_id: int) -> None:
        """
        Remove a series definition and all its slices.

        Idempotent: dropping a missing series is a no-op.
        """
        cur = self._cursor("delete_slices")
        cur.execute(self._DELETE_SLICES_SQL, (int(series_id),))
        cur = self._cursor("delete_series")
        cur.execute(self._DELETE_SERIES_SQL, (int(series_id),))
        self._series_cache.pop(int(series_id), None)
        self._conn.commit()

    def get_series_id(
        self,
        *,
        instrument_id: str,
        dataset: str,
        field: str,
        step_us: int,
    ) -> int | None:
        """
        Look up a series_id by its natural unique key.

        Returns None when missing.
        """
        cur = self._cursor("select_series_id")
        cur.execute(
            self._SELECT_SERIES_ID_SQL,
            (instrument_id, dataset, field, int(step_us)),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    def get_high_water_ts_us(self, series_id: int) -> int | None:
        cfg = self.get_series(series_id)
        return cfg.high_water_ts_us

    def set_high_water_ts_us(self, series_id: int, ts_us: int | None) -> None:
        cfg = self.get_series(series_id)
        if ts_us is not None:
            ts_us = int(ts_us)
        if cfg.high_water_ts_us == ts_us:
            return
        cur = self._cursor("update_high_water")
        cur.execute("UPDATE __col_series__ SET high_water_ts_us = ? WHERE series_id = ?", (ts_us, int(series_id)))
        self._conn.commit()
        self._series_cache[series_id] = replace(cfg, high_water_ts_us=ts_us)

    def bump_high_water_ts_us(self, series_id: int, ts_us: int) -> None:
        cfg = self.get_series(series_id)
        if cfg.high_water_ts_us is not None and ts_us <= cfg.high_water_ts_us:
            return
        self.set_high_water_ts_us(series_id, ts_us)

    def get_or_create_series(
        self,
        *,
        instrument_id: str,
        dataset: str,
        field: str,
        step_us: int,
        grid_origin_ts_us: int,
        window_points: int,
        compression: str = "none",
        offsets_enabled: bool = False,
        checksum_enabled: bool = True,
        sentinel_f64: float = float("nan"),
    ) -> int:
        """
        Create a series if needed, otherwise return the existing series_id.
        """
        existing = self.get_series_id(
            instrument_id=instrument_id,
            dataset=dataset,
            field=field,
            step_us=step_us,
        )
        if existing is not None:
            return existing
        try:
            return self.create_series(
                instrument_id=instrument_id,
                dataset=dataset,
                field=field,
                step_us=step_us,
                grid_origin_ts_us=grid_origin_ts_us,
                window_points=window_points,
                compression=compression,
                offsets_enabled=offsets_enabled,
                checksum_enabled=checksum_enabled,
                sentinel_f64=sentinel_f64,
            )
        except sqlite3.IntegrityError:
            # Concurrent process or race: re-read.
            existing2 = self.get_series_id(
                instrument_id=instrument_id,
                dataset=dataset,
                field=field,
                step_us=step_us,
            )
            if existing2 is None:
                raise
            return existing2

    # Maintenance ------------------------------------------------------
    def checkpoint(self, mode: str = "PASSIVE") -> tuple[int, int, int]:
        """
        Run a WAL checkpoint and return SQLite's (busy, log, checkpointed) tuple.

        Mode is one of PASSIVE|FULL|RESTART|TRUNCATE (case-insensitive).
        """
        # Use a fresh connection when we don't own the shared connection to avoid
        # conflicting with in-flight statements on the primary handle.
        if not getattr(self, "_owns_conn", True) and self.db_path is not None:
            tmp_conn = sqlite3.connect(self.db_path)
            try:
                tmp_conn.execute("PRAGMA busy_timeout=5000")
                cur = tmp_conn.cursor()
                cur.execute(f"PRAGMA wal_checkpoint({mode})")
                row = cur.fetchone()
                return (int(row[0]), int(row[1]), int(row[2]))  # type: ignore[index]
            finally:
                tmp_conn.close()
        cur = self._cursor("wal_checkpoint")
        cur.execute(f"PRAGMA wal_checkpoint({mode})")
        row = cur.fetchone()
        return (int(row[0]), int(row[1]), int(row[2]))  # type: ignore[index]

    def optimize(self) -> None:
        """Run SQLite PRAGMA optimize for opportunistic maintenance."""
        self._conn.execute("PRAGMA optimize")
        self._conn.commit()

    def vacuum(self) -> None:
        """Run a VACUUM to reclaim space; requires no open write transactions."""
        self._conn.execute("VACUUM")
        self._conn.commit()

    def flush(self) -> None:
        """
        Public flush for pending slice writes. Mostly useful for callers that
        batch large writes and want an explicit barrier.
        """
        self._flush_pending_slices()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._flush_pending_slices()
        for cur in self._cursor_cache.values():
            try:
                cur.close()
            except Exception:
                pass
        if getattr(self, "_owns_conn", True):
            self._conn.close()

    # Writes -----------------------------------------------------------
    def write(self, series_id: int, points: Iterable[tuple[datetime, float]]) -> None:
        pts = list(points)
        logger.debug(
            "columnar.write series_id=%s points=%s first_ts=%s",
            series_id,
            len(pts),
            pts[0][0].isoformat() if pts else None,
        )
        cfg = self.get_series(series_id)
        grouped: dict[int, list[tuple[int, float]]] = {}
        grouped_offsets: dict[int, list[tuple[int, int]]] = {}

        for ts, value in pts:
            ts_us = _dt_to_us(ts)
            if ts_us < cfg.grid_origin_ts_us:
                raise ValueError("timestamp is before grid origin")
            prev_hw = self._pending_high_water.get(series_id)
            if prev_hw is None or ts_us > prev_hw:
                self._pending_high_water[series_id] = ts_us

            # Assign the point to the fixed-step bucket containing ts_us.
            rel_us = ts_us - cfg.grid_origin_ts_us
            idx = rel_us // cfg.step_us
            if idx > 2**63 - 1:
                raise ValueError("timestamp index overflow")

            slice_start = (idx // cfg.window_points) * cfg.window_points
            pos = int(idx - slice_start)
            grouped.setdefault(int(slice_start), []).append((pos, float(value)))

            if cfg.offsets_enabled:
                nominal_ts_us = cfg.grid_origin_ts_us + int(idx) * cfg.step_us
                delta_us = ts_us - nominal_ts_us
                # Store offsets as int32 milliseconds; this comfortably covers offsets
                # within a step (e.g., within a UTC day) without overflowing int32.
                if delta_us % 1000 != 0:
                    raise ValueError("offset requires millisecond alignment")
                offset_ms = delta_us // 1000
                if offset_ms < -(2**31) or offset_ms > (2**31 - 1):
                    raise ValueError("offset_ms out of int32 range")
                grouped_offsets.setdefault(int(slice_start), []).append((pos, int(offset_ms)))

        for slice_start, updates in grouped.items():
            values, offsets = self._load_or_init_slice(cfg, slice_start)
            for pos, value in updates:
                values[pos] = value  # Explicit sentinel overwrites are allowed.

            if cfg.offsets_enabled:
                off_updates = grouped_offsets.get(slice_start, [])
                for pos, offset_us in off_updates:
                    offsets[pos] = offset_us

            self._enqueue_slice(cfg, slice_start, values, offsets)

    # Reads (minimal; primarily for tests and debugging) ----------------
    def read_slice_values(self, series_id: int, slice_start_index: int) -> list[float]:
        cfg = self.get_series(series_id)
        values, _offsets = self._get_slice(cfg, slice_start_index, require_existing=False)
        logger.info(
            "columnar.read_slice series_id=%s slice_start_index=%s len=%s",
            series_id,
            slice_start_index,
            len(values),
        )
        return list(values)

    def read_points(
        self,
        series_id: int,
        *,
        start: datetime,
        end: datetime,
        include_sentinel: bool = True,
    ) -> list[tuple[datetime, float]]:
        cfg = self.get_series(series_id)
        start_us = _dt_to_us(start)
        end_us = _dt_to_us(end)
        if start_us < cfg.grid_origin_ts_us or end_us < cfg.grid_origin_ts_us:
            raise ValueError("timestamp is before grid origin")
        if start_us > end_us:
            raise ValueError("start must be <= end")

        start_idx = (start_us - cfg.grid_origin_ts_us) // cfg.step_us
        end_idx = (end_us - cfg.grid_origin_ts_us) // cfg.step_us
        start_slice = (start_idx // cfg.window_points) * cfg.window_points
        end_slice = (end_idx // cfg.window_points) * cfg.window_points

        out: list[tuple[datetime, float]] = []
        slice_start = int(start_slice)
        while slice_start <= int(end_slice):
            values, offsets = self._get_slice(cfg, slice_start, require_existing=False)
            for i in range(cfg.window_points):
                idx = slice_start + i
                if idx < int(start_idx) or idx > int(end_idx):
                    continue
                nominal_ts_us = cfg.grid_origin_ts_us + idx * cfg.step_us
                ts_us = nominal_ts_us
                if cfg.offsets_enabled:
                    ts_us = nominal_ts_us + int(offsets[i]) * 1000
                value = float(values[i])
                if not include_sentinel and (
                    _is_sentinel(value, cfg.sentinel_f64_bits) or _is_unfetched(value, cfg)
                ):
                    continue
                out.append((_us_to_dt(ts_us), value))
            slice_start += cfg.window_points
        logger.info(
            "columnar.read_points series_id=%s start=%s end=%s returned=%s",
            series_id,
            start.isoformat(),
            end.isoformat(),
            len(out),
        )
        return out

    def mark_range_fetched(
        self,
        series_id: int,
        *,
        start: datetime,
        end: datetime,
        missing_value: float | None = None,
    ) -> None:
        """
        Mark all unfetched points in [start, end] as fetched-but-missing by writing
        `missing_value` (defaults to the series sentinel).
        """
        cfg = self.get_series(series_id)
        miss = cfg.sentinel_f64 if missing_value is None else missing_value
        start_us = _dt_to_us(start)
        end_us = _dt_to_us(end)
        if start_us > end_us:
            raise ValueError("start must be <= end")
        if start_us < cfg.grid_origin_ts_us or end_us < cfg.grid_origin_ts_us:
            raise ValueError("timestamp is before grid origin")

        start_idx = (start_us - cfg.grid_origin_ts_us) // cfg.step_us
        end_idx = (end_us - cfg.grid_origin_ts_us) // cfg.step_us
        start_slice = (start_idx // cfg.window_points) * cfg.window_points
        end_slice = (end_idx // cfg.window_points) * cfg.window_points

        slice_start = int(start_slice)
        while slice_start <= int(end_slice):
            values, offsets = self._get_slice(cfg, slice_start, require_existing=False)
            changed = False
            for pos in range(cfg.window_points):
                idx = slice_start + pos
                if idx < int(start_idx) or idx > int(end_idx):
                    continue
                if _is_unfetched(values[pos], cfg):
                    values[pos] = miss
                    changed = True
            if changed:
                self._enqueue_slice(cfg, slice_start, values, offsets)
            slice_start += cfg.window_points

    def get_unfetched_ranges(
        self,
        series_id: int,
        *,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        """
        Return a list of contiguous sub-ranges within [start, end] that are still unfetched.
        """
        cfg = self.get_series(series_id)
        start_us = _dt_to_us(start)
        end_us = _dt_to_us(end)
        if start_us > end_us:
            raise ValueError("start must be <= end")
        if end_us < cfg.grid_origin_ts_us:
            return []

        start_us = max(start_us, cfg.grid_origin_ts_us)
        start_idx = (start_us - cfg.grid_origin_ts_us) // cfg.step_us
        end_idx = (end_us - cfg.grid_origin_ts_us) // cfg.step_us
        start_slice = (start_idx // cfg.window_points) * cfg.window_points
        end_slice = (end_idx // cfg.window_points) * cfg.window_points

        unfetched_ranges: list[tuple[int, int]] = []
        current_range: tuple[int, int] | None = None

        slice_start = int(start_slice)
        while slice_start <= int(end_slice):
            values, _offsets = self._get_slice(cfg, slice_start, require_existing=False)
            for pos in range(cfg.window_points):
                idx = slice_start + pos
                if idx < int(start_idx) or idx > int(end_idx):
                    continue
                if _is_unfetched(values[pos], cfg):
                    if current_range is None:
                        current_range = (idx, idx)
                    else:
                        current_range = (current_range[0], idx)
                else:
                    if current_range is not None:
                        unfetched_ranges.append(current_range)
                        current_range = None
            slice_start += cfg.window_points

        if current_range is not None:
            unfetched_ranges.append(current_range)

        def idx_to_dt(idx: int) -> datetime:
            ts_us = cfg.grid_origin_ts_us + idx * cfg.step_us
            return _us_to_dt(ts_us)

        return [(idx_to_dt(r0), idx_to_dt(r1)) for r0, r1 in unfetched_ranges]

    def is_range_complete(self, series_id: int, *, start: datetime, end: datetime) -> bool:
        """
        Return True if every point in [start, end] has been fetched (i.e., no unfetched sentinel).
        """
        cfg = self.get_series(series_id)
        start_us = _dt_to_us(start)
        end_us = _dt_to_us(end)
        if start_us < cfg.grid_origin_ts_us or end_us < cfg.grid_origin_ts_us:
            return False
        if start_us > end_us:
            return False

        start_idx = (start_us - cfg.grid_origin_ts_us) // cfg.step_us
        end_idx = (end_us - cfg.grid_origin_ts_us) // cfg.step_us
        start_slice = (start_idx // cfg.window_points) * cfg.window_points
        end_slice = (end_idx // cfg.window_points) * cfg.window_points

        slice_start = int(start_slice)
        while slice_start <= int(end_slice):
            values, _offsets = self._get_slice(cfg, slice_start, require_existing=False)
            for pos in range(cfg.window_points):
                idx = slice_start + pos
                if idx < int(start_idx) or idx > int(end_idx):
                    continue
                if _is_unfetched(values[pos], cfg):
                    return False
            slice_start += cfg.window_points
        return True

    # Internals --------------------------------------------------------
    def _init_schema(self) -> None:
        cur = self._cursor("schema_init")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS __col_series__ (
                series_id INTEGER PRIMARY KEY,
                instrument_id TEXT NOT NULL,
                dataset TEXT NOT NULL,
                field TEXT NOT NULL,
                step_us INTEGER NOT NULL,
                grid_origin_ts_us INTEGER NOT NULL,
                window_points INTEGER NOT NULL,
                compression TEXT NOT NULL,
                offsets_enabled INTEGER NOT NULL,
                checksum_enabled INTEGER NOT NULL,
                sentinel_f64_bits INTEGER NOT NULL,
                sentinel_unfetched_f64_bits INTEGER NOT NULL,
                high_water_ts_us INTEGER,
                UNIQUE (instrument_id, dataset, field, step_us)
            )
            """
        )
        cur.execute("PRAGMA table_info(__col_series__)")
        series_cols = {row[1] for row in cur.fetchall()}
        if "sentinel_unfetched_f64_bits" not in series_cols:
            cur.execute("ALTER TABLE __col_series__ ADD COLUMN sentinel_unfetched_f64_bits INTEGER NOT NULL DEFAULT 0")
            cur.execute("UPDATE __col_series__ SET sentinel_unfetched_f64_bits = sentinel_f64_bits")
        if "high_water_ts_us" not in series_cols:
            cur.execute("ALTER TABLE __col_series__ ADD COLUMN high_water_ts_us INTEGER")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS __col_slice__ (
                series_id INTEGER NOT NULL,
                start_index INTEGER NOT NULL,
                values_blob BLOB NOT NULL,
                offsets_blob BLOB NOT NULL,
                checksum_blob BLOB NOT NULL,
                completeness INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (series_id, start_index)
            )
            """
        )
        # Backfill completeness column for existing installs.
        cur.execute("PRAGMA table_info(__col_slice__)")
        cols = {row[1] for row in cur.fetchall()}
        if "completeness" not in cols:
            cur.execute("ALTER TABLE __col_slice__ ADD COLUMN completeness INTEGER NOT NULL DEFAULT 0")
        self._conn.commit()

    def _preload_series_cache(self) -> None:
        cur = self._cursor("select_all_series")
        cur.execute(self._SELECT_ALL_SERIES_SQL)
        for row in cur.fetchall():
            cfg = self._row_to_series_config(row)
            self._series_cache[cfg.series_id] = cfg

    def _load_or_init_slice(
        self,
        cfg: SeriesConfig,
        slice_start_index: int,
        *,
        require_existing: bool = False,
    ) -> tuple[array, array]:
        if slice_start_index % cfg.window_points != 0:
            raise ValueError("slice_start_index must be aligned to window_points")

        cur = self._cursor("select_slice")
        cur.execute(
            self._SELECT_SLICE_SQL,
            (int(cfg.series_id), int(slice_start_index)),
        )
        row = cur.fetchone()
        if row is None:
            if require_existing:
                raise KeyError((cfg.series_id, slice_start_index))
            values = _init_values(cfg.window_points, cfg.sentinel_unfetched_f64)
            offsets = _init_offsets(cfg.window_points)
            return values, offsets

        values_blob = bytes(row[0])
        offsets_blob = bytes(row[1])
        checksum_blob = bytes(row[2])
        return self._decode_slice_blobs(cfg, slice_start_index, values_blob, offsets_blob, checksum_blob)

    def _decode_slice_blobs(
        self,
        cfg: SeriesConfig,
        slice_start_index: int,
        values_blob: bytes,
        offsets_blob: bytes,
        checksum_blob: bytes,
    ) -> tuple[array, array]:
        values_bytes = _decompress_if_needed(values_blob, cfg.compression)
        offsets_bytes = _decompress_if_needed(offsets_blob, cfg.compression) if cfg.offsets_enabled else b""
        if cfg.checksum_enabled:
            expected = _slice_checksum(values_bytes, offsets_bytes)
            if checksum_blob != expected:
                raise SliceCorruptionError(
                    f"Checksum mismatch for series_id={cfg.series_id} start_index={slice_start_index}"
                )
        values = _decode_f64(values_bytes, cfg.window_points)
        offsets = _decode_i32(offsets_bytes, cfg.window_points) if cfg.offsets_enabled else _init_offsets(cfg.window_points)
        return values, offsets

    def _enqueue_slice(self, cfg: SeriesConfig, slice_start_index: int, values: array, offsets: array) -> None:
        if slice_start_index % cfg.window_points != 0:
            raise ValueError("slice_start_index must be aligned to window_points")
        if len(values) != cfg.window_points:
            raise ValueError("values must match window_points")
        if len(offsets) != cfg.window_points:
            raise ValueError("offsets must match window_points")

        values_bytes = _encode_f64(values)
        offsets_bytes = _encode_i32(offsets) if cfg.offsets_enabled else b""

        checksum = _slice_checksum(values_bytes, offsets_bytes) if cfg.checksum_enabled else b""
        values_blob = _compress_if_needed(values_bytes, cfg.compression)
        offsets_blob = _compress_if_needed(offsets_bytes, cfg.compression) if cfg.offsets_enabled else b""
        completeness = 0 if any(_is_unfetched(v, cfg) for v in values) else 1

        entry = (
            int(cfg.series_id),
            int(slice_start_index),
            sqlite3.Binary(values_blob),
            sqlite3.Binary(offsets_blob),
            sqlite3.Binary(checksum),
            int(completeness),
        )
        if self._dedupe_pending:
            for idx in range(len(self._pending_slices) - 1, -1, -1):
                if self._pending_slices[idx][0] == entry[0] and self._pending_slices[idx][1] == entry[1]:
                    del self._pending_slices[idx]
                    break
        self._pending_slices.append(entry)
        if len(self._pending_slices) >= self._pending_limit:
            self._flush_pending_slices()

    def _flush_pending_slices(self) -> None:
        if not self._pending_slices:
            return
        cur = self._cursor("insert_slice")
        in_tx = self._conn.in_transaction
        if not in_tx:
            cur.execute("BEGIN IMMEDIATE")
        try:
            cur.executemany(
                self._SLICE_INSERT_SQL,
                self._pending_slices,
            )
            self._apply_pending_high_water()
            if not in_tx:
                self._conn.commit()
        except Exception:
            if not in_tx:
                self._conn.rollback()
            raise
        finally:
            self._pending_slices.clear()

    def _pending_slice(self, cfg: SeriesConfig, slice_start_index: int) -> tuple[array, array] | None:
        for entry in reversed(self._pending_slices):
            sid, start, values_blob, offsets_blob, checksum_blob, _ = entry
            if sid == cfg.series_id and start == slice_start_index:
                return self._decode_slice_blobs(cfg, slice_start_index, values_blob, offsets_blob, checksum_blob)
        return None

    def _get_slice(self, cfg: SeriesConfig, slice_start_index: int, *, require_existing: bool) -> tuple[array, array]:
        pending = self._pending_slice(cfg, slice_start_index)
        if pending is not None:
            return pending
        return self._load_or_init_slice(cfg, slice_start_index, require_existing=require_existing)

    def _extract_db_path(self, conn: sqlite3.Connection) -> Path | None:
        try:
            cur = conn.execute("PRAGMA database_list")
            for row in cur.fetchall():
                if row[1] == "main" and row[2]:
                    return Path(row[2])
        except Exception:
            return None
        return None

    def _apply_pending_high_water(self) -> None:
        if not self._pending_high_water:
            return
        cur = self._cursor("update_high_water")
        for series_id, ts_us in self._pending_high_water.items():
            cfg = self.get_series(series_id)
            if cfg.high_water_ts_us is not None and ts_us <= cfg.high_water_ts_us:
                continue
            cur.execute(
                "UPDATE __col_series__ SET high_water_ts_us = ? WHERE series_id = ?",
                (int(ts_us), int(series_id)),
            )
            self._series_cache[series_id] = replace(cfg, high_water_ts_us=int(ts_us))
        self._pending_high_water.clear()


def _is_bitmatch(value: float, bits: int) -> bool:
    return _f64_to_u64(value) == bits


def _is_sentinel(value: float, sentinel_bits: int) -> bool:
    return _is_bitmatch(value, sentinel_bits)


def _is_unfetched(value: float, cfg: SeriesConfig) -> bool:
    return _is_bitmatch(value, cfg.sentinel_unfetched_f64_bits)


def _init_values(n: int, sentinel: float) -> array:
    values = array("d", [sentinel] * n)
    return values


def _init_offsets(n: int) -> array:
    offsets = array("i", [0] * n)
    return offsets


def _encode_f64(values: array) -> bytes:
    # Ensure stable little-endian encoding across platforms.
    vals = array("d", values)
    if sys.byteorder == "big":
        vals.byteswap()
    return vals.tobytes()


def _decode_f64(buf: bytes, n: int) -> array:
    vals = array("d")
    vals.frombytes(buf)
    if len(vals) != n:
        raise ValueError("Decoded f64 array length mismatch")
    if sys.byteorder == "big":
        vals.byteswap()
    return vals


def _encode_i32(offsets: array) -> bytes:
    offs = array("i", offsets)
    if sys.byteorder == "big":
        offs.byteswap()
    return offs.tobytes()


def _decode_i32(buf: bytes, n: int) -> array:
    offs = array("i")
    offs.frombytes(buf)
    if len(offs) != n:
        raise ValueError("Decoded i32 array length mismatch")
    if sys.byteorder == "big":
        offs.byteswap()
    return offs


def _compress_if_needed(buf: bytes, compression: str) -> bytes:
    if not buf:
        return b""
    if compression == "none":
        return buf
    return zlib.compress(buf)


def _decompress_if_needed(buf: bytes, compression: str) -> bytes:
    if not buf:
        return b""
    if compression == "none":
        return buf
    return zlib.decompress(buf)


def _slice_checksum(values_bytes: bytes, offsets_bytes: bytes) -> bytes:
    h = hashlib.blake2b(digest_size=16)
    h.update(values_bytes)
    h.update(offsets_bytes)
    return h.digest()


def _f64_to_u64(value: float) -> int:
    # sqlite3 treats NaN as NULL for REAL bindings; store sentinel as bits.
    return int.from_bytes(struct.pack("<d", float(value)), "little", signed=False)


def _u64_to_f64(bits: int) -> float:
    return struct.unpack("<d", int(bits).to_bytes(8, "little", signed=False))[0]
