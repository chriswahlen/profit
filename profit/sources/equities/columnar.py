from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from profit.cache import ColumnarSqliteStore
from profit.sources.equities import EquityDailyBar


DAY_US = 86_400_000_000


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


@dataclass(frozen=True)
class ColumnarOhlcvConfig:
    dataset_base: str = "bar_ohlcv"
    step_us: int = DAY_US
    grid_origin_ts_us: int = 0
    window_points: int = 365
    compression: str = "zlib"
    offsets_enabled: bool = False
    checksum_enabled: bool = True
    sentinel_f64: float = float("nan")

    def dataset_name(self, *, source: str, version: str) -> str:
        # Encode lineage into the dataset identifier so ColumnarSqliteStore's
        # series key remains globally unambiguous.
        return f"{self.dataset_base}:{source}:{version}"


class ColumnarOhlcvWriter:
    """
    Write daily OHLCV (raw + adjusted) into ColumnarSqliteStore.

    Storage model:
    - One series per (instrument_id, dataset, field, step_us)
    - Field names include variant suffix: `open_raw`, `open_adj`, etc.
    """

    def __init__(self, store: ColumnarSqliteStore, *, cfg: ColumnarOhlcvConfig | None = None) -> None:
        self.store = store
        self.cfg = cfg or ColumnarOhlcvConfig()

    def write_daily_bars(
        self,
        bars: Iterable[EquityDailyBar],
        *,
        coverage_start: datetime | None = None,
        coverage_end: datetime | None = None,
    ) -> dict[str, int]:
        """
        Write all fields for the given bars.

        Returns a mapping of field name -> points written.
        """
        rows = list(bars)
        if not rows:
            return {}

        instrument_ids = {b.instrument_id for b in rows}
        if len(instrument_ids) != 1:
            raise ValueError("bars must contain exactly one instrument_id per write")
        instrument_id = next(iter(instrument_ids))

        sources = {b.source for b in rows}
        versions = {b.version for b in rows}
        if len(sources) != 1 or len(versions) != 1:
            raise ValueError("bars must have a single (source, version) per write")
        source = next(iter(sources))
        version = next(iter(versions))

        dataset = self.cfg.dataset_name(source=source, version=version)

        series_ids: dict[str, int] = {}
        for field in (
            "open_raw",
            "high_raw",
            "low_raw",
            "close_raw",
            "volume_raw",
            "open_adj",
            "high_adj",
            "low_adj",
            "close_adj",
            "volume_adj",
        ):
            series_ids[field] = self.store.get_or_create_series(
                instrument_id=instrument_id,
                dataset=dataset,
                field=field,
                step_us=self.cfg.step_us,
                grid_origin_ts_us=self.cfg.grid_origin_ts_us,
                window_points=self.cfg.window_points,
                compression=self.cfg.compression,
                offsets_enabled=self.cfg.offsets_enabled,
                checksum_enabled=self.cfg.checksum_enabled,
                sentinel_f64=self.cfg.sentinel_f64,
            )

        points_by_field: dict[str, list[tuple[datetime, float]]] = {k: [] for k in series_ids}
        for bar in rows:
            ts = _to_utc(bar.ts_utc)
            points_by_field["open_raw"].append((ts, float(bar.open_raw)))
            points_by_field["high_raw"].append((ts, float(bar.high_raw)))
            points_by_field["low_raw"].append((ts, float(bar.low_raw)))
            points_by_field["close_raw"].append((ts, float(bar.close_raw)))
            points_by_field["volume_raw"].append((ts, float(bar.volume_raw)))
            points_by_field["open_adj"].append((ts, float(bar.open_adj)))
            points_by_field["high_adj"].append((ts, float(bar.high_adj)))
            points_by_field["low_adj"].append((ts, float(bar.low_adj)))
            points_by_field["close_adj"].append((ts, float(bar.close_adj)))
            points_by_field["volume_adj"].append((ts, float(bar.volume_adj)))

        counts: dict[str, int] = {}
        for field, series_id in series_ids.items():
            self.store.write(series_id, points_by_field[field])
            counts[field] = len(points_by_field[field])
        if coverage_start and coverage_end:
            for series_id in series_ids.values():
                self.store.mark_range_fetched(
                    series_id,
                    start=coverage_start,
                    end=coverage_end,
                    missing_value=self.cfg.sentinel_f64,
                )
        return counts
