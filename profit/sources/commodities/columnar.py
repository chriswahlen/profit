from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from profit.cache import ColumnarSqliteStore
from profit.sources.commodities.base import CommodityDailyPrice


DAY_US = 86_400_000_000


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


@dataclass(frozen=True)
class ColumnarCommodityConfig:
    dataset_base: str = "commodity_price"
    step_us: int = DAY_US
    grid_origin_ts_us: int = 0
    window_points: int = 365
    compression: str = "zlib"
    offsets_enabled: bool = False
    checksum_enabled: bool = True
    sentinel_f64: float = float("nan")

    def dataset_name(self, *, source: str, version: str) -> str:
        return f"{self.dataset_base}:{source}:{version}"


class ColumnarCommodityWriter:
    """
    Write daily commodity prices into ColumnarSqliteStore.
    """

    def __init__(self, store: ColumnarSqliteStore, *, cfg: ColumnarCommodityConfig | None = None) -> None:
        self.store = store
        self.cfg = cfg or ColumnarCommodityConfig()

    def write_daily_prices(
        self,
        prices: Iterable[CommodityDailyPrice],
        *,
        coverage_start: datetime | None = None,
        coverage_end: datetime | None = None,
    ) -> int:
        rows = list(prices)
        if not rows:
            return 0

        instrument_ids = {p.instrument_id for p in rows}
        if len(instrument_ids) != 1:
            raise ValueError("prices must contain exactly one instrument_id per write")
        instrument_id = next(iter(instrument_ids))

        sources = {p.source for p in rows}
        versions = {p.version for p in rows}
        if len(sources) != 1 or len(versions) != 1:
            raise ValueError("prices must have a single (source, version) per write")
        source = next(iter(sources))
        version = next(iter(versions))

        dataset = self.cfg.dataset_name(source=source, version=version)
        series_id = self.store.get_or_create_series(
            instrument_id=instrument_id,
            dataset=dataset,
            field="price",
            step_us=self.cfg.step_us,
            grid_origin_ts_us=self.cfg.grid_origin_ts_us,
            window_points=self.cfg.window_points,
            compression=self.cfg.compression,
            offsets_enabled=self.cfg.offsets_enabled,
            checksum_enabled=self.cfg.checksum_enabled,
            sentinel_f64=self.cfg.sentinel_f64,
        )

        points = [( _to_utc(p.ts_utc), float(p.price)) for p in rows]
        self.store.write(series_id, points)

        if coverage_start and coverage_end:
            self.store.mark_range_fetched(
                series_id,
                start=coverage_start,
                end=coverage_end,
                missing_value=self.cfg.sentinel_f64,
            )

        return len(points)
