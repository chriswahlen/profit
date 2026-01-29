from __future__ import annotations

from datetime import datetime
from typing import Sequence

from profit.cache import ColumnarSqliteStore
from profit.sources.commodities.columnar import ColumnarCommodityConfig, ColumnarCommodityWriter
from profit.sources.commodities.base import CommodityDailyPrice


class CommoditiesCoverageAdapter:
    def __init__(
        self,
        store: ColumnarSqliteStore,
        *,
        instrument_id: str,
        source: str,
        version: str,
        cfg: ColumnarCommodityConfig | None = None,
    ) -> None:
        self.store = store
        self.instrument_id = instrument_id
        self.cfg = cfg or ColumnarCommodityConfig()
        self.dataset = self.cfg.dataset_name(source=source, version=version)
        self.writer = ColumnarCommodityWriter(store, cfg=self.cfg)

    def get_series_id(self) -> int | None:
        return self.store.get_series_id(
            instrument_id=self.instrument_id,
            dataset=self.dataset,
            field="price",
            step_us=self.cfg.step_us,
        )

    def get_unfetched_ranges(self, start: datetime, end: datetime) -> Sequence[tuple[datetime, datetime]]:
        sid = self.get_series_id()
        if sid is None:
            return [(start, end)]
        return self.store.get_unfetched_ranges(sid, start=start, end=end)

    def write_points(self, prices: list[CommodityDailyPrice]) -> None:
        if not prices:
            return
        prices_sorted = sorted(prices, key=lambda p: p.ts_utc)
        self.writer.write_daily_prices(
            prices_sorted,
            coverage_start=prices_sorted[0].ts_utc,
            coverage_end=prices_sorted[-1].ts_utc,
        )

    def read_points(self, start: datetime, end: datetime):
        sid = self.get_series_id()
        if sid is None:
            return []
        return self.store.read_points(sid, start=start, end=end, include_sentinel=False)
