from __future__ import annotations

from datetime import datetime
from typing import Sequence

from profit.cache import ColumnarSqliteStore
from profit.sources.equities import ColumnarOhlcvConfig, ColumnarOhlcvWriter, EquityDailyBar


class EquitiesCoverageAdapter:
    def __init__(
        self,
        store: ColumnarSqliteStore,
        *,
        instrument_id: str,
        source: str,
        version: str,
        cfg: ColumnarOhlcvConfig | None = None,
    ) -> None:
        self.store = store
        self.instrument_id = instrument_id
        self.cfg = cfg or ColumnarOhlcvConfig()
        self.dataset = self.cfg.dataset_name(source=source, version=version)
        self.writer = ColumnarOhlcvWriter(store, cfg=self.cfg)

    def get_series_id(self, field: str) -> int | None:
        return self.store.get_series_id(
            instrument_id=self.instrument_id,
            dataset=self.dataset,
            field=field,
            step_us=self.cfg.step_us,
        )

    def get_unfetched_ranges(self, start: datetime, end: datetime) -> Sequence[tuple[datetime, datetime]]:
        sid = self.get_series_id("close_raw")
        if sid is None:
            return [(start, end)]
        return self.store.get_unfetched_ranges(sid, start=start, end=end)

    def write_points(self, bars: list[EquityDailyBar]) -> None:
        if not bars:
            return
        bars_sorted = sorted(bars, key=lambda b: b.ts_utc)
        self.writer.write_daily_bars(bars_sorted, coverage_start=bars_sorted[0].ts_utc, coverage_end=bars_sorted[-1].ts_utc)

    def read_points(self, start: datetime, end: datetime):
        sid = self.get_series_id("close_raw")
        if sid is None:
            return []
        return self.store.read_points(sid, start=start, end=end, include_sentinel=False)
