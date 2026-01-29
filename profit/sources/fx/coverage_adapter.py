from __future__ import annotations

from datetime import datetime
from typing import Sequence

from profit.cache import ColumnarSqliteStore
from profit.sources.fx import ColumnarFxConfig, ColumnarFxWriter, FxRatePoint


class FxCoverageAdapter:
    def __init__(
        self,
        store: ColumnarSqliteStore,
        *,
        pair: str,
        source: str,
        version: str,
        cfg: ColumnarFxConfig | None = None,
    ) -> None:
        self.store = store
        self.pair = pair
        self.cfg = cfg or ColumnarFxConfig()
        self.dataset = self.cfg.dataset_name(source=source, version=version)
        self.writer = ColumnarFxWriter(store, cfg=self.cfg)

    def get_series_id(self) -> int | None:
        return self.store.get_series_id(
            instrument_id=self.pair,
            dataset=self.dataset,
            field="rate",
            step_us=self.cfg.step_us,
        )

    def get_unfetched_ranges(self, start: datetime, end: datetime) -> Sequence[tuple[datetime, datetime]]:
        sid = self.get_series_id()
        if sid is None:
            return [(start, end)]
        return self.store.get_unfetched_ranges(sid, start=start, end=end)

    def write_points(self, points: list[FxRatePoint]) -> None:
        if not points:
            return
        pts_sorted = sorted(points, key=lambda p: p.ts_utc)
        self.writer.write_rates(pts_sorted, coverage_start=pts_sorted[0].ts_utc, coverage_end=pts_sorted[-1].ts_utc)

    def read_points(self, start: datetime, end: datetime):
        sid = self.get_series_id()
        if sid is None:
            return []
        return self.store.read_points(sid, start=start, end=end, include_sentinel=False)
