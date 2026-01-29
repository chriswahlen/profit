from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from profit.cache import ColumnarSqliteStore
from profit.sources.fx.base import FxRatePoint


DAY_US = 86_400_000_000


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


@dataclass(frozen=True)
class ColumnarFxConfig:
    dataset_base: str = "fx_rate"
    step_us: int = DAY_US
    grid_origin_ts_us: int = 0
    window_points: int = 365
    compression: str = "zlib"
    offsets_enabled: bool = False
    checksum_enabled: bool = True
    sentinel_f64: float = float("nan")

    def dataset_name(self, *, source: str, version: str) -> str:
        return f"{self.dataset_base}:{source}:{version}"


class ColumnarFxWriter:
    """
    Write daily FX rates into ColumnarSqliteStore.

    One series per currency pair, dataset per (source, version), field name 'rate'.
    """

    def __init__(self, store: ColumnarSqliteStore, *, cfg: ColumnarFxConfig | None = None) -> None:
        self.store = store
        self.cfg = cfg or ColumnarFxConfig()

    def write_rates(self, points: Iterable[FxRatePoint]) -> int:
        rows = list(points)
        if not rows:
            return 0

        pairs = {f"{p.base_ccy}/{p.quote_ccy}" for p in rows}
        if len(pairs) != 1:
            raise ValueError("rates must contain exactly one currency pair per write")
        pair = next(iter(pairs))

        sources = {p.source for p in rows}
        versions = {p.version for p in rows}
        if len(sources) != 1 or len(versions) != 1:
            raise ValueError("rates must have a single (source, version) per write")
        source = next(iter(sources))
        version = next(iter(versions))

        dataset = self.cfg.dataset_name(source=source, version=version)
        series_id = self.store.get_or_create_series(
            instrument_id=pair,
            dataset=dataset,
            field="rate",
            step_us=self.cfg.step_us,
            grid_origin_ts_us=self.cfg.grid_origin_ts_us,
            window_points=self.cfg.window_points,
            compression=self.cfg.compression,
            offsets_enabled=self.cfg.offsets_enabled,
            checksum_enabled=self.cfg.checksum_enabled,
            sentinel_f64=self.cfg.sentinel_f64,
        )

        pts = [( _to_utc(p.ts_utc), float(p.rate)) for p in rows]
        self.store.write(series_id, pts)
        return len(pts)
