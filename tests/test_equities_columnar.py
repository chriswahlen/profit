from __future__ import annotations

from datetime import datetime, timezone
import math

from profit.cache import ColumnarSqliteStore, FileCache
from profit.sources.equities import (
    EquityDailyBar,
    EquityDailyBarsRequest,
    EquitiesDailyFetcher,
)
from profit.sources.equities.columnar import ColumnarOhlcvConfig, ColumnarOhlcvWriter, DAY_US


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


def test_get_or_create_series_is_stable(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    sid1 = store.get_or_create_series(
        instrument_id="AAPL|XNAS",
        dataset="bar_ohlcv:yfinance:v1",
        field="close_raw",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=10,
        compression="none",
    )
    sid2 = store.get_or_create_series(
        instrument_id="AAPL|XNAS",
        dataset="bar_ohlcv:yfinance:v1",
        field="close_raw",
        step_us=DAY_US,
        grid_origin_ts_us=0,
        window_points=10,
        compression="none",
    )
    assert sid1 == sid2


def test_columnar_ohlcv_writer_roundtrip(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    writer = ColumnarOhlcvWriter(store)

    bars = [
        EquityDailyBar(
            instrument_id="AAPL|XNAS",
            ts_utc=_dt(2020, 1, 1),
            open_raw=1.0,
            high_raw=2.0,
            low_raw=0.5,
            close_raw=1.5,
            volume_raw=100.0,
            open_adj=1.1,
            high_adj=2.2,
            low_adj=0.55,
            close_adj=1.65,
            volume_adj=100.0,
            source="yfinance",
            version="v1",
            asof=_dt(2026, 1, 1),
        ),
        EquityDailyBar(
            instrument_id="AAPL|XNAS",
            ts_utc=_dt(2020, 1, 2),
            open_raw=3.0,
            high_raw=4.0,
            low_raw=2.5,
            close_raw=3.5,
            volume_raw=200.0,
            open_adj=3.3,
            high_adj=4.4,
            low_adj=2.75,
            close_adj=3.85,
            volume_adj=200.0,
            source="yfinance",
            version="v1",
            asof=_dt(2026, 1, 1),
        ),
    ]

    counts = writer.write_daily_bars(bars)
    assert counts["close_raw"] == 2
    assert counts["close_adj"] == 2

    cfg = ColumnarOhlcvConfig()
    dataset = cfg.dataset_name(source="yfinance", version="v1")
    series_id = store.get_series_id(
        instrument_id="AAPL|XNAS",
        dataset=dataset,
        field="close_raw",
        step_us=DAY_US,
    )
    assert series_id is not None

    pts = store.read_points(series_id, start=_dt(2020, 1, 1), end=_dt(2020, 1, 3), include_sentinel=True)
    assert len(pts) == 3
    assert pts[0] == (_dt(2020, 1, 1), 1.5)
    assert pts[1] == (_dt(2020, 1, 2), 3.5)
    assert math.isnan(pts[2][1])


def test_equities_fetcher_combines_and_dedups(tmp_path):
    class FakeFetcher(EquitiesDailyFetcher):
        def _fetch_timeseries_chunk(self, request, start, end):
            bar = EquityDailyBar(
                instrument_id=request.instrument_id,
                ts_utc=start,
                open_raw=1.0,
                high_raw=1.0,
                low_raw=1.0,
                close_raw=1.0,
                volume_raw=1.0,
                open_adj=1.0,
                high_adj=1.0,
                low_adj=1.0,
                close_adj=1.0,
                volume_adj=1.0,
                source=request.provider,
                version="v1",
                asof=_dt(2026, 1, 1),
            )
            return [bar, bar]

    fetcher = FakeFetcher(cache=FileCache(base_dir=tmp_path), max_window_days=None)
    req = EquityDailyBarsRequest(
        instrument_id="AAPL|XNAS",
        provider="yfinance",
        provider_code="AAPL",
        freq="1d",
    )
    bars = fetcher.timeseries_fetch(req, _dt(2020, 1, 1), _dt(2020, 1, 1))
    assert isinstance(bars, list)
    assert len(bars) == 1
