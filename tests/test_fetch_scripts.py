from __future__ import annotations

from datetime import datetime, timezone

import pytest

from profit.cache import ColumnarSqliteStore
from profit.sources.equities import ColumnarOhlcvConfig


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


def test_script_skip_fetch_when_complete(monkeypatch, tmp_path):
    # Prepare store with complete range.
    db_path = tmp_path / "columnar.sqlite3"
    store = ColumnarSqliteStore(db_path)
    cfg = ColumnarOhlcvConfig()
    dataset = cfg.dataset_name(source="yfinance", version="v1")
    sid = store.get_or_create_series(
        instrument_id="AAPL|XNAS",
        dataset=dataset,
        field="close_raw",
        step_us=86_400_000_000,
        grid_origin_ts_us=0,
        window_points=4,
        sentinel_f64=float("nan"),
    )
    store.mark_range_fetched(sid, start=_dt(2020, 1, 1), end=_dt(2020, 1, 2))

    called = {"fetch": 0}

    class FakeFetcher:
        def __init__(self, *_, **__):
            pass

        def timeseries_fetch(self, *_, **__):
            called["fetch"] += 1
            return []

    monkeypatch.setenv("PROFIT_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("PROFIT_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setattr("scripts.fetch_equities.YFinanceDailyBarsFetcher", FakeFetcher)

    from scripts import fetch_equities

    args = [
        "--ticker",
        "AAPL",
        "--mic",
        "XNAS",
        "--start",
        "2020-01-01",
        "--end",
        "2020-01-02",
        "--store-path",
        str(db_path),
    ]
    # Run main; should skip fetch because range is already complete.
    fetch_equities.main(args)
    assert called["fetch"] == 0
