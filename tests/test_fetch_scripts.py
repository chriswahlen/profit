from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest

from profit.cache import ColumnarSqliteStore
from profit.catalog import CatalogStore, InstrumentRecord
from profit.sources.equities import ColumnarOhlcvConfig
from profit.config import ProfitConfig


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


def test_script_skip_fetch_when_complete(monkeypatch, tmp_path):
    # Prepare store with complete range.
    db_path = tmp_path / "columnar.sqlite3"
    store = ColumnarSqliteStore(db_path)
    catalog_path = tmp_path / "catalog.sqlite3"
    catalog = CatalogStore(catalog_path)
    catalog.upsert_instruments(
        [
            InstrumentRecord(
                instrument_id="AAPL|XNAS",
                instrument_type="equity",
                provider="yfinance",
                provider_code="AAPL",
                mic="XNAS",
                currency="USD",
                active_from=_dt(2010, 1, 1),
                active_to=None,
                attrs={},
            )
        ]
    )
    catalog.write_meta(provider="yfinance", refreshed_at=_dt(2026, 1, 1), source_version=None, row_count=1)
    ohlcv_cfg = ColumnarOhlcvConfig()
    dataset = ohlcv_cfg.dataset_name(source="yfinance", version="v1")
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

    cfg = ProfitConfig(
        data_root=tmp_path,
        cache_root=tmp_path / "cache",
        store_path=db_path,
        log_level="INFO",
        refresh_catalog=False,
    )
    monkeypatch.setattr("profit.config.get_data_root", lambda: tmp_path)
    monkeypatch.setenv("PROFIT_CACHE_DIR", str(cfg.cache_root))
    calls = {"fetch": 0}

    def fake_chunk(self, requests, start, end):
        calls["fetch"] += 1
        return {req: [] for req in requests}

    # Ensure repo root is on sys.path for namespace import of scripts.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from scripts import fetch_equities
    from profit.sources.equities.yfinance import YFinanceDailyBarsFetcher

    monkeypatch.setattr(YFinanceDailyBarsFetcher, "_fetch_timeseries_chunk_many", fake_chunk, raising=False)


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
    assert calls["fetch"] == 0
