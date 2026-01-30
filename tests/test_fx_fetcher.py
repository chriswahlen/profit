from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from profit.sources.fx import FxDailyFetcher, FxRatePoint, FxRequest, YFinanceFxDailyFetcher
from profit.sources.types import LifecycleReader
from profit.config import ProfitConfig


class _NoopCatalogChecker:
    def ensure_fresh(self, provider: str): ...
    def require_present(self, provider: str, provider_code: str): ...


class _AlwaysActiveLifecycle(LifecycleReader):
    def get_lifecycle(self, provider: str, provider_code: str):
        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


def test_fx_fetcher_dedup_and_sort():
    class FakeFxFetcher(FxDailyFetcher):
        def __init__(self, *args, **kwargs):
            cfg = kwargs.pop("cfg")
            super().__init__(*args, lifecycle=_AlwaysActiveLifecycle(), catalog_checker=_NoopCatalogChecker(), cfg=cfg, **kwargs)

        def _fetch_timeseries_chunk_many(self, requests, start, end):
            results = {}
            for request in requests:
                results[request] = [
                    FxRatePoint(
                        base_ccy=request.base_ccy,
                        quote_ccy=request.quote_ccy,
                        ts_utc=start,
                        rate=1.1,
                        source=request.provider,
                        version="v1",
                        asof=_dt(2026, 1, 1),
                    ),
                    FxRatePoint(
                        base_ccy=request.base_ccy,
                        quote_ccy=request.quote_ccy,
                        ts_utc=start,
                        rate=1.1,
                        source=request.provider,
                        version="v1",
                        asof=_dt(2026, 1, 1),
                    ),
                ]
            return results

    cfg = ProfitConfig(
        data_root=Path("."),
        cache_root=Path("."),
        store_path=Path("col.sqlite3"),
        log_level="INFO",
        refresh_catalog=False,
    )
    fetcher = FakeFxFetcher(cache=None, max_window_days=None, cfg=cfg)
    req = FxRequest(base_ccy="EUR", quote_ccy="USD", provider="fake", provider_code="EURUSD")
    pts = fetcher.timeseries_fetch_many([req], _dt(2020, 1, 1), _dt(2020, 1, 1))[0]
    assert len(pts) == 1
    assert pts[0].rate == 1.1


@pytest.mark.skipif("pandas" not in globals(), reason="pandas not installed")
def test_yfinance_fx_handles_dataframe(monkeypatch):
    pd = pytest.importorskip("pandas")

    idx = pd.date_range("2025-01-02", periods=1, tz="UTC")
    df = pd.DataFrame({"Close": [1.2345]}, index=idx)

    def fake_download(symbol, *args, **kwargs):
        return df

    monkeypatch.setattr("profit.sources.fx.yfinance.yf", type("YF", (), {"download": fake_download}))

    cfg = ProfitConfig(
        data_root=Path("."),
        cache_root=Path("."),
        store_path=Path("col.sqlite3"),
        log_level="INFO",
        refresh_catalog=False,
    )
    fetcher = YFinanceFxDailyFetcher(
        cfg=cfg,
        cache=None,
        max_window_days=None,
        lifecycle=_AlwaysActiveLifecycle(),
        catalog_checker=_NoopCatalogChecker(),
    )
    req = FxRequest(base_ccy="EUR", quote_ccy="USD", provider="yfinance", provider_code="EURUSD=X")
    pts = fetcher.timeseries_fetch_many([req], _dt(2025, 1, 2), _dt(2025, 1, 2))[0]
    assert len(pts) == 1
    assert pts[0].rate == 1.2345
