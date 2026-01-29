from __future__ import annotations

from datetime import datetime, timezone

import pytest

from profit.sources.fx import FxDailyFetcher, FxRatePoint, FxRequest, YFinanceFxDailyFetcher


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


    def test_fx_fetcher_dedup_and_sort():
        class FakeFxFetcher(FxDailyFetcher):
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

    fetcher = FakeFxFetcher(cache=None, max_window_days=None)
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

    fetcher = YFinanceFxDailyFetcher(cache=None, max_window_days=None)
    req = FxRequest(base_ccy="EUR", quote_ccy="USD", provider="yfinance", provider_code="EURUSD=X")
    pts = fetcher.timeseries_fetch_many([req], _dt(2025, 1, 2), _dt(2025, 1, 2))[0]
    assert len(pts) == 1
    assert pts[0].rate == 1.2345
