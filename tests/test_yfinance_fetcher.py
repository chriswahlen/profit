from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from profit.sources.equities import EquityDailyBarsRequest, YFinanceDailyBarsFetcher
from profit.sources.types import LifecycleReader
from profit.config import ProfitConfig

class _NoopCatalogChecker:
    def ensure_fresh(self, provider: str): ...
    def require_present(self, provider: str, provider_code: str): ...


class _AlwaysActiveLifecycle(LifecycleReader):
    def get_lifecycle(self, provider: str, provider_code: str):
        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


@pytest.mark.skipif("pandas" not in globals(), reason="pandas not installed")
def test_yfinance_fetcher_handles_multiindex(monkeypatch):
    pd = pytest.importorskip("pandas")

    idx = pd.date_range("2025-01-01", periods=1, tz="UTC")
    cols = pd.MultiIndex.from_product(
        [["Open", "High", "Low", "Close", "Volume"], ["AAPL"]],
        names=["field", "ticker"],
    )
    raw_df = pd.DataFrame([[1.0, 2.0, 0.5, 1.5, 100.0]], index=idx, columns=cols)
    adj_df = pd.DataFrame([[1.1, 2.2, 0.55, 1.65, 100.0]], index=idx, columns=cols)

    def fake_download(symbol, *args, **kwargs):
        if kwargs.get("auto_adjust"):
            return adj_df
        return raw_df

    monkeypatch.setattr("profit.sources.equities.yfinance.yf", type("YF", (), {"download": fake_download}))

    cfg = ProfitConfig(
        data_root=tmp_path := Path("."),
        cache_root=tmp_path,
        store_path=tmp_path / "col.sqlite3",
        log_level="INFO",
        refresh_catalog=False,
    )
    fetcher = YFinanceDailyBarsFetcher(
        cfg=cfg,
        cache=None,
        max_window_days=None,
        lifecycle=_AlwaysActiveLifecycle(),
        catalog_checker=_NoopCatalogChecker(),
    )
    req = EquityDailyBarsRequest(
        instrument_id="AAPL|XNAS",
        provider="yfinance",
        provider_code="AAPL",
        freq="1d",
    )
    bars = fetcher.timeseries_fetch_many(
        [req],
        datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime(2025, 1, 1, tzinfo=timezone.utc),
    )[0]
    assert len(bars) == 1
    bar = bars[0]
    assert bar.close_raw == 1.5
    assert bar.close_adj == 1.65
