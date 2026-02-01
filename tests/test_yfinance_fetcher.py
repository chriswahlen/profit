from datetime import datetime, timedelta, timezone

import pandas as pd

from profit.cache import FileCache
from profit.config import ProfitConfig
from profit.sources.yfinance import FIELD_ORDER, YFinanceFetcher, YFinanceRequest


class _AlwaysActiveLifecycle:
    def get_lifecycle(self, provider: str, provider_code: str):
        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


class _NoopCatalogChecker:
    def ensure_fresh(self, provider: str):
        return

    def require_present(self, provider: str, provider_code: str):
        return


def _cfg(base):
    return ProfitConfig(
        data_root=base,
        cache_root=base,
        store_path=base / "col.sqlite3",
        log_level="INFO",
        refresh_catalog=False,
    )


def test_download_end_padded(tmp_path):
    calls = {}

    def fake_download(tickers, start, end, interval):
        calls["tickers"] = tickers
        calls["start"] = start
        calls["end"] = end
        calls["interval"] = interval
        return pd.DataFrame()

    fetcher = YFinanceFetcher(
        cfg=_cfg(tmp_path),
        cache=FileCache(base_dir=tmp_path),
        lifecycle=_AlwaysActiveLifecycle(),
        catalog_checker=_NoopCatalogChecker(),
        download_fn=fake_download,
    )

    req = YFinanceRequest("AAPL")
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    fetcher.timeseries_fetch_many([req], start, end)

    assert calls["tickers"] == ["AAPL"]
    assert calls["interval"] == "1d"
    assert calls["start"].date() == start.date()
    # yfinance download uses exclusive end; we add one day
    assert calls["end"].date() == (end + timedelta(days=1)).date()


def test_multiindex_split_and_normalization(tmp_path):
    idx = pd.date_range("2024-01-01", periods=2, freq="D", tz=None)
    cols = pd.MultiIndex.from_product([["AAPL", "MSFT"], ["Open", "Close", "Volume"]])
    data = [
        [10, 11, 100, 20, 21, 200],
        [12, 13, 110, 22, 23, 210],
    ]
    df = pd.DataFrame(data, index=idx, columns=cols)

    def fake_download(tickers, start, end, interval):
        return df

    fetcher = YFinanceFetcher(
        cfg=_cfg(tmp_path),
        cache=FileCache(base_dir=tmp_path),
        lifecycle=_AlwaysActiveLifecycle(),
        catalog_checker=_NoopCatalogChecker(),
        download_fn=fake_download,
    )

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    reqs = [YFinanceRequest("AAPL"), YFinanceRequest("MSFT")]
    out = fetcher.timeseries_fetch_many(reqs, start, end)

    aapl_df, msft_df = out
    assert list(aapl_df.columns) == FIELD_ORDER
    assert list(msft_df.columns) == FIELD_ORDER
    assert aapl_df.index.tz == timezone.utc
    assert msft_df.index.tz == timezone.utc
    assert len(aapl_df) == 2
    assert len(msft_df) == 2
    assert pd.isna(aapl_df.loc[aapl_df.index[0], "adj_close"])
    assert aapl_df.loc[aapl_df.index[0], "open"] == 10


def test_single_ticker_frame_normalizes_columns(tmp_path):
    idx = pd.date_range("2024-01-05", periods=1, freq="D", tz=None)
    df = pd.DataFrame(
        {"Open": [1.0], "High": [2.0], "Low": [0.5], "Close": [1.5], "Adj Close": [1.4], "Volume": [100]},
        index=idx,
    )

    def fake_download(tickers, start, end, interval):
        return df

    fetcher = YFinanceFetcher(
        cfg=_cfg(tmp_path),
        cache=FileCache(base_dir=tmp_path),
        lifecycle=_AlwaysActiveLifecycle(),
        catalog_checker=_NoopCatalogChecker(),
        download_fn=fake_download,
    )

    start = datetime(2024, 1, 5, tzinfo=timezone.utc)
    end = start
    req = YFinanceRequest("TSLA")
    out = fetcher.timeseries_fetch_many([req], start, end)[0]

    assert list(out.columns) == FIELD_ORDER
    assert out.index.tz == timezone.utc
    assert out.iloc[0].to_dict() == {
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "adj_close": 1.4,
        "volume": 100,
    }
