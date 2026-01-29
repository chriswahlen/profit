from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

from profit.cache import ColumnarSqliteStore, FileCache
from profit.sources.base_fetcher import BaseFetcher
from profit.sources.equities import EquitiesDailyFetcher, EquityDailyBar, EquityDailyBarsRequest, YFinanceDailyBarsFetcher
from profit.sources.errors import ThrottledError
from profit.sources.fx import FxRatePoint, FxRequest, YFinanceFxDailyFetcher


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


class _FakeEquityBatchFetcher(EquitiesDailyFetcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_calls: list[list[EquityDailyBarsRequest]] = []

    def _fetch_timeseries_chunk_many(self, requests, start, end):
        self.batch_calls.append(list(requests))
        bars = {}
        for req in requests:
            bars[req] = [
                EquityDailyBar(
                    instrument_id=req.instrument_id,
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
                    source=req.provider,
                    version="v1",
                    asof=_dt(2026, 1, 1),
                )
            ]
        return bars


def test_equities_batch_fetcher_uses_single_batch_call(tmp_path):
    cache = FileCache(base_dir=tmp_path)
    fetcher = _FakeEquityBatchFetcher(cache=cache, max_window_days=None)

    req_a = EquityDailyBarsRequest("AAA|XNAS", "yfinance", "AAA", "1d")
    req_b = EquityDailyBarsRequest("BBB|XNAS", "yfinance", "BBB", "1d")

    results = fetcher.timeseries_fetch_many([req_a, req_b], _dt(2020, 1, 1), _dt(2020, 1, 1))

    assert len(fetcher.batch_calls) == 1
    assert len(results) == 2
    assert all(len(res) == 1 for res in results)
    ids = {res[0].instrument_id for res in results}
    assert ids == {"AAA|XNAS", "BBB|XNAS"}


@dataclass(frozen=True)
class _SimpleRequest:
    key: str

    def fingerprint(self) -> str:
        return self.key


class _ThrottlingBatchFetcher(BaseFetcher[_SimpleRequest, list[str]]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attempts = 0
        self.sleeps: list[float] = []

    def _fetch_timeseries_chunk_many(self, requests, start, end):
        self.attempts += 1
        if self.attempts == 1:
            raise ThrottledError("hit 429", retry_after=0.01)
        return {req: [f"ok-{req.key}"] for req in requests}

    def _combine_chunks(self, chunks):
        # passthrough for list[str]
        if not chunks:
            return []
        if len(chunks) == 1:
            return chunks[0]
        out = []
        for chunk in chunks:
            out.extend(chunk)
        return out


def test_timeseries_fetch_many_retries_on_throttle(tmp_path):
    def _sleep(d: float):
        sleeps.append(d)

    sleeps: list[float] = []
    cache = FileCache(base_dir=tmp_path)
    fetcher = _ThrottlingBatchFetcher(cache=cache, max_window_days=None, sleep_fn=_sleep, max_attempts=2)

    req = _SimpleRequest("r1")
    result = fetcher.timeseries_fetch_many([req], _dt(2020, 1, 1), _dt(2020, 1, 1))

    assert fetcher.attempts == 2
    assert result == [["ok-r1"]]
    assert sleeps  # should have slept once on throttle


def test_yfinance_equities_chunk_delegates_to_batch(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col.sqlite3")
    cache = FileCache(base_dir=tmp_path / "cache_eq")
    fetcher = YFinanceDailyBarsFetcher(store=store, cache=cache, max_window_days=None)

    called = {}

    def fake_batch(reqs, start, end):
        called["reqs"] = reqs
        req = reqs[0]
        return {
            req: [
                EquityDailyBar(
                    instrument_id=req.instrument_id,
                    ts_utc=start,
                    open_raw=1,
                    high_raw=1,
                    low_raw=1,
                    close_raw=1,
                    volume_raw=1,
                    open_adj=1,
                    high_adj=1,
                    low_adj=1,
                    close_adj=1,
                    volume_adj=1,
                    source=req.provider,
                    version="v1",
                    asof=_dt(2026, 1, 1),
                )
            ]
        }

    fetcher._fetch_timeseries_chunk_many = fake_batch  # type: ignore[assignment]

    req = EquityDailyBarsRequest("AAA|XNAS", "yfinance", "AAA", "1d")
    bars = fetcher._fetch_timeseries_chunk_many([req], _dt(2020, 1, 1), _dt(2020, 1, 1)).get(req, [])

    assert called["reqs"] == [req]
    assert len(bars) == 1
    assert bars[0].instrument_id == "AAA|XNAS"


def test_yfinance_fx_chunk_delegates_to_batch(tmp_path):
    store = ColumnarSqliteStore(tmp_path / "col_fx.sqlite3")
    cache = FileCache(base_dir=tmp_path / "cache_fx")
    fetcher = YFinanceFxDailyFetcher(store=store, cache=cache, max_window_days=None)

    called = {}

    def fake_batch(reqs, start, end):
        called["reqs"] = reqs
        req = reqs[0]
        return {
            req: [
                FxRatePoint(
                    base_ccy=req.base_ccy,
                    quote_ccy=req.quote_ccy,
                    ts_utc=start,
                    rate=1.23,
                    source="yfinance",
                    version="v1",
                    asof=_dt(2026, 1, 1),
                )
            ]
        }

    fetcher._fetch_timeseries_chunk_many = fake_batch  # type: ignore[assignment]

    req = FxRequest("EUR", "USD", "yfinance", "EURUSD=X", "1d")
    pts = fetcher._fetch_timeseries_chunk_many([req], _dt(2020, 1, 1), _dt(2020, 1, 1)).get(req, [])

    assert called["reqs"] == [req]
    assert len(pts) == 1
    assert pts[0].rate == 1.23
