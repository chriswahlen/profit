from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

from profit.cache import FileCache
from profit.config import ProfitConfig
from profit.sources.base_fetcher import BaseFetcher
from profit.sources.batch_fetcher import BatchFetcher
from profit.sources.errors import ThrottledError
from profit.sources.types import Fingerprintable, LifecycleReader

class _NoopCatalogChecker:
    def ensure_fresh(self, provider: str):
        return

    def require_present(self, provider: str, provider_code: str):
        return


class _AlwaysActiveLifecycle(LifecycleReader):
    def get_lifecycle(self, provider: str, provider_code: str):
        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


def _dt(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _DummyBar:
    instrument_id: str
    ts_utc: datetime
    value: float
    source: str
    version: str
    asof: datetime


@dataclass(frozen=True)
class _DummyBarsRequest(Fingerprintable):
    instrument_id: str
    provider: str
    provider_code: str
    interval: str

    def fingerprint(self) -> str:
        return f"{self.provider}:{self.provider_code}:{self.instrument_id}:{self.interval}"


class _DummyBatchFetcher(BatchFetcher[_DummyBarsRequest, list[_DummyBar]]):
    def __init__(self, *args, **kwargs):
        cfg = kwargs.pop("cfg")
        super().__init__(*args, lifecycle=_AlwaysActiveLifecycle(), catalog_checker=_NoopCatalogChecker(), cfg=cfg, **kwargs)
        self.batch_calls: list[list[_DummyBarsRequest]] = []

    def _fetch_timeseries_chunk_many(self, requests, start, end):
        self.batch_calls.append(list(requests))
        bars = {}
        for req in requests:
            bars[req] = [
                _DummyBar(
                    instrument_id=req.instrument_id,
                    ts_utc=start,
                    value=1.0,
                    source=req.provider,
                    version="v1",
                    asof=_dt(2026, 1, 1),
                )
            ]
        return bars

    def _combine_chunks(self, chunks):
        if not chunks:
            return []
        if len(chunks) == 1:
            return chunks[0]
        out = []
        for chunk in chunks:
            out.extend(chunk)
        return out


def test_batch_fetcher_uses_single_batch_call(tmp_path):
    cache = FileCache(base_dir=tmp_path)
    cfg = ProfitConfig(
        data_root=tmp_path,
        cache_root=tmp_path,
        store_path=tmp_path / "col.sqlite3",
        log_level="INFO",
        refresh_catalog=False,
    )
    fetcher = _DummyBatchFetcher(cache=cache, cfg=cfg)

    req_a = _DummyBarsRequest("AAA|XNAS", "provider", "AAA", "1d")
    req_b = _DummyBarsRequest("BBB|XNAS", "provider", "BBB", "1d")

    results = fetcher.timeseries_fetch_many([req_a, req_b], _dt(2020, 1, 1), _dt(2020, 1, 1))

    assert len(fetcher.batch_calls) == 1
    assert len(results) == 2
    assert all(len(res) == 1 for res in results)
    ids = {res[0].instrument_id for res in results}
    assert ids == {"AAA|XNAS", "BBB|XNAS"}


class _SimpleRequest(Fingerprintable):
    def __init__(self, key: str, provider: str = "test", provider_code: str = "TEST") -> None:
        self.key = key
        self.provider = provider
        self.provider_code = provider_code

    def fingerprint(self) -> str:
        return self.key


class _ThrottlingBatchFetcher(BaseFetcher[_SimpleRequest, list[str]]):
    def __init__(self, *args, **kwargs):
        cfg = kwargs.pop("cfg")
        super().__init__(*args, lifecycle=_AlwaysActiveLifecycle(), catalog_checker=_NoopCatalogChecker(), cfg=cfg, **kwargs)
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
    cfg = ProfitConfig(
        data_root=tmp_path,
        cache_root=tmp_path,
        store_path=tmp_path / "col.sqlite3",
        log_level="INFO",
        refresh_catalog=False,
    )
    fetcher = _ThrottlingBatchFetcher(cache=cache, max_window_days=None, sleep_fn=_sleep, max_attempts=2, cfg=cfg)

    req = _SimpleRequest("r1")
    result = fetcher.timeseries_fetch_many([req], _dt(2020, 1, 1), _dt(2020, 1, 1))

    assert fetcher.attempts == 2
    assert result == [["ok-r1"]]
    assert sleeps  # should have slept once on throttle
def test_lifecycle_clipping_batches_by_window(tmp_path):
    # One symbol active full window, one delisted mid-window.
    class _Req:
        def __init__(self, instrument_id, provider, provider_code):
            self.instrument_id = instrument_id
            self.provider = provider
            self.provider_code = provider_code

        def fingerprint(self):
            return f"{self.provider}:{self.provider_code}:{self.instrument_id}"

    class _MapLifecycle(LifecycleReader):
        def __init__(self, mapping):
            self.mapping = mapping

        def get_lifecycle(self, provider: str, provider_code: str):
            return self.mapping.get((provider, provider_code))

    calls = []

    class _Fetcher(BaseFetcher[_Req, list[str]]):
        def _fetch_timeseries_chunk_many(self, requests, start, end):
            calls.append((start, end, list(requests)))
            return {req: [] for req in requests}

    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2020, 1, 30, tzinfo=timezone.utc)
    lifecycles = {
        ("p", "FULL"): (datetime(1900, 1, 1, tzinfo=timezone.utc), None),
        ("p", "CUT"): (datetime(1900, 1, 1, tzinfo=timezone.utc), datetime(2020, 1, 15, tzinfo=timezone.utc)),
    }
    fetcher = _Fetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        lifecycle=_MapLifecycle(lifecycles),
        catalog_checker=_NoopCatalogChecker(),
        cfg=ProfitConfig(
            data_root=tmp_path,
            cache_root=tmp_path,
            store_path=tmp_path / "col.sqlite3",
            log_level="INFO",
            refresh_catalog=False,
        ),
    )
    req_full = _Req("FULL|X", "p", "FULL")
    req_cut = _Req("CUT|X", "p", "CUT")

    fetcher.timeseries_fetch_many([req_full, req_cut], start, end)

    # Expect two batch calls: one full window (only FULL), one clipped (only CUT).
    assert len(calls) == 2
    windows = {(c[0].date(), c[1].date(), len(c[2])) for c in calls}
    assert windows == {
        (start.date(), end.date(), 1),
        (start.date(), datetime(2020, 1, 15, tzinfo=timezone.utc).date(), 1),
    }
