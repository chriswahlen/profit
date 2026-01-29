from datetime import datetime, timedelta, timezone

import os
import pytest

from profit.cache import FileCache, OfflineModeError
from profit.sources.base_fetcher import BaseFetcher
from profit.sources.batch_fetcher import BatchFetcher
from profit.sources.types import Fingerprintable


class FakeRequest(Fingerprintable):
    def __init__(self, code: str) -> None:
        self.code = code

    def fingerprint(self) -> str:  # pragma: no cover - trivial
        return f"fake:{self.code}"


class FakeTimeseriesFetcher(BaseFetcher[FakeRequest, tuple[datetime, datetime]]):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[datetime, datetime]] = []

    def _fetch_timeseries_chunk(self, request, start, end):
        self.calls.append((start, end))
        return (start, end)


class FakeBatchFetcher(BatchFetcher[FakeRequest, int]):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.counter = 0

    def _download_bulk(self, request: FakeRequest) -> int:
        self.counter += 1
        return self.counter


def test_chunking_and_cache_hit(tmp_path):
    fetcher = FakeTimeseriesFetcher(cache=FileCache(base_dir=tmp_path), max_window_days=30)
    req = FakeRequest("ABC")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=60)

    first = fetcher.timeseries_fetch(req, start, end)
    assert len(fetcher.calls) == 3  # 61 days -> 3 chunks of 30/30/1
    assert len(first) == 3

    second = fetcher.timeseries_fetch(req, start, end)
    assert second == first
    assert len(fetcher.calls) == 3  # no additional network calls


def test_offline_cache_miss_raises(tmp_path):
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=tmp_path),
        offline=True,
        max_window_days=None,
    )
    req = FakeRequest("MISS")
    start = datetime(2021, 1, 1, tzinfo=timezone.utc)
    end = start

    with pytest.raises(OfflineModeError):
        fetcher.timeseries_fetch(req, start, end)


def test_expired_cache_triggers_refetch(tmp_path):
    cache = FileCache(base_dir=tmp_path, ttl=timedelta(days=1))
    fetcher = FakeTimeseriesFetcher(cache=cache, max_window_days=None)
    req = FakeRequest("TTL")
    start = datetime(2022, 6, 1, tzinfo=timezone.utc)
    end = start

    first = fetcher.timeseries_fetch(req, start, end)
    assert len(fetcher.calls) == 1

    # Force the cache entry to be stale.
    cache_key = fetcher._fingerprint(req, start, end)
    path = cache._key_to_path(cache_key)  # type: ignore[attr-defined]
    old_ts = (start - timedelta(days=10)).timestamp()
    os.utime(path, (old_ts, old_ts))

    second = fetcher.timeseries_fetch(req, start, end)
    assert second == first
    assert len(fetcher.calls) == 2  # refetched because TTL expired


def test_batch_fetcher_caches_bulk_download(tmp_path):
    batch = FakeBatchFetcher(cache=FileCache(base_dir=tmp_path))
    req = FakeRequest("BULK")

    first = batch.fetch(req)
    assert first == 1
    assert batch.counter == 1

    second = batch.fetch(req)
    assert second == 1
    assert batch.counter == 1  # reused cache

    offline = FakeBatchFetcher(cache=batch.cache, offline=True)
    with pytest.raises(OfflineModeError):
        offline.fetch(FakeRequest("NEW"))


def test_logging_hits_and_misses(tmp_path, caplog):
    fetcher = FakeTimeseriesFetcher(cache=FileCache(base_dir=tmp_path), max_window_days=None)
    req = FakeRequest("LOG")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start

    with caplog.at_level("INFO"):
        fetcher.timeseries_fetch(req, start, end)
        assert any("cache miss" in rec.message for rec in caplog.records)
        assert any("network request" in rec.message for rec in caplog.records)

    caplog.clear()
    with caplog.at_level("INFO"):
        fetcher.timeseries_fetch(req, start, end)
        assert any("cache hit" in rec.message for rec in caplog.records)
