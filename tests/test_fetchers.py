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

    def _fetch_timeseries_chunk_many(self, requests, start, end):
        # Record each chunk window once (per batch slice).
        self.calls.append((start, end))
        return {req: (start, end) for req in requests}


def _fetch_single(fetcher: BaseFetcher, req: FakeRequest, start: datetime, end: datetime, *, coverage=None):
    cov_map = {req: coverage} if coverage else None
    return fetcher.timeseries_fetch_many([req], start, end, coverage_by_request=cov_map)[0]


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

    first = _fetch_single(fetcher, req, start, end)
    assert len(fetcher.calls) == 3  # 61 days -> 3 chunks of 30/30/1
    assert len(first) == 3

    second = _fetch_single(fetcher, req, start, end)
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
        _fetch_single(fetcher, req, start, end)


def test_expired_cache_triggers_refetch(tmp_path):
    cache = FileCache(base_dir=tmp_path, ttl=timedelta(days=1))
    fetcher = FakeTimeseriesFetcher(cache=cache, max_window_days=None)
    req = FakeRequest("TTL")
    start = datetime(2022, 6, 1, tzinfo=timezone.utc)
    end = start

    first = _fetch_single(fetcher, req, start, end)
    assert len(fetcher.calls) == 1

    # Force the cache entry to be stale.
    cache_key = fetcher._fingerprint(req, start, end)
    path = cache._key_to_path(cache_key)  # type: ignore[attr-defined]
    old_ts = (start - timedelta(days=10)).timestamp()
    os.utime(path, (old_ts, old_ts))

    second = _fetch_single(fetcher, req, start, end)
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


class DummyCoverageAdapter:
    def __init__(self, gaps, result=None):
        self.gaps = gaps
        self.result = result or []

    def get_unfetched_ranges(self, start, end):
        return list(self.gaps)

    def write_points(self, payload):
        pass

    def read_points(self, start, end):
        return self.result


def test_timeseries_fetch_skips_when_coverage_complete():
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=None),  # in-memory path
        max_window_days=None,
    )
    req = FakeRequest("SKIP")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start

    coverage = DummyCoverageAdapter(gaps=[], result=[("from_store", 1)])
    out = _fetch_single(fetcher, req, start, end, coverage=coverage)
    assert out == [("from_store", 1)]
    # No network calls made.
    assert fetcher.calls == []


def test_timeseries_fetch_uses_network_when_gaps_exist(tmp_path):
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        max_window_days=None,
    )
    req = FakeRequest("GAPS")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start

    coverage = DummyCoverageAdapter(gaps=[(start, end)])
    out = _fetch_single(fetcher, req, start, end, coverage=coverage)
    # Should call network once for the chunk.
    assert len(fetcher.calls) == 1
    assert out == []


def test_timeseries_fetch_chunks_with_coverage(tmp_path):
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        max_window_days=30,
    )
    req = FakeRequest("CHUNK")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = datetime(2020, 3, 1, tzinfo=timezone.utc)  # spans 61 days → 3 chunks

    calls = {"writes": 0}

    class Cov(DummyCoverageAdapter):
        def write_points(self, payload):
            calls["writes"] += 1

        def read_points(self, s, e):
            return "from_store"

    coverage = Cov(gaps=[(start, end)])
    out = _fetch_single(fetcher, req, start, end, coverage=coverage)
    assert calls["writes"] == 3  # one per chunk
    assert len(fetcher.calls) == 3
    assert out == "from_store"


def test_timeseries_fetch_uses_cache_for_some_gaps(tmp_path):
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        max_window_days=30,
    )
    req = FakeRequest("CACHE")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    mid = datetime(2020, 1, 30, tzinfo=timezone.utc)
    end = datetime(2020, 2, 1, tzinfo=timezone.utc)

    # Seed cache for first gap
    cache_key = fetcher._fingerprint(req, start, mid)  # type: ignore[attr-defined]
    fetcher.cache.set(cache_key, ("cached",))

    calls = {"writes": 0}

    class Cov(DummyCoverageAdapter):
        def write_points(self, payload):
            calls["writes"] += 1

        def read_points(self, s, e):
            return ["final"]

    coverage = Cov(gaps=[(start, mid), (mid + timedelta(days=1), end)])
    out = _fetch_single(fetcher, req, start, end, coverage=coverage)
    # One cache write for cached chunk, one network chunk
    assert calls["writes"] == 2
    assert len(fetcher.calls) == 1  # only the uncached chunk hit network
    assert out == ["final"]


def test_timeseries_fetch_offline_with_gaps_raises(tmp_path):
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        max_window_days=None,
        offline=True,
    )
    req = FakeRequest("OFFLINE")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start

    coverage = DummyCoverageAdapter(gaps=[(start, end)])
    with pytest.raises(OfflineModeError):
        _fetch_single(fetcher, req, start, end, coverage=coverage)
    assert fetcher.calls == []


def test_timeseries_fetch_retry_with_coverage(tmp_path):
    attempts = {"n": 0}

    class RetryFetcher(FakeTimeseriesFetcher):
        def _fetch_timeseries_chunk_many(self, requests, start, end):  # type: ignore[override]
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ValueError("transient")
            return super()._fetch_timeseries_chunk_many(requests, start, end)

    fetcher = RetryFetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        max_window_days=None,
    )
    req = FakeRequest("RETRY")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start
    coverage = DummyCoverageAdapter(gaps=[(start, end)])
    out = _fetch_single(fetcher, req, start, end, coverage=coverage)
    assert attempts["n"] == 2
    assert out == []


def test_timeseries_fetch_complete_but_empty_returns_store(tmp_path):
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        max_window_days=None,
    )
    req = FakeRequest("EMPTY")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start
    coverage = DummyCoverageAdapter(gaps=[], result=[])
    out = _fetch_single(fetcher, req, start, end, coverage=coverage)
    assert out == []


def test_timeseries_fetch_malformed_gap(tmp_path):
    fetcher = FakeTimeseriesFetcher(
        cache=FileCache(base_dir=tmp_path / "cache"),
        max_window_days=None,
    )
    req = FakeRequest("BADGAP")
    start = datetime(2020, 1, 2, tzinfo=timezone.utc)
    end = datetime(2020, 1, 1, tzinfo=timezone.utc)  # malformed (end before start)

    class Cov(DummyCoverageAdapter):
        def get_unfetched_ranges(self, s, e):
            return [(end, start)]  # reversed

    coverage = Cov(gaps=[])
    with pytest.raises(ValueError):
        _fetch_single(fetcher, req, start, end, coverage=coverage)


def test_logging_hits_and_misses(tmp_path, caplog):
    fetcher = FakeTimeseriesFetcher(cache=FileCache(base_dir=tmp_path), max_window_days=None)
    req = FakeRequest("LOG")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start

    with caplog.at_level("INFO"):
        _fetch_single(fetcher, req, start, end)
        assert any("cache miss" in rec.message for rec in caplog.records)
        assert any("network request" in rec.message for rec in caplog.records)

    caplog.clear()
    with caplog.at_level("INFO"):
        _fetch_single(fetcher, req, start, end)
        assert any("cache hit" in rec.message for rec in caplog.records)
