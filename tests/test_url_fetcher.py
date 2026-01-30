from __future__ import annotations

import logging
import os
from datetime import timedelta

import pytest

from profit.cache import FileCache
from profit.utils.url_fetcher import (
    PermanentFetchError,
    TemporaryFetchError,
    fetch_url,
)


class _FakeFetcher:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def __call__(self, url: str, *, timeout: float, headers=None):  # pragma: no cover - exercised via fetch_url
        self.calls += 1
        status, body = self.responses[self.calls - 1]
        headers = headers or {}
        return type("Resp", (), {"status": status, "body": body, "headers": headers})()


def test_success_is_cached(tmp_path):
    cache = FileCache(base_dir=tmp_path)
    fetcher = _FakeFetcher([(200, b"ok")])

    first = fetch_url("http://example.com", cache=cache, fetch_fn=fetcher)
    assert first == b"ok"
    second = fetch_url("http://example.com", cache=cache, fetch_fn=fetcher)
    assert second == b"ok"
    assert fetcher.calls == 1


def test_permanent_error_cached(tmp_path):
    cache = FileCache(base_dir=tmp_path, ttl=timedelta(days=1))
    fetcher = _FakeFetcher([(404, b"missing")])

    with pytest.raises(PermanentFetchError):
        fetch_url("http://example.com/missing", cache=cache, fetch_fn=fetcher)

    with pytest.raises(PermanentFetchError) as excinfo:
        fetch_url("http://example.com/missing", cache=cache, fetch_fn=fetcher)
    assert excinfo.value.cached is True
    assert fetcher.calls == 1


def test_temporary_error_not_cached(tmp_path):
    cache = FileCache(base_dir=tmp_path)
    fetcher = _FakeFetcher([(503, b"busy"), (503, b"busy")])

    with pytest.raises(TemporaryFetchError):
        fetch_url("http://example.com/busy", cache=cache, fetch_fn=fetcher)

    with pytest.raises(TemporaryFetchError):
        fetch_url("http://example.com/busy", cache=cache, fetch_fn=fetcher)
    assert fetcher.calls == 2


def test_cache_respects_ttl(tmp_path):
    ttl = timedelta(minutes=1)
    cache = FileCache(base_dir=tmp_path, ttl=ttl)
    fetcher = _FakeFetcher([(200, b"first"), (200, b"second")])

    assert fetch_url("http://example.com/ttl", cache=cache, ttl=ttl, fetch_fn=fetcher) == b"first"
    key = "urlfetch::http://example.com/ttl"
    path = cache._key_to_path(key)
    old_time = path.stat().st_mtime - (ttl.total_seconds() + 1)
    os.utime(path, (old_time, old_time))

    assert fetch_url("http://example.com/ttl", cache=cache, ttl=ttl, fetch_fn=fetcher) == b"second"
    assert fetcher.calls == 2


def test_cache_stores_gzipped_payload(tmp_path):
    cache = FileCache(base_dir=tmp_path)
    fetcher = _FakeFetcher([(200, b"zip!")])

    assert fetch_url("http://example.com/zip", cache=cache, fetch_fn=fetcher) == b"zip!"

    entry = cache.get("urlfetch::http://example.com/zip", ttl=timedelta(days=1)).value
    assert entry.body.startswith(b'\x1f\x8b')
    assert fetcher.calls == 1


def test_logging_records_cache_and_network_events(tmp_path, caplog):
    cache = FileCache(base_dir=tmp_path)
    fetcher = _FakeFetcher([(200, b"log!")])

    caplog.set_level(logging.INFO, logger="profit.utils.url_fetcher")
    fetch_url("http://example.com/log", cache=cache, fetch_fn=fetcher)

    assert any("cache miss" in rec.message for rec in caplog.records)
    assert any("network request" in rec.message for rec in caplog.records)
    assert any("network response" in rec.message for rec in caplog.records)

    caplog.clear()
    fetch_url("http://example.com/log", cache=cache, fetch_fn=fetcher)

    assert any("cache hit" in rec.message for rec in caplog.records)
    assert not any("network request" in rec.message for rec in caplog.records)
    assert not any("network response" in rec.message for rec in caplog.records)
