from __future__ import annotations

from datetime import datetime, timezone
import pytest

from profit.sources.base_fetcher import BaseFetcher
from profit.cache import FileCache
from profit.config import ProfitConfig
from profit.sources.errors import ThrottledError
from profit.sources.types import Fingerprintable, LifecycleReader


class _AlwaysActiveLifecycle(LifecycleReader):
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


class DummyReq(Fingerprintable):
    def __init__(self, code: str, provider: str = "p", provider_code: str = "PC") -> None:
        self.code = code
        self.provider = provider
        self.provider_code = provider_code

    def fingerprint(self) -> str:
        return f"dummy:{self.code}"


class ThrottleFetcher(BaseFetcher[DummyReq, int]):
    def __init__(self, raises: list[ThrottledError], cache_dir, cfg):
        super().__init__(
            cache=FileCache(base_dir=cache_dir),
            max_attempts=5,
            backoff_factor=0.1,
            max_backoff=1.0,
            retry_after_cap=0.5,
            lifecycle=_AlwaysActiveLifecycle(),
            catalog_checker=_NoopCatalogChecker(),
            cfg=cfg,
        )
        self.raises = raises
        self.calls = 0

    def _fetch_timeseries_chunk_many(self, requests, start, end):
        self.calls += 1
        if self.raises:
            raise self.raises.pop(0)
        return {req: 1 for req in requests}


def test_throttled_error_retries_until_success(tmp_path):
    # Avoid real sleep.
    sleeps = []

    def fake_sleep(x):
        sleeps.append(x)

    t1 = ThrottledError("429", retry_after=2.0)
    fetcher = ThrottleFetcher([t1], cache_dir=tmp_path / "cache1", cfg=_cfg(tmp_path))
    fetcher._sleep = fake_sleep  # type: ignore

    req = DummyReq("t")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start
    out = fetcher.timeseries_fetch_many([req], start, end)[0]
    assert out == 1
    assert fetcher.calls == 2  # one throttle, one success
    # Sleep should respect retry_after_cap (0.5) and max_backoff (1.0)
    assert sleeps[0] == 0.5


def test_throttled_error_exceeds_attempts(tmp_path):
    sleeps = []

    def fake_sleep(x):
        sleeps.append(x)

    t1 = ThrottledError("429", retry_after=10.0)
    t2 = ThrottledError("429", retry_after=None)
    fetcher = ThrottleFetcher([t1, t2, ThrottledError("429", retry_after=None)], cache_dir=tmp_path / "cache2", cfg=_cfg(tmp_path))
    fetcher._sleep = fake_sleep  # type: ignore
    fetcher.max_attempts = 2

    req = DummyReq("t")
    start = datetime(2020, 1, 1, tzinfo=timezone.utc)
    end = start
    with pytest.raises(ThrottledError):
        fetcher.timeseries_fetch_many([req], start, end)[0]
    # Two attempts: raise, retry, raise -> then stop.
    assert fetcher.calls == 2
    # First sleep capped by retry_after_cap=0.5
    assert sleeps and sleeps[0] == 0.5
