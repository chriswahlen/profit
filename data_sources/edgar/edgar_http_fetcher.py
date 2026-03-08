from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Tuple

from data_sources.base_fetcher import BaseFetcher
from data_sources.edgar.http import fetch_url
from network.cache import FileCache
from network.config import ProfitConfig
from network.sources.errors import ThrottledError
from network.sources.types import Fingerprintable, LifecycleReader


class _AlwaysActiveLifecycle(LifecycleReader):
    def get_lifecycle(self, provider: str, provider_code: str):
        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


class _NoopCatalogChecker:
    def mark_stale(self, provider: str) -> None:
        return

    def ensure_fresh(self, provider: str) -> None:
        return

    def require_present(self, provider: str, provider_code: str) -> None:
        return


def _profit_config_for_root(root: Path) -> ProfitConfig:
    root.mkdir(parents=True, exist_ok=True)
    cache_root = root / "fetcher-cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    store_path = root / "fetcher-store.sqlite"
    return ProfitConfig(
        data_root=root,
        cache_root=cache_root,
        store_path=store_path,
        log_level="INFO",
        refresh_catalog=False,
    )


@dataclass(frozen=True)
class EdgarHttpRequest(Fingerprintable):
    url: str
    headers: Tuple[Tuple[str, str], ...] = ()
    provider: str = "edgar"
    provider_code: str = "archive"

    def fingerprint(self) -> str:
        headers_str = "|".join(f"{k}:{v}" for k, v in self.headers)
        return f"{self.url}|{headers_str}|{self.provider}|{self.provider_code}"

    def headers_dict(self) -> dict[str, str]:
        return dict(self.headers)


class EdgarHttpFetcher(BaseFetcher[EdgarHttpRequest, bytes]):
    def __init__(
        self,
        *,
        cfg: ProfitConfig,
        user_agent: str,
        rate_limit_per_sec: float | None = 5.0,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(
            cfg=cfg,
            cache=FileCache(base_dir=cfg.cache_root),
            lifecycle=_AlwaysActiveLifecycle(),
            catalog_checker=_NoopCatalogChecker(),
            rate_limit_per_sec=rate_limit_per_sec,
            retry_exceptions=(ThrottledError, Exception),
        )
        self._user_agent = user_agent
        self._timeout = timeout

    @classmethod
    def from_data_root(cls, data_root: Path, *, user_agent: str, rate_limit_per_sec: float | None = 5.0) -> "EdgarHttpFetcher":
        cfg = _profit_config_for_root(data_root)
        return cls(cfg=cfg, user_agent=user_agent, rate_limit_per_sec=rate_limit_per_sec)

    def _fetch_timeseries_chunk_many(
        self,
        requests: Iterable[EdgarHttpRequest],
        start: datetime,
        end: datetime,
    ) -> dict[EdgarHttpRequest, bytes]:
        payloads: dict[EdgarHttpRequest, bytes] = {}
        for req in requests:
            headers = {"User-Agent": self._user_agent}
            headers.update(req.headers_dict())
            payloads[req] = fetch_url(req.url, headers=headers, timeout=self._timeout)
        return payloads

    def fetch(self, url: str, *, headers: Mapping[str, str] | None = None) -> bytes:
        sorted_headers = tuple(sorted((headers or {}).items()))
        req = EdgarHttpRequest(url=url, headers=sorted_headers)
        fixed = datetime(1970, 1, 1, tzinfo=timezone.utc)
        result = self.timeseries_fetch_many([req], fixed, fixed)[0]
        if isinstance(result, list):
            raise RuntimeError("unexpected list response from EdgarHttpFetcher")
        return result
