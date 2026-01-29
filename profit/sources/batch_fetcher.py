from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional, TypeVar

from profit.cache import CacheMissError, FileCache, OfflineModeError
from profit.sources.base_fetcher import BaseFetcher, RequestT, ResultT

logger = logging.getLogger(__name__)


class BatchFetcher(BaseFetcher[RequestT, ResultT]):
    """
    Fetcher for providers that deliver data via a single bulk download.

    The bulk payload is cached under the request fingerprint and reused until it
    expires. Derived classes only need to implement `_download_bulk`.
    """

    def __init__(
        self,
        *,
        cache: Optional[FileCache] = None,
        ttl: timedelta = timedelta(days=30),
        offline: bool = False,
        max_attempts: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 5.0,
        lifecycle=None,
    ) -> None:
        super().__init__(
            cache=cache,
            ttl=ttl,
            offline=offline,
            max_window_days=None,
            max_attempts=max_attempts,
            backoff_factor=backoff_factor,
            max_backoff=max_backoff,
            lifecycle=lifecycle,
        )

    def fetch(self, request: RequestT, *, ttl: Optional[timedelta] = None) -> ResultT:
        cache_key = request.fingerprint()
        try:
            entry = self.cache.get(cache_key, ttl=ttl)
            logger.info("cache hit key=%s", cache_key)
            return entry.value
        except CacheMissError:
            logger.info("cache miss key=%s", cache_key)
            if self.offline:
                raise OfflineModeError(
                    f"Offline mode enabled and cache miss for {cache_key}"
                )

        def _call():
            logger.info("network request fingerprint=%s", request.fingerprint())
            return self._download_bulk(request)

        result = self._with_retries(_call)
        self.cache.set(cache_key, result)
        return result

    def _fetch_timeseries_chunk_many(self, requests, start, end):  # type: ignore[override]
        raise NotImplementedError("BatchFetcher does not support timeseries batch timeseries_fetch_many")

    def _combine_chunks(self, chunks):  # type: ignore[override]
        return chunks

    def _download_bulk(self, request: RequestT) -> ResultT:
        """
        Provider-specific bulk download.
        """
        raise NotImplementedError
