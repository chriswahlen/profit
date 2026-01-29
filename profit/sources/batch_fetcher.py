from __future__ import annotations

from datetime import timedelta
from typing import Optional, TypeVar

from profit.cache import CacheMissError, FileCache, OfflineModeError
from profit.sources.base_fetcher import BaseFetcher, RequestT, ResultT


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
    ) -> None:
        super().__init__(
            cache=cache,
            ttl=ttl,
            offline=offline,
            max_window_days=None,
            max_attempts=max_attempts,
            backoff_factor=backoff_factor,
            max_backoff=max_backoff,
        )

    def fetch(self, request: RequestT, *, ttl: Optional[timedelta] = None) -> ResultT:
        cache_key = request.fingerprint()
        try:
            return self.cache.get(cache_key, ttl=ttl).value
        except CacheMissError:
            if self.offline:
                raise OfflineModeError(
                    f"Offline mode enabled and cache miss for {cache_key}"
                )

        result = self._with_retries(lambda: self._download_bulk(request))
        self.cache.set(cache_key, result)
        return result

    def _fetch_timeseries_chunk(self, request: RequestT, start, end) -> ResultT:  # type: ignore[override]
        raise NotImplementedError("BatchFetcher does not support timeseries chunks")

    def _combine_chunks(self, chunks):  # type: ignore[override]
        return chunks

    def _download_bulk(self, request: RequestT) -> ResultT:
        """
        Provider-specific bulk download.
        """
        raise NotImplementedError
