from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Callable, Generic, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar

from profit.cache import CacheMissError, FileCache, OfflineModeError
from profit.sources.types import Fingerprintable

RequestT = TypeVar("RequestT", bound=Fingerprintable)
ResultT = TypeVar("ResultT")


class BaseFetcher(Generic[RequestT, ResultT], ABC):
    """
    Abstract fetcher with retry, caching, and range chunking support.

    Derived classes only need to implement `_fetch_timeseries_chunk` and, if
    desired, `_combine_chunks`. Everything else (retry, cache TTL, offline
    behavior) is handled here.
    """

    def __init__(
        self,
        *,
        cache: Optional[FileCache] = None,
        ttl: timedelta = timedelta(days=30),
        offline: bool = False,
        max_window_days: Optional[int] = None,
        max_attempts: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 5.0,
        retry_exceptions: Tuple[Type[BaseException], ...] = (Exception,),
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.cache = cache or FileCache(ttl=ttl)
        self.ttl = ttl
        self.offline = offline
        self.max_window_days = max_window_days
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff
        self.retry_exceptions = retry_exceptions
        self._sleep = sleep_fn

    # Public API ---------------------------------------------------------
    def timeseries_fetch(
        self,
        request: RequestT,
        start: datetime,
        end: datetime,
        *,
        ttl: Optional[timedelta] = None,
    ) -> ResultT | List[ResultT]:
        """
        Fetch data for the requested time span, splitting into chunks when
        necessary and merging the results.
        """
        start = self._normalize_ts(start)
        end = self._normalize_ts(end)
        if start > end:
            raise ValueError("start must be <= end")

        chunks: List[ResultT] = []
        for chunk_start, chunk_end in self._chunk_ranges(start, end):
            cache_key = self._fingerprint(request, chunk_start, chunk_end)
            try:
                entry = self.cache.get(cache_key, ttl=ttl)
                chunks.append(entry.value)
                continue
            except CacheMissError:
                if self.offline:
                    raise OfflineModeError(
                        f"Offline mode enabled and cache miss for {cache_key}"
                    )

            def _call() -> ResultT:
                return self._fetch_timeseries_chunk(request, chunk_start, chunk_end)

            result = self._with_retries(_call)
            self.cache.set(cache_key, result)
            chunks.append(result)

        return self._combine_chunks(chunks)

    # Hooks for subclasses -----------------------------------------------
    @abstractmethod
    def _fetch_timeseries_chunk(
        self, request: RequestT, start: datetime, end: datetime
    ) -> ResultT:
        """
        Perform the provider-specific fetch for a single time window.
        Implementations should raise exceptions to trigger retry semantics.
        """

    def _combine_chunks(self, chunks: Sequence[ResultT]) -> ResultT | List[ResultT]:
        """
        Default chunk combiner.

        - If only one chunk exists, return it.
        - If pandas is available and all chunks are DataFrames, concatenate and
          sort by index.
        - Otherwise return the list of chunks so callers can decide how to merge.
        """
        if not chunks:
            return []  # type: ignore[return-value]
        if len(chunks) == 1:
            return chunks[0]

        try:
            import pandas as pd  # type: ignore

            if all(hasattr(chunk, "__class__") and isinstance(chunk, pd.DataFrame) for chunk in chunks):
                combined = pd.concat(list(chunks))
                try:
                    return combined.sort_index()
                except Exception:
                    return combined
        except ModuleNotFoundError:
            pass

        return list(chunks)

    # Internal helpers ---------------------------------------------------
    def _fingerprint(self, request: RequestT, start: datetime, end: datetime) -> str:
        start_str = start.isoformat()
        end_str = end.isoformat()
        return f"{request.fingerprint()}|{start_str}|{end_str}"

    def _chunk_ranges(self, start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
        """
        Yield (start, end) windows respecting max_window_days.
        """
        if not self.max_window_days or self.max_window_days <= 0:
            yield start, end
            return

        cursor = start
        step = timedelta(days=self.max_window_days)
        one_day = timedelta(days=1)

        while cursor <= end:
            chunk_end = min(end, cursor + step - one_day)
            yield cursor, chunk_end
            cursor = chunk_end + one_day

    def _with_retries(self, fn: Callable[[], ResultT]) -> ResultT:
        attempt = 0
        while True:
            attempt += 1
            try:
                return fn()
            except self.retry_exceptions as exc:  # type: ignore[misc]
                if attempt >= self.max_attempts:
                    raise
                backoff = min(self.max_backoff, self.backoff_factor * 2 ** (attempt - 1))
                self._sleep(backoff)
            except Exception:
                # Do not retry for unexpected exception types.
                raise

    @staticmethod
    def _normalize_ts(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
