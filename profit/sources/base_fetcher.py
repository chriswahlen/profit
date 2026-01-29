from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Callable, Generic, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar

from profit.cache import CacheMissError, FileCache, OfflineModeError
from profit.sources.coverage import CoverageAdapter
from profit.sources.errors import ThrottledError, InactiveInstrumentError
from profit.sources.types import Fingerprintable, LifecycleReader

RequestT = TypeVar("RequestT", bound=Fingerprintable)
ResultT = TypeVar("ResultT")

logger = logging.getLogger(__name__)


class BaseFetcher(Generic[RequestT, ResultT], ABC):
    """
    Abstract fetcher with retry, caching, and range chunking support.

    Derived classes only need to implement `_fetch_timeseries_chunk_many` and,
    if desired, `_combine_chunks`. Everything else (retry, cache TTL, offline
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
        retry_after_cap: float = 60.0,
        retry_exceptions: Tuple[Type[BaseException], ...] = (Exception,),
        sleep_fn: Callable[[float], None] = time.sleep,
        max_batch_size: Optional[int] = None,
        lifecycle: LifecycleReader | None = None,
        catalog_checker=None,
    ) -> None:
        self.cache = cache or FileCache(ttl=ttl)
        self.ttl = ttl
        self.offline = offline
        self.max_window_days = max_window_days
        self.max_attempts = max_attempts
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff
        self.retry_after_cap = retry_after_cap
        self.retry_exceptions = retry_exceptions
        self._sleep = sleep_fn
        self.max_batch_size = max_batch_size
        if lifecycle is None:
            raise ValueError("lifecycle reader is required")
        self.lifecycle = lifecycle
        if catalog_checker is None:
            raise ValueError("catalog checker is required")
        self.catalog_checker = catalog_checker

    # Public API ---------------------------------------------------------
    def timeseries_fetch_many(
        self,
        requests: Sequence[RequestT],
        start: datetime,
        end: datetime,
        *,
        ttl: Optional[timedelta] = None,
        coverage_by_request: Optional[dict[RequestT, CoverageAdapter]] = None,
    ) -> list[ResultT | List[ResultT]]:
        """
        Fetch the same time window for many requests.

        Default implementation preserves per-request cache/coverage semantics
        and opportunistically batches network calls via an optional
        `_fetch_timeseries_chunk_many` hook implemented by subclasses. If the
        hook is absent, this simply loops over `timeseries_fetch`.
        """

        if not requests:
            return []

        start = self._normalize_ts(start)
        end = self._normalize_ts(end)
        if start > end:
            raise ValueError("start must be <= end")

        # Ensure catalog freshness once per provider (assume uniform provider across requests).
        providers = {getattr(r, "provider", None) for r in requests}
        if None in providers:
            raise ValueError("Requests must expose provider for catalog enforcement")
        for p in providers:
            self.catalog_checker.ensure_fresh(str(p))

        # Lifecycle clipping per request.
        windows_by_req: dict[RequestT, Tuple[datetime, datetime]] = {}
        skipped_errors: dict[RequestT, InactiveInstrumentError] = {}
        for req in requests:
            provider = getattr(req, "provider", None)
            provider_code = getattr(req, "provider_code", None)
            if provider is None or provider_code is None:
                raise ValueError("Request must expose provider and provider_code for lifecycle enforcement")

            try:
                self.catalog_checker.require_present(str(provider), str(provider_code))
            except Exception as exc:
                skipped_errors[req] = InactiveInstrumentError(
                    str(provider),
                    str(provider_code),
                    reason="not_in_catalog",
                    requested_start=start,
                    requested_end=end,
                    active_from=None,
                    active_to=None,
                )
                continue

            lc = self.lifecycle.get_lifecycle(str(provider), str(provider_code))
            if lc is None:
                skipped_errors[req] = InactiveInstrumentError(
                    str(provider),
                    str(provider_code),
                    reason="not_in_catalog",
                    requested_start=start,
                    requested_end=end,
                    active_from=None,
                    active_to=None,
                )
                continue
            active_from, active_to = lc
            clip_start = max(start, active_from)
            clip_end = min(end, active_to or end)
            if clip_start > clip_end:
                skipped_errors[req] = InactiveInstrumentError(
                    str(provider),
                    str(provider_code),
                    reason="outside_lifecycle",
                    requested_start=start,
                    requested_end=end,
                    active_from=active_from,
                    active_to=active_to,
                )
                continue
            windows_by_req[req] = (clip_start, clip_end)

        if not windows_by_req:
            # All requests inactive.
            # Raise the first error for clarity.
            first_err = next(iter(skipped_errors.values()))
            raise first_err

        if skipped_errors:
            for err in skipped_errors.values():
                logger.warning("%s", err)

        active_requests: list[RequestT] = list(windows_by_req.keys())

        # Resolve coverage adapters for active requests.
        coverage_by_req: dict[RequestT, CoverageAdapter | None] = {}
        has_coverage_factory = hasattr(self, "coverage_adapter")
        for req in active_requests:
            cov: CoverageAdapter | None = None
            if coverage_by_request and req in coverage_by_request:
                cov = coverage_by_request[req]
            elif has_coverage_factory:
                try:
                    cov = getattr(self, "coverage_adapter")(req)  # type: ignore[call-arg]
                except Exception:
                    cov = None
            coverage_by_req[req] = cov

        # Determine unfetched gaps per request using clipped windows.
        gaps_by_req: dict[RequestT, Sequence[Tuple[datetime, datetime]]] = {}
        for req in active_requests:
            clip_start, clip_end = windows_by_req[req]
            cov = coverage_by_req[req]
            if cov is None:
                gaps_by_req[req] = [(clip_start, clip_end)]
                continue
            gaps = cov.get_unfetched_ranges(clip_start, clip_end)
            gaps_by_req[req] = gaps

        # Build chunk jobs grouped by identical (chunk_start, chunk_end).
        jobs_by_window: dict[Tuple[datetime, datetime], list[RequestT]] = {}
        for req, gaps in gaps_by_req.items():
            if not gaps:
                continue  # fully covered
            for gap_start, gap_end in gaps:
                for chunk_start, chunk_end in self._chunk_ranges(gap_start, gap_end):
                    jobs_by_window.setdefault((chunk_start, chunk_end), []).append(req)

        # Collect fetched chunks per request.
        chunks_by_req: dict[RequestT, list[ResultT]] = {req: [] for req in requests}

        for (chunk_start, chunk_end), reqs in jobs_by_window.items():
            # First, satisfy cache hits per-request.
            pending: list[RequestT] = []
            for req in reqs:
                cache_key = self._fingerprint(req, chunk_start, chunk_end)
                try:
                    entry = self.cache.get(cache_key, ttl=ttl)
                    logger.info("cache hit key=%s", cache_key)
                    chunks_by_req[req].append(entry.value)
                    cov = coverage_by_req.get(req)
                    if cov:
                        cov.write_points(entry.value)
                    continue
                except CacheMissError:
                    logger.info("cache miss key=%s", cache_key)
                    if self.offline:
                        raise OfflineModeError(f"Offline mode enabled and cache miss for {cache_key}")
                pending.append(req)

            if not pending:
                continue

            # Subclasses are expected to implement the batch hook.
            batch_size = self.max_batch_size or len(pending)
            for i in range(0, len(pending), batch_size):
                slice_reqs = pending[i : i + batch_size]
                logger.info(
                    "network request batch_size=%d start=%s end=%s",
                    len(slice_reqs),
                    chunk_start.isoformat(),
                    chunk_end.isoformat(),
                )

                def _call():
                    return self._fetch_timeseries_chunk_many(slice_reqs, chunk_start, chunk_end)

                result_map = self._with_retries(_call)
                if not isinstance(result_map, dict):
                    raise TypeError("_fetch_timeseries_chunk_many must return a mapping of request -> result")

                for req in slice_reqs:
                    data = result_map.get(req, [])
                    cache_key = self._fingerprint(req, chunk_start, chunk_end)
                    self.cache.set(cache_key, data)
                    cov = coverage_by_req.get(req)
                    if cov:
                        cov.write_points(data)
                    chunks_by_req[req].append(data)

        # Finalize per-request results honoring coverage when available.
        out: list[ResultT | List[ResultT]] = []
        for req in requests:
            cov = coverage_by_req.get(req)
            if req in windows_by_req:
                clip_start, clip_end = windows_by_req[req]
            else:
                clip_start, clip_end = start, end
            if cov:
                out.append(cov.read_points(clip_start, clip_end))
            else:
                out.append(self._combine_chunks(chunks_by_req.get(req, [])))
        return out

    # Hooks for subclasses -----------------------------------------------
    @abstractmethod
    def _fetch_timeseries_chunk_many(
        self, requests: Sequence[RequestT], start: datetime, end: datetime
    ) -> dict[RequestT, ResultT | List[ResultT]]:
        """
        Provider-specific batch fetch for the inclusive [start, end] window.

        Must return a mapping from each request to its result payload.
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
            except ThrottledError as exc:
                if attempt >= self.max_attempts:
                    raise
                retry_after = exc.retry_after
                computed = self.backoff_factor * 2 ** (attempt - 1)
                sleep_for = min(
                    self.retry_after_cap,
                    retry_after if retry_after is not None else computed,
                )
                self._sleep(min(self.max_backoff, sleep_for))
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
