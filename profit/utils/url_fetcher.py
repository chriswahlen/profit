from __future__ import annotations

"""Lightweight HTTP/FTP fetcher with disk caching and error classification.

The helper caches both successful payloads and *permanent* HTTP errors (4xx
except 429) so callers avoid repeatedly hitting endpoints that will not
recover. Temporary errors (429/5xx) are **not** cached.

This module keeps a tiny API surface on purpose: `fetch_url` is the only entry
point and takes a URL plus a `FileCache` instance. Callers may optionally
provide a custom fetch function (useful for tests) and headers. The default TTL
is seven days, aligning with the repository guidelines for deterministic and
reproducible behavior.
"""

import gzip
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from profit.cache import CacheMissError, FileCache, OfflineModeError

logger = logging.getLogger(__name__)


DEFAULT_TTL = timedelta(days=7)


@dataclass(frozen=True)
class FetchResponse:
    """Normalized network response returned by a fetch function."""

    status: int
    body: bytes
    headers: Mapping[str, str]


class FetchFn(Protocol):
    def __call__(self, url: str, *, timeout: float, headers: Mapping[str, str] | None) -> FetchResponse:
        ...


@dataclass(frozen=True)
class _CachedEntry:
    """Value stored in FileCache for a fetch result (success or error)."""

    status: int
    body: bytes
    headers: dict[str, str]
    permanent_error: bool


class PermanentFetchError(RuntimeError):
    def __init__(self, url: str, status: int | None, message: str, *, cached: bool = False) -> None:
        self.url = url
        self.status = status
        self.cached = cached
        super().__init__(message)


class TemporaryFetchError(RuntimeError):
    def __init__(self, url: str, status: int | None, message: str) -> None:
        self.url = url
        self.status = status
        super().__init__(message)


def fetch_url(
    url: str,
    *,
    cache: FileCache,
    ttl: timedelta = DEFAULT_TTL,
    allow_network: bool = True,
    timeout: float = 30.0,
    headers: Mapping[str, str] | None = None,
    fetch_fn: FetchFn | None = None,
) -> bytes:
    """Fetch a URL via HTTP or FTP with disk caching and error bucketing.

    Successful responses and permanent errors (HTTP 4xx except 429) are cached
    for ``ttl``. Temporary errors (429, 5xx) are not cached so callers can
    retry with backoff.
    """

    if not url:
        raise ValueError("url is required")

    fetch_fn = fetch_fn or _default_fetch
    cache_key = f"urlfetch::{url}"

    try:
        cached = cache.get(cache_key, ttl=ttl).value
        if cached.permanent_error:
            raise PermanentFetchError(
                url,
                cached.status,
                message=f"cached permanent error status={cached.status} url={url}",
                cached=True,
            )
        payload = _decompress_payload(cached.body)
        logger.info("urlfetch cache hit url=%s status=%s size=%s", url, cached.status, len(payload))
        return payload
    except CacheMissError:
        logger.info("urlfetch cache miss url=%s", url)
        pass

    if not allow_network:
        raise OfflineModeError(f"offline mode prevents fetch for {url}")

    logger.info("urlfetch network request url=%s timeout=%s", url, timeout)
    try:
        resp = fetch_fn(url, timeout=timeout, headers=headers)
    except PermanentFetchError:
        # Preserve already-classified permanent errors from custom fetchers.
        raise
    except TemporaryFetchError:
        # Preserve already-classified temporary errors from custom fetchers.
        raise
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("urlfetch unexpected exception url=%s exc=%s", url, exc)
        raise TemporaryFetchError(url, None, f"unexpected fetch failure: {exc}") from exc

    response_size = len(resp.body)
    logger.info("urlfetch network response url=%s status=%s size=%s", url, resp.status, response_size)

    if _is_temporary_status(resp.status):
        raise TemporaryFetchError(url, resp.status, f"temporary HTTP status {resp.status} for {url}")

    if resp.status >= 400:
        entry = _CachedEntry(
            status=resp.status,
            body=_compress_payload(resp.body),
            headers=dict(resp.headers),
            permanent_error=True,
        )
        try:
            cache.set(cache_key, entry)
        except Exception:
            logger.debug("urlfetch failed to cache permanent error url=%s", url)
        raise PermanentFetchError(url, resp.status, f"permanent HTTP status {resp.status} for {url}")

    entry = _CachedEntry(status=resp.status, body=_compress_payload(resp.body), headers=dict(resp.headers), permanent_error=False)
    try:
        cache.set(cache_key, entry)
    except Exception:
        logger.debug("urlfetch failed to cache success url=%s", url)
    return resp.body


def _default_fetch(url: str, *, timeout: float, headers: Mapping[str, str] | None) -> FetchResponse:
    """Default HTTP/FTP fetcher using urllib.

    Returns a ``FetchResponse`` even for HTTP errors so the caller can decide
    whether to cache or retry.
    """

    hdrs = dict(headers or {})
    req = Request(url, headers=hdrs)
    try:
        with urlopen(req, timeout=timeout) as resp:  # type: ignore[call-arg]
            status = getattr(resp, "status", None) or resp.getcode() or 200
            body = resp.read()
            return FetchResponse(status=status, body=body, headers=dict(resp.headers.items()))
    except HTTPError as exc:
        body = exc.read() or b""
        status = getattr(exc, "code", None) or 0
        return FetchResponse(status=status, body=body, headers=dict(exc.headers.items() if exc.headers else {}))
    except URLError as exc:
        raise TemporaryFetchError(url, None, f"network unavailable for {url}: {exc}") from exc


def _is_temporary_status(status: int) -> bool:
    if status == 429:
        return True
    if status >= 500:
        return True
    return False


def _compress_payload(payload: bytes) -> bytes:
    return gzip.compress(payload)


def _decompress_payload(payload: bytes) -> bytes:
    try:
        return gzip.decompress(payload)
    except OSError:
        return payload
    if _is_temporary_status(resp.status):
        raise TemporaryFetchError(url, resp.status, f"temporary HTTP status {resp.status} for {url}")
