from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Mapping, Protocol
from urllib import request


@dataclass(frozen=True)
class FetchResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


class FetchFn(Protocol):
    def __call__(self, url: str, *, timeout: float, headers: Mapping[str, str]) -> FetchResponse: ...


class FetchError(RuntimeError):
    def __init__(self, url: str, status: int, message: str | None = None):
        super().__init__(message or f"fetch failed status={status} url={url}")
        self.url = url
        self.status = status


class TemporaryFetchError(FetchError):
    pass


class PermanentFetchError(FetchError):
    pass


def default_fetch(url: str, *, timeout: float, headers: Mapping[str, str]) -> FetchResponse:
    req = request.Request(url, headers=dict(headers))
    with request.urlopen(req, timeout=timeout) as resp:  # nosec B310
        return FetchResponse(status=int(resp.status), body=resp.read(), headers=dict(resp.headers))


def fetch_url(
    url: str,
    *,
    timeout: float = 30.0,
    headers: Mapping[str, str] | None = None,
    fetch_fn: FetchFn | None = None,
) -> bytes:
    fn = fetch_fn or default_fetch
    resp = fn(url, timeout=timeout, headers=headers or {})
    if resp.status == 200:
        return resp.body
    if resp.status in {408, 425, 429, 500, 502, 503, 504}:
        raise TemporaryFetchError(url, resp.status)
    raise PermanentFetchError(url, resp.status)


def fetch_with_retry(
    url: str,
    *,
    headers: Mapping[str, str],
    fetch_fn: FetchFn | None = None,
    timeout: float = 30.0,
    max_attempts: int = 10,
    backoff_initial_s: float = 0.5,
    backoff_max_s: float = 30.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> bytes:
    attempts = 0
    while True:
        attempts += 1
        try:
            return fetch_url(url, timeout=timeout, headers=headers, fetch_fn=fetch_fn)
        except TemporaryFetchError:
            if attempts >= max_attempts:
                raise
            delay = min(backoff_initial_s * (2 ** (attempts - 1)), backoff_max_s)
            sleep_fn(delay)

