from __future__ import annotations

"""YFinance OHLCV fetcher.

This fetcher batches ticker requests through ``yfinance.download`` while
honoring the repository's caching/backoff conventions. It normalizes returned
timestamps to UTC, lowercases column names, and pads the end window by one day
to account for yfinance's exclusive ``end`` handling.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Mapping, Sequence

import pandas as pd

from profit.cache import FileCache
from profit.catalog.types import DiscoverableFetcher, FetcherDescription
from profit.config import ProfitConfig
from profit.sources.base_fetcher import BaseFetcher
from profit.sources.errors import ThrottledError
from profit.sources.types import Fingerprintable

logger = logging.getLogger(__name__)


PROVIDER = "yfinance"
DATASET = "yfinance_bar_ohlcv"
VERSION = "v1"
SUPPORTED_INTERVALS = {"1d"}
FIELD_ORDER = ["open", "high", "low", "close", "adj_close", "volume"]

DownloadFn = Callable[[list[str], datetime, datetime, str], pd.DataFrame]


def _default_download(tickers: list[str], start: datetime, end: datetime, interval: str) -> pd.DataFrame:
    """
    Thin wrapper around ``yfinance.download`` with options tuned for batch use.

    We import yfinance lazily to keep it optional for environments that only
    run tests with a stubbed ``download_fn``.
    """

    import yfinance as yf  # type: ignore

    return yf.download(
        tickers=tickers,
        start=start,
        end=end,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        actions=False,
        progress=False,
        threads=True,
    )


@dataclass(frozen=True)
class YFinanceRequest(Fingerprintable):
    """Single ticker/interval request for yfinance OHLCV data."""

    ticker: str
    interval: str = "1d"
    provider: str = PROVIDER
    provider_code: str | None = None

    def __post_init__(self) -> None:
        ticker = (self.ticker or "").strip().upper()
        if not ticker:
            raise ValueError("ticker is required")
        object.__setattr__(self, "ticker", ticker)

        interval = (self.interval or "").strip()
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(f"interval '{interval}' not supported; choices: {sorted(SUPPORTED_INTERVALS)}")
        object.__setattr__(self, "interval", interval)

        if self.provider_code is None:
            object.__setattr__(self, "provider_code", ticker)

    def fingerprint(self) -> str:  # pragma: no cover - trivial
        return f"{self.provider}:{self.interval}:{self.ticker}"


class YFinanceFetcher(BaseFetcher[YFinanceRequest, pd.DataFrame], DiscoverableFetcher):
    """
    Fetch OHLCV bars from yfinance with caching, retry, and UTC normalization.

    The fetcher pads the requested end window by one calendar day because
    ``yfinance`` treats the ``end`` parameter as exclusive for daily bars.
    """

    def __init__(
        self,
        *,
        cfg: ProfitConfig,
        cache: FileCache,
        ttl: timedelta = timedelta(days=1),
        offline: bool = False,
        max_attempts: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 5.0,
        lifecycle,
        catalog_checker,
        download_fn: DownloadFn | None = None,
    ) -> None:
        super().__init__(
            cfg=cfg,
            cache=cache,
            ttl=ttl,
            offline=offline,
            max_window_days=365,
            max_attempts=max_attempts,
            backoff_factor=backoff_factor,
            max_backoff=max_backoff,
            max_batch_size=50,
            batch_pause_s=0.5,
            lifecycle=lifecycle,
            catalog_checker=catalog_checker,
        )
        self.download_fn = download_fn or _default_download

    # DiscoverableFetcher -------------------------------------------------
    def describe(self) -> FetcherDescription:
        return FetcherDescription(
            provider=PROVIDER,
            dataset=DATASET,
            version=VERSION,
            freqs=["1d"],
            fields=FIELD_ORDER,
            max_window_days=self.max_window_days,
            notes="Daily OHLCV via yfinance.download; timestamps normalized to UTC.",
        )

    # BaseFetcher hooks ---------------------------------------------------
    def _fetch_timeseries_chunk_many(
        self, requests: Sequence[YFinanceRequest], start: datetime, end: datetime
    ) -> dict[YFinanceRequest, pd.DataFrame]:
        if not requests:
            return {}

        interval = self._ensure_single_interval(requests)
        tickers = [req.ticker for req in requests]

        # yfinance's ``end`` is exclusive; pad by one day so the inclusive
        # window requested by BaseFetcher is preserved after filtering.
        fetch_start = start.astimezone(timezone.utc).replace(tzinfo=None)
        fetch_end = (end + timedelta(days=1)).astimezone(timezone.utc).replace(tzinfo=None)

        logger.info(
            "yfinance request provider=%s tickers=%s start=%s end=%s interval=%s",
            PROVIDER,
            ",".join(tickers),
            start.isoformat(),
            end.isoformat(),
            interval,
        )

        try:
            raw = self.download_fn(tickers, fetch_start, fetch_end, interval)
        except Exception as exc:
            retry_after = _extract_retry_after(exc)
            if retry_after is not None:
                raise ThrottledError("yfinance throttled", retry_after=retry_after) from exc
            raise

        frames = self._split_frames(raw, tickers, start, end)

        results: dict[YFinanceRequest, pd.DataFrame] = {}
        for req in requests:
            frame = frames.get(req.ticker, self._empty_frame())
            points = len(frame.index)
            logger.info(
                "yfinance fetched provider=%s ticker=%s start=%s end=%s interval=%s points=%s",
                PROVIDER,
                req.ticker,
                start.isoformat(),
                end.isoformat(),
                interval,
                points,
            )
            results[req] = frame
        return results

    # Helpers -------------------------------------------------------------
    def _ensure_single_interval(self, requests: Sequence[YFinanceRequest]) -> str:
        intervals = {req.interval for req in requests}
        if len(intervals) != 1:
            raise ValueError(f"mixed intervals not supported: {sorted(intervals)}")
        return intervals.pop()

    def _split_frames(
        self, raw: pd.DataFrame, tickers: list[str], start: datetime, end: datetime
    ) -> dict[str, pd.DataFrame]:
        if raw is None or raw.empty:
            return {ticker: self._empty_frame() for ticker in tickers}

        frames: dict[str, pd.DataFrame] = {}

        if isinstance(raw.columns, pd.MultiIndex):
            # Expect level 0 = ticker, level 1 = field when group_by="ticker".
            for ticker in tickers:
                if ticker in raw.columns.get_level_values(0):
                    sub = raw.xs(ticker, axis=1, level=0, drop_level=True)
                else:
                    sub = pd.DataFrame(index=raw.index)
                frames[ticker] = self._normalize_frame(sub, start, end)
        else:
            # Single ticker result.
            frames[tickers[0]] = self._normalize_frame(raw, start, end)

        return frames

    def _normalize_frame(self, frame: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
        if frame is None or frame.empty:
            return self._empty_frame()

        renamed = frame.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
        )

        if not isinstance(renamed.index, pd.DatetimeIndex):
            raise ValueError("yfinance response index must be datetime")

        idx = renamed.index
        if idx.tz is None:
            idx = idx.tz_localize(timezone.utc)
        else:
            idx = idx.tz_convert(timezone.utc)
        renamed.index = idx

        # Filter to inclusive window after timezone normalization.
        windowed = renamed.sort_index()
        windowed = windowed[(windowed.index >= start) & (windowed.index <= end)]

        for field in FIELD_ORDER:
            if field not in windowed.columns:
                windowed[field] = pd.NA

        return windowed[FIELD_ORDER]

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        return pd.DataFrame(columns=FIELD_ORDER, index=pd.DatetimeIndex([], tz=timezone.utc))


def _extract_retry_after(exc: Exception) -> float | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None) if response is not None else None
    if status == 429:
        retry_val = None
        headers: Mapping[str, str] | None = getattr(response, "headers", None)
        if headers is not None:
            retry_val = headers.get("Retry-After")
        try:
            return float(retry_val) if retry_val is not None else None
        except (TypeError, ValueError):
            return None
    return None
