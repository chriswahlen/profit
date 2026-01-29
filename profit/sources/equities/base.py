from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence

from profit.sources.base_fetcher import BaseFetcher
from profit.sources.types import Fingerprintable


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


@dataclass(frozen=True)
class EquityDailyBarsRequest(Fingerprintable):
    """
    Provider-neutral request for daily equity bars.

    `instrument_id` is a deterministic internal identifier; for equities we
    currently recommend `TICKER|MIC` (e.g., `AAPL|XNAS`).

    `provider_code` is the provider-specific symbol/code for fetching (e.g.,
    yfinance's ticker like `AAPL` or `BRK-B`). It is explicitly included in the
    fingerprint to make cache keys stable even when instrument mapping changes.
    """

    instrument_id: str
    provider: str
    provider_code: str
    freq: str = "1d"

    def fingerprint(self) -> str:  # pragma: no cover - trivial
        return f"equity_bars:{self.provider}:{self.freq}:{self.instrument_id}:{self.provider_code}"


@dataclass(frozen=True)
class EquityDailyBar:
    """
    Provider-neutral daily OHLCV bar with raw + adjusted variants.

    All timestamps are UTC. For daily data we interpret `ts_utc` as the bar's
    session date bucket (00:00:00Z) unless a provider supplies a more precise
    convention.
    """

    instrument_id: str
    ts_utc: datetime
    open_raw: float
    high_raw: float
    low_raw: float
    close_raw: float
    volume_raw: float
    open_adj: float
    high_adj: float
    low_adj: float
    close_adj: float
    volume_adj: float
    source: str
    version: str
    asof: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts_utc", _to_utc(self.ts_utc))
        object.__setattr__(self, "asof", _to_utc(self.asof))


class EquitiesDailyFetcher(BaseFetcher[EquityDailyBarsRequest, list[EquityDailyBar]]):
    """
    Base class for daily equity bar providers.

    Subclasses implement `_fetch_timeseries_chunk` and return a list of
    `EquityDailyBar` entries for the inclusive [start, end] window.
    """

    def _combine_chunks(
        self, chunks: Sequence[list[EquityDailyBar]]
    ) -> list[EquityDailyBar]:
        if not chunks:
            return []

        merged: list[EquityDailyBar] = []
        for chunk in chunks:
            merged.extend(chunk)

        # Deterministic ordering and de-dup by the natural bar key.
        merged.sort(key=lambda b: (b.instrument_id, b.ts_utc, b.source, b.version))
        out: list[EquityDailyBar] = []
        last_key: tuple[str, datetime, str, str] | None = None
        for bar in merged:
            key = (bar.instrument_id, bar.ts_utc, bar.source, bar.version)
            if key == last_key:
                continue
            out.append(bar)
            last_key = key
        return out

    @staticmethod
    def validate_daily_bars(bars: Iterable[EquityDailyBar]) -> None:
        """
        Basic boundary validation for provider output.
        """
        for bar in bars:
            if bar.ts_utc.tzinfo is None or bar.ts_utc.utcoffset() != timezone.utc.utcoffset(bar.ts_utc):
                raise ValueError("bar.ts_utc must be timezone-aware UTC")

