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
class FxRequest(Fingerprintable):
    """
    Provider-neutral FX request.

    `provider_code` carries the exact symbol used by the provider (e.g., `EURUSD=X` for yfinance).
    """

    base_ccy: str
    quote_ccy: str
    provider: str
    provider_code: str
    freq: str = "1d"

    def fingerprint(self) -> str:  # pragma: no cover - trivial
        pair = f"{self.base_ccy}{self.quote_ccy}"
        return f"fx:{self.provider}:{self.freq}:{pair}:{self.provider_code}"


@dataclass(frozen=True)
class FxRatePoint:
    base_ccy: str
    quote_ccy: str
    ts_utc: datetime
    rate: float
    source: str
    version: str
    asof: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts_utc", _to_utc(self.ts_utc))
        object.__setattr__(self, "asof", _to_utc(self.asof))


class FxDailyFetcher(BaseFetcher[FxRequest, list[FxRatePoint]]):
    """
    Base class for daily FX rates fetchers.
    """

    def _combine_chunks(self, chunks: Sequence[list[FxRatePoint]]) -> list[FxRatePoint]:
        if not chunks:
            return []
        merged: list[FxRatePoint] = []
        for chunk in chunks:
            merged.extend(chunk)
        merged.sort(key=lambda r: (r.base_ccy, r.quote_ccy, r.ts_utc, r.source, r.version))
        out: list[FxRatePoint] = []
        last_key: tuple[str, str, datetime, str, str] | None = None
        for r in merged:
            key = (r.base_ccy, r.quote_ccy, r.ts_utc, r.source, r.version)
            if key == last_key:
                continue
            out.append(r)
            last_key = key
        return out

    @staticmethod
    def validate_points(points: Iterable[FxRatePoint]) -> None:
        for p in points:
            if p.ts_utc.tzinfo is None or p.ts_utc.utcoffset() != timezone.utc.utcoffset(p.ts_utc):
                raise ValueError("ts_utc must be timezone-aware UTC")
