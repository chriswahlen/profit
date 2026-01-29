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
class CommodityDailyRequest(Fingerprintable):
    """
    Provider-neutral daily commodity price request.

    instrument_id: provider-neutral ID (e.g., XAU|LBMA, XAG|LBMA).
    provider_code: provider-specific symbol (e.g., GOLD, SILVER for Alpha Vantage).
    """

    instrument_id: str
    provider: str
    provider_code: str
    freq: str = "1d"

    def fingerprint(self) -> str:  # pragma: no cover - trivial
        return f"commodity:{self.provider}:{self.freq}:{self.instrument_id}:{self.provider_code}"


@dataclass(frozen=True)
class CommodityDailyPrice:
    instrument_id: str
    ts_utc: datetime
    price: float
    currency: str
    source: str
    version: str
    asof: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts_utc", _to_utc(self.ts_utc))
        object.__setattr__(self, "asof", _to_utc(self.asof))


class CommoditiesDailyFetcher(BaseFetcher[CommodityDailyRequest, list[CommodityDailyPrice]]):
    """
    Base class for daily commodity price providers.
    """

    def _combine_chunks(
        self, chunks: Sequence[list[CommodityDailyPrice]]
    ) -> list[CommodityDailyPrice]:
        if not chunks:
            return []
        merged: list[CommodityDailyPrice] = []
        for chunk in chunks:
            merged.extend(chunk)

        merged.sort(key=lambda r: (r.instrument_id, r.ts_utc, r.source, r.version))
        out: list[CommodityDailyPrice] = []
        last_key: tuple[str, datetime, str, str] | None = None
        for r in merged:
            key = (r.instrument_id, r.ts_utc, r.source, r.version)
            if key == last_key:
                continue
            out.append(r)
            last_key = key
        return out

    @staticmethod
    def validate_points(points: Iterable[CommodityDailyPrice]) -> None:
        for p in points:
            if p.ts_utc.tzinfo is None or p.ts_utc.utcoffset() != timezone.utc.utcoffset(p.ts_utc):
                raise ValueError("ts_utc must be timezone-aware UTC")
