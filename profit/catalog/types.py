from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class FetcherDescription:
    """
    Static capability surface for a fetcher/provider combo.
    """

    provider: str
    dataset: str  # e.g., equity_ohlcv, fx_rate, commodity_price
    version: str
    freqs: list[str]
    fields: list[str]
    max_window_days: int | None = None
    notes: str | None = None


class DiscoverableFetcher(Protocol):
    """
    Optional mixin for fetchers that can describe their capabilities.
    """

    def describe(self) -> FetcherDescription:
        ...
