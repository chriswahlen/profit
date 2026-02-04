from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


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


@dataclass(frozen=True)
class InstrumentRecord:
    """
    Provider-neutral instrument metadata, mapped to a provider code.
    """

    instrument_id: str
    instrument_type: str
    provider: str
    provider_code: str
    mic: str | None
    currency: str | None
    active_from: datetime | None
    active_to: datetime | None
    attrs: dict[str, Any]


@dataclass(frozen=True)
class EntityRecord:
    """
    Provider-neutral entity metadata (companies, commodities, crypto, etc.).
    """

    entity_id: str
    entity_type: str
    name: str
    country_iso2: str | None = None
    status: str = "active"
    attrs: dict[str, Any] | None = None


@dataclass(frozen=True)
class EntityIdentifierRecord:
    """
    Identifier mapping between an entity and a scheme (provider codes, ISIN, FIGI, ticker+mic).
    """

    entity_id: str
    scheme: str
    value: str
    provider_id: str | None = None
    active_from: datetime | None = None
    active_to: datetime | None = None
    last_seen: datetime | None = None


@dataclass(frozen=True)
class DiscoverableFetcher(Protocol):
    """
    Optional mixin for fetchers that can describe their capabilities.
    """

    def describe(self) -> FetcherDescription:
        ...
