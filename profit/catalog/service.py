from __future__ import annotations

from dataclasses import asdict
from typing import Iterable, Sequence

from profit.catalog.types import DiscoverableFetcher, FetcherDescription, InstrumentRecord
from profit.catalog.store import CatalogStore


class CatalogService:
    """
    Read-only facade over CatalogStore plus fetcher capability descriptors.
    """

    def __init__(self, store: CatalogStore) -> None:
        self.store = store

    # Instrument queries -------------------------------------------------
    def search_instruments(
        self,
        *,
        query: str | None = None,
        provider: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[InstrumentRecord]:
        return self.store.search_instruments(query=query, provider=provider, limit=limit, offset=offset)

    def get_instrument(self, provider: str, provider_code: str) -> InstrumentRecord | None:
        return self.store.get_instrument(provider, provider_code)

    # Fetcher descriptors -----------------------------------------------
    @staticmethod
    def describe_fetcher(fetcher: DiscoverableFetcher) -> FetcherDescription:
        return fetcher.describe()

    @staticmethod
    def describe_all(fetchers: Sequence[DiscoverableFetcher]) -> list[FetcherDescription]:
        return [f.describe() for f in fetchers]
