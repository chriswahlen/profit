from __future__ import annotations

import logging
from typing import Iterable, List, Protocol, Sequence

from config import Config
from data_sources.data_source import DataSource, DataSourceUpdateResults
from data_sources.entity import EntityStore
from data_sources.market.market_data_store import Candle, MarketDataStore


class CandleProvider(Protocol):
    """Minimal provider interface for fetching OHLCV candles."""

    name: str

    def fetch(self, entity_ids: Iterable[str]) -> Sequence[Candle]:
        ...


class MarketDataSource(DataSource):
    """Coordinates provider pulls into the MarketDataStore."""

    def __init__(self, config: Config, entity_store: EntityStore, providers: List[CandleProvider]):
        super().__init__(name="market", summary="OHLCV bars from multiple providers", config=config, entity_store=entity_store)
        self.config = config
        self.entity_store = entity_store
        self.providers = providers
        self.store = MarketDataStore(config)
        self.logger = logging.getLogger(__name__)

    def describe_detailed(self, *, indent: str = "  ") -> str:
        lines = [f"{indent}Storage: sqlite at {self.store.db_path}"]
        lines.append(self.store.describe_detailed(indent=indent))
        return "\n".join(lines)

    def ensure_up_to_date(self, entity_ids: Iterable[str]) -> DataSourceUpdateResults:
        entity_ids = list(entity_ids)
        if not entity_ids:
            return DataSourceUpdateResults(detail="no entity_ids supplied")

        total_updated = total_failed = 0
        for provider in self.providers:
            try:
                candles = provider.fetch(entity_ids)
                res = self.store.upsert_candles_raw(candles)
                total_updated += res.updated
                total_failed += res.failed
                self.logger.info("Provider %s upserted %d candles (failed=%d)", provider.name, res.updated, res.failed)
            except Exception:  # noqa: BLE001 - log and continue
                total_failed += 1
                self.logger.exception("Provider %s fetch failed", provider.name)
        return DataSourceUpdateResults(updated=total_updated, failed=total_failed)
