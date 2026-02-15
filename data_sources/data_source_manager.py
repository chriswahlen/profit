
from __future__ import annotations

from typing import Dict

from config import Config
from data_sources.data_source import DataSource
from data_sources.entity import EntityStore
from data_sources.redfin.redfin_data_source import RedfinDataSource
from data_sources.market.market_data_source import MarketDataSource
from data_sources.market.stooq_provider import StooqProviderAdapter
from data_sources.market.market_data_store import MarketDataStore
from data_sources.market.yfinance_provider import YFinanceProviderAdapter


class DataSourceManager:
    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self._sources: Dict[str, DataSource] = {}
        self.entity_store = EntityStore(self.config)
        self.market_store = MarketDataStore(self.config)
        # Register built-ins.
        self.add(RedfinDataSource(self.config, entity_store=self.entity_store))
        # Stooq daily bars via MarketDataSource wrapper
        stooq_provider = StooqProviderAdapter(config=self.config, store=self.market_store, entity_store=self.entity_store)
        providers = [stooq_provider]
        try:
            providers.append(YFinanceProviderAdapter(config=self.config, entity_store=self.entity_store))
        except ImportError:
            # yfinance optional; skip if not installed.
            pass
        self.add(
            MarketDataSource(
                self.config,
                entity_store=self.entity_store,
                providers=providers,
                store=self.market_store,
            )
        )

    # Adds a known data source to this manager.
    def add(self, source: DataSource):
        self._sources[source.name] = source

    # Returns the data source of the given key.
    def get(self, source_name: str) -> DataSource:
        if source_name not in self._sources:
            raise KeyError(f"Unknown data source: {source_name}")
        return self._sources[source_name]
