
from __future__ import annotations

from typing import Dict

from config import Config
from data_sources.data_source import DataSource
from data_sources.redfin.redfin_data_source import RedfinDataSource


class DataSourceManager:
    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self._sources: Dict[str, DataSource] = {}
        # Register built-ins.
        self.add(RedfinDataSource(self.config))

    # Adds a known data source to this manager.
    def add(self, source: DataSource):
        self._sources[source.name] = source

    # Returns the data source of the given key.
    def get(self, source_name: str) -> DataSource:
        if source_name not in self._sources:
            raise KeyError(f"Unknown data source: {source_name}")
        return self._sources[source_name]
