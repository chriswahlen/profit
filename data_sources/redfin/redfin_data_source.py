

from __future__ import annotations

from typing import Iterable

from config import Config
from data_sources.data_source import DataSource, DataSourceUpdateResults
from data_sources.redfin.redfin_data_store import RedfinDataStore


class RedfinDataSource(DataSource):
    """Redfin data source stub that ingests into `RedfinDataStore`.

    This currently accepts pre-fetched listing payloads (e.g., from a scraper
    elsewhere) and focuses on wiring the store into the framework.
    """

    def __init__(self, config: Config):
        super().__init__(name="redfin", summary="Residential listings from Redfin", config=config)
        self.store = RedfinDataStore(config)

    def describe_detailed(self, *, indent: str = "  ") -> str:
        lines = [f"{indent}Storage: sqlite at {self.store.db_path}"]
        lines.append(self.store.describe_detailed(indent=indent))
        return "\n".join(lines)

    def ensure_up_to_date(self, entity_ids: Iterable[str]) -> DataSourceUpdateResults:
        # Placeholder: ingestion contract not defined yet.
        return DataSourceUpdateResults(
            updated=0,
            skipped=0,
            failed=0,
            detail="Redfin ingest pipeline TBD",
        )
