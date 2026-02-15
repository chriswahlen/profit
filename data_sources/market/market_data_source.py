from __future__ import annotations

import logging
from typing import Iterable, List, Protocol

from config import Config
from data_sources.data_source import DataSource, DataSourceUpdateResults
from data_sources.entity import EntityStore
from data_sources.market.market_data_store import Candle, MarketDataStore


class CandleProvider(Protocol):
    """Minimal provider interface for fetching OHLCV candles."""

    name: str

    def fetch(self, entity_ids: Iterable[str]) -> Iterable[Candle]:
        ...


class MarketDataSource(DataSource):
    """Coordinates provider pulls into the MarketDataStore."""

    def __init__(
        self,
        config: Config,
        entity_store: EntityStore,
        providers: List[CandleProvider],
        store: MarketDataStore | None = None,
    ):
        super().__init__(name="market", summary="OHLCV bars from multiple providers", config=config, entity_store=entity_store)
        self.config = config
        self.entity_store = entity_store
        self.providers = providers
        self.store = store or MarketDataStore(config)
        self.logger = logging.getLogger(__name__)

    def describe_detailed(self, *, indent: str = "  ") -> str:
        lines = [f"{indent}Storage: sqlite at {self.store.db_path}"]
        lines.append(self.store.describe_detailed(indent=indent))
        return "\n".join(lines)

    def ensure_up_to_date(self, entity_ids: Iterable[str], providers: List[str] | None = None) -> DataSourceUpdateResults:
        active_providers = [p for p in self.providers if providers is None or p.name in providers]
        if not active_providers:
            return DataSourceUpdateResults(detail="no matching providers")

        total_updated = total_failed = 0
        batch_size = 5000
        for provider in active_providers:
            run_id = self.store.start_ingestion_run(provider=provider.name, source=f"provider:{provider.name}")
            updated = failed = 0
            batch: list[Candle] = []
            try:
                for idx, candle in enumerate(provider.fetch(entity_ids), start=1):
                    batch.append(candle)
                    if len(batch) >= batch_size:
                        res = self.store.upsert_candles_raw(batch)
                        updated += res.updated
                        failed += res.failed
                        batch.clear()
                        if idx % batch_size == 0:
                            self.logger.info("Provider %s processed %d candles", provider.name, idx)
                if batch:
                    res = self.store.upsert_candles_raw(batch)
                    updated += res.updated
                    failed += res.failed
                status = "success" if failed == 0 else "partial"
                self.store.finish_ingestion_run(run_id=run_id, status=status, row_count=updated, notes=f"failed={failed}")
                self.logger.info("Provider %s upserted %d candles (failed=%d)", provider.name, updated, failed)
            except Exception:  # noqa: BLE001 - log and continue
                failed += 1
                self.store.finish_ingestion_run(run_id=run_id, status="failed", row_count=updated, notes="exception")
                self.logger.exception("Provider %s fetch failed", provider.name)
            total_updated += updated
            total_failed += failed
        return DataSourceUpdateResults(updated=total_updated, failed=total_failed)
