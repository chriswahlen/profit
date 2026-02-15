from __future__ import annotations

import logging
from typing import Iterable, Sequence
from pathlib import Path

from config import Config
from data_sources.market.market_data_store import Candle
from data_sources.market.stooq_importer import StooqImporter


class StooqProviderAdapter:
    """Adapter to expose StooqImporter as a CandleProvider."""

    name = "stooq"

    def __init__(
        self,
        config: Config,
        zip_paths: Iterable[Path] | None = None,
        store=None,
        entity_store=None,
    ):
        self.config = config
        self.zip_paths = list(zip_paths) if zip_paths else None
        self._importer = StooqImporter(
            config=config,
            zip_paths=self.zip_paths,
            provider=self.name,
            store=store,
            entity_store=entity_store,
        )
        self._logger = logging.getLogger(__name__)

    def fetch(self, entity_ids: Iterable[str]) -> Iterable[Candle]:
        # Stooq is batch: ignores entity_ids, returns all available candles with progress logs.
        self._logger.info("Stooq provider loading all available Stooq archives")
        yield from self._importer.iter_all_candles_with_progress(logger=self._logger)
