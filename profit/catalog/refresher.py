from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol, Sequence

from profit.catalog.store import CatalogStore
from profit.catalog.types import InstrumentRecord

logger = logging.getLogger(__name__)


class CatalogRefresher(Protocol):
    def refresh(self, provider: str, *, allow_network: bool, use_cache_only: bool = False) -> None:
        ...


@dataclass
class CatalogChecker:
    store: CatalogStore
    refresher: CatalogRefresher
    max_age: timedelta
    allow_network: bool
    use_cache_only: bool = False

    def mark_stale(self, provider: str) -> None:
        cur = self.store.conn.cursor()
        cur.execute("DELETE FROM catalog_meta WHERE provider = ?", (provider,))
        self.store.conn.commit()
        logger.info("catalog meta marked stale provider=%s", provider)

    def ensure_fresh(self, provider: str) -> None:
        meta = self.store.read_meta(provider)
        now = datetime.now(timezone.utc)
        if meta is None or now - meta["refreshed_at"] > self.max_age:
            logger.info("catalog refresh start provider=%s reason=%s", provider, "missing" if meta is None else "stale")
            self.refresher.refresh(provider, allow_network=self.allow_network, use_cache_only=self.use_cache_only)
            meta_after = self.store.read_meta(provider)
            if meta_after is None or now - meta_after["refreshed_at"] > self.max_age:
                raise RuntimeError(
                    f"Catalog refresh failed or still stale for provider={provider}: refreshed_at={meta_after}"
                )
            logger.info("catalog refresh done provider=%s refreshed_at=%s rows=%s", provider, meta_after["refreshed_at"], meta_after.get("row_count"))

    def require_present(self, provider: str, provider_code: str) -> None:
        row = self.store.get_instrument(provider, provider_code)
        if row is None:
            meta = self.store.read_meta(provider)
            now = datetime.now(timezone.utc)
            stale = meta is None or now - meta["refreshed_at"] > self.max_age
            if stale:
                logger.info("catalog missing symbol provider=%s code=%s; triggering refresh", provider, provider_code)
                self.refresher.refresh(provider, allow_network=self.allow_network, use_cache_only=self.use_cache_only)
                row = self.store.get_instrument(provider, provider_code)
            if row is None:
                raise RuntimeError(f"Instrument {provider}:{provider_code} not found in catalog (stale={stale})")
