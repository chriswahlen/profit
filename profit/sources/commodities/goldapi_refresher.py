from __future__ import annotations

from datetime import datetime, timezone
import logging

from profit.catalog.refresher import CatalogRefresher
from profit.catalog.store import CatalogStore
from profit.catalog.types import InstrumentRecord

logger = logging.getLogger(__name__)


class GoldApiRefresher(CatalogRefresher):
    """
    Simple refresher that seeds gold/silver entries for goldapi.
    """

    def __init__(self, store: CatalogStore) -> None:
        self.store = store

    def refresh(self, provider: str, *, allow_network: bool, use_cache_only: bool = False) -> None:
        if provider != "goldapi":
            raise ValueError("GoldApiRefresher only supports provider='goldapi'")
        seen_at = datetime.now(timezone.utc)
        active_from = datetime(1900, 1, 1, tzinfo=timezone.utc)
        logger.info("catalog refresh goldapi start")
        rows = [
            InstrumentRecord(
                instrument_id="XAU|LBMA",
                instrument_type="commodity",
                provider="goldapi",
                provider_code="XAU",
                mic=None,
                currency="USD",
                active_from=active_from,
                active_to=None,
                attrs={"name": "gold"},
            ),
            InstrumentRecord(
                instrument_id="XAG|LBMA",
                instrument_type="commodity",
                provider="goldapi",
                provider_code="XAG",
                mic=None,
                currency="USD",
                active_from=active_from,
                active_to=None,
                attrs={"name": "silver"},
            ),
        ]
        self.store.upsert_instruments(rows, last_seen=seen_at)
        self.store.write_meta(provider="goldapi", refreshed_at=seen_at, source_version=None, row_count=len(rows))
        logger.info("catalog refresh goldapi done rows=%s", len(rows))
