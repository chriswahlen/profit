from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from profit.agent_v2.models import RealEstateRequest
from profit.stores.redfin_store import RedfinStore

logger = logging.getLogger(__name__)


def _to_iso(date_str: str) -> datetime:
    return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class RealEstateResult:
    payload: dict
    data_needs: list[dict]


class RealEstateRetrieverV2:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or Path("data/redfin.sqlite")

    def fetch(self, request: RealEstateRequest) -> RealEstateResult:
        params = request.params
        store = RedfinStore(self.db_path, readonly=True)
        start = _to_iso(params.start_utc)
        end = _to_iso(params.end_utc)
        rows = store.fetch_market_metrics(
            [params.geo_id],
            start_date=start.date(),
            end_date=end.date(),
        )
        payload = {
            "type": "real_estate",
            "request_id": request.request_id,
            "region": params.geo_id,
            "rows": rows,
            "measures": params.measures,
            "aggregation": params.aggregation,
        }
        logger.info("real_estate_v2 got %s rows", len(rows))
        return RealEstateResult(payload=payload, data_needs=[])
