from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from profit.agent.retrievers.base import BaseRetriever, RetrieverResult
from profit.agent.retrievers.helpers import (
    compute_aggregations,
    normalize_window,
    parse_date_bound,
)
from profit.config import get_columnar_db_path
from profit.stores.redfin_store import RedfinStore


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

logger = logging.getLogger(__name__)


def _resolve_redfin_path(default: Path) -> Path:
    try:
        return get_columnar_db_path()
    except RuntimeError:
        return default


class RealEstateRetriever(BaseRetriever):
    def __init__(self, store: RedfinStore | None = None, *, db_path: Path | None = None) -> None:
        if store:
            self.store = store
        else:
            base = db_path or _resolve_redfin_path(Path("data/profit.sqlite"))
            self.store = RedfinStore(base, readonly=True)

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("real_estate retriever fetching %s", request)
        start = parse_date_bound(request.get("start"))
        end = parse_date_bound(request.get("end"))
        window_start, window_end = normalize_window(start, end, default_span=timedelta(days=90))

        region_ids = request.get("regions") or []
        rows: list[dict] = []
        if region_ids:
            rows = self.store.fetch_market_metrics(
                region_ids,
                start_date=window_start.date(),
                end_date=window_end.date(),
            )

        grouped: dict[str, list[dict]] = {}
        for row in rows:
            region = row["region_id"]
            grouped.setdefault(region, []).append(dict(row))

        results: list[dict] = []
        data_needs: list[dict] = []
        aggregations = request.get("aggregation") or []
        for region in region_ids:
            metrics = grouped.get(region)
            if not metrics:
                data_needs.append(
                    {
                        "name": region,
                        "reason": "no real estate metrics in requested window",
                        "criticality": "medium",
                    }
                )
                continue
            price_points = [
                (_to_utc(datetime.fromisoformat(entry["period_start_date"])), entry["median_sale_price"])
                for entry in metrics
                if entry["median_sale_price"] is not None
            ]
            price_aggregations = compute_aggregations(
                price_points,
                aggregations=aggregations,
                window_end=window_end,
            )
            results.append(
                {
                    "region": region,
                    "granularity": metrics[0]["period_granularity"],
                    "metrics": metrics,
                    "aggregations": price_aggregations,
                }
            )

        payload = {
            "type": "real_estate",
            "request": request,
            "data": results,
            "notes": notes,
            "window": {"start": window_start.date().isoformat(), "end": window_end.date().isoformat()},
        }
        return RetrieverResult(payload=payload, data_needs=data_needs)
