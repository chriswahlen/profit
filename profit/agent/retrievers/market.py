from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Iterable

from profit.agent.retrievers.base import BaseRetriever, RetrieverResult
from profit.agent.retrievers.helpers import (
    compute_aggregations,
    normalize_window,
    parse_date_bound,
)
from profit.cache.columnar_store import ColumnarSqliteStore

logger = logging.getLogger(__name__)


class MarketRetriever(BaseRetriever):
    def __init__(self, store: ColumnarSqliteStore | None = None) -> None:
        self.store = store or ColumnarSqliteStore()

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("market retriever fetching %s", request)
        start = parse_date_bound(request.get("start"))
        end = parse_date_bound(request.get("end"))
        window_start, window_end = normalize_window(start, end, default_span=timedelta(days=30))
        results: list[dict] = []
        data_needs: list[dict] = []

        aggregations = request.get("aggregation") or []
        for instrument in request.get("instruments") or []:
            for field in request.get("fields") or []:
                points = self._collect_points(instrument, field, window_start, window_end)
                if not points:
                    data_needs.append(
                        {
                            "name": f"{instrument}|{field}",
                            "reason": "no data available for requested window",
                            "criticality": "high",
                        }
                    )
                    continue
                point_dicts = [
                    {"timestamp": ts.isoformat(), "value": value} for ts, value in points
                ]
                aggs = compute_aggregations(points, aggregations=aggregations, window_end=window_end)
                results.append(
                    {
                        "instrument": instrument,
                        "field": field,
                        "points": point_dicts,
                        "aggregations": aggs,
                    }
                )

        payload = {
            "type": "market",
            "request": request,
            "data": results,
            "notes": notes,
            "window": {"start": window_start.isoformat(), "end": window_end.isoformat()},
        }
        return RetrieverResult(payload=payload, data_needs=data_needs)

    def _collect_points(
        self,
        instrument: str,
        field: str,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, float]]:
        points: list[tuple[datetime, float]] = []
        for cfg in self.store._series_cache.values():
            if cfg.instrument_id != instrument or cfg.field != field:
                continue
            series_points = self.store.read_points(
                cfg.series_id,
                start=start,
                end=end,
                include_sentinel=False,
            )
            points.extend(series_points)
        return sorted(points, key=lambda p: p[0])
