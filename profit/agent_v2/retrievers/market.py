from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.agent_v2.models import MarketOhlcvRequest

logger = logging.getLogger(__name__)


def _parse_date(date_str: str) -> datetime:
    # date-only YYYY-MM-DD interpreted as 00:00Z
    return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class MarketResult:
    payload: dict
    data_needs: list[dict]


class MarketRetrieverV2:
    def __init__(self, store: ColumnarSqliteStore | None = None) -> None:
        self.store = store or ColumnarSqliteStore()

    def fetch(self, request: MarketOhlcvRequest) -> MarketResult:
        params = request.params
        instrument = f"{params.exchange_mic}|{params.ticker}"
        start = _parse_date(params.start_utc)
        end = _parse_date(params.end_utc)
        results: list[dict] = []
        data_needs: list[dict] = []

        for field in params.fields:
            points: list[tuple[datetime, float]] = []
            for cfg in self.store.find_series_configs(instrument_id=instrument, field=field):
                points.extend(
                    self.store.read_points(
                        cfg.series_id,
                        start=start,
                        end=end,
                        include_sentinel=False,
                    )
                )
            points = sorted(points, key=lambda p: p[0])
            if not points:
                data_needs.append(
                    {
                        "name": f"{instrument}|{field}",
                        "reason": "no market data available for requested window",
                        "criticality": "high",
                        "error_code": "missing_data",
                        "instrument": instrument,
                        "field": field,
                        "window": {"start": params.start_utc, "end": params.end_utc},
                    }
                )
                continue
            results.append(
                {
                    "instrument": instrument,
                    "field": field,
                    "points": [{"timestamp": ts.isoformat(), "value": value} for ts, value in points],
                }
            )

        payload = {
            "type": "market",
            "request_id": request.request_id,
            "request": params.model_dump(),
            "data": results,
        }
        logger.info(
            "market_v2 fetched instrument=%s fields=%s points=%s",
            instrument,
            list(params.fields),
            sum(len(r["points"]) for r in results),
        )
        return MarketResult(payload=payload, data_needs=data_needs)

