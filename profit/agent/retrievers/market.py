from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from profit.agent.retrievers.base import BaseRetriever, RetrieverResult
from profit.agent.retrievers.helpers import (
    compute_aggregations,
    normalize_window,
    parse_date_bound,
)
from profit.cache import FileCache
from profit.cache.columnar_store import ColumnarSqliteStore
from profit.config import (
    ProfitConfig,
    get_cache_root,
    get_columnar_db_path,
    get_data_root,
)
from profit.sources.yfinance_ingest import fetch_and_store_yfinance
from profit.stores import StoreContainer

logger = logging.getLogger(__name__)
_CACHE_SUBDIR = "yfinance_fetcher"


class MarketRetriever(BaseRetriever):
    def __init__(self, store: ColumnarSqliteStore | None = None) -> None:
        self.store = store or ColumnarSqliteStore()
        self._catchup_cfg: ProfitConfig | None = None

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("market retriever fetching %s", request)
        start = parse_date_bound(request.get("start"))
        end = parse_date_bound(request.get("end"))
        window_start, window_end = normalize_window(start, end, default_span=timedelta(days=30))
        instruments = [inst for inst in request.get("instruments") or [] if inst]
        if request.get("catch_up") and instruments:
            self._run_catch_up(instruments, window_start, window_end)
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
                            "error_code": "missing_data",
                            "instrument": instrument,
                            "field": field,
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
        for cfg in self.store.find_series_configs(instrument_id=instrument, field=field):
            series_points = self.store.read_points(
                cfg.series_id,
                start=start,
                end=end,
                include_sentinel=False,
            )
            points.extend(series_points)
        return sorted(points, key=lambda p: p[0])

    def _run_catch_up(self, instruments: list[str], start: datetime, end: datetime) -> None:
        cfg = self._resolve_catchup_config()
        if cfg is None:
            logger.warning("market retriever catch-up skipped; config missing")
            return
        ProfitConfig.apply_runtime_env(cfg)
        cache = FileCache(base_dir=cfg.cache_root / _CACHE_SUBDIR, ttl=timedelta(days=1))
        stores = StoreContainer.open(cfg.store_path)
        try:
            fetch_and_store_yfinance(
                instrument_ids=instruments,
                start=start or datetime(1900, 1, 1, tzinfo=timezone.utc),
                end=end or datetime.now(timezone.utc),
                cfg=cfg,
                stores=stores,
                cache=cache,
                ttl=timedelta(days=1),
                catch_up=True,
            )
        finally:
            stores.close()

    def _resolve_catchup_config(self) -> ProfitConfig | None:
        if self._catchup_cfg is not None:
            return self._catchup_cfg
        try:
            data_root = get_data_root()
        except RuntimeError as exc:
            logger.warning("catch-up config unavailable: %s", exc)
            return None
        try:
            cache_root = get_cache_root()
        except RuntimeError:
            cache_root = data_root / "cache"
        store_path = get_columnar_db_path()
        self._catchup_cfg = ProfitConfig(
            data_root=data_root,
            cache_root=cache_root,
            store_path=store_path,
            log_level="INFO",
            refresh_catalog=False,
        )
        return self._catchup_cfg
