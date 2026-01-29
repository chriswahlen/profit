from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List
import os

import requests

from profit.sources.commodities.base import (
    CommoditiesDailyFetcher,
    CommodityDailyPrice,
    CommodityDailyRequest,
)
from profit.sources.errors import ThrottledError


logger = logging.getLogger(__name__)


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class GoldApiCommoditiesFetcher(CommoditiesDailyFetcher):
    """
    Daily commodity prices via goldapi.io (gold/silver spot).

    - Uses per-day historical endpoint; one HTTP request per day per symbol.
    - Authentication: GOLDAPI_API_KEY env or api_key arg; sent as header `x-access-token`.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        source: str = "goldapi",
        version: str = "v1",
        base_url: str = "https://www.goldapi.io/api",
        clock: Callable[[], datetime] | None = None,
        max_window_days: int | None = None,
        max_batch_size: int | None = 1,
        **kwargs,
    ) -> None:
        super().__init__(max_window_days=max_window_days, max_batch_size=max_batch_size, **kwargs)
        self.source = source
        self.version = version
        self.base_url = base_url.rstrip("/")
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.api_key = api_key or os.getenv("GOLDAPI_API_KEY")
        if not self.api_key:
            raise ValueError("GoldAPI API key is required (set GOLDAPI_API_KEY or pass api_key)")

    def coverage_adapter(self, request: CommodityDailyRequest):
        from profit.sources.commodities.coverage_adapter import CommoditiesCoverageAdapter
        from profit.cache import ColumnarSqliteStore

        store: ColumnarSqliteStore = getattr(self, "_coverage_store")
        return CommoditiesCoverageAdapter(
            store,
            instrument_id=request.instrument_id,
            source=self.source,
            version=self.version,
        )

    def _fetch_timeseries_chunk_many(
        self, requests: List[CommodityDailyRequest], start: datetime, end: datetime
    ) -> Dict[CommodityDailyRequest, List[CommodityDailyPrice]]:
        if not requests:
            return {}

        out: Dict[CommodityDailyRequest, List[CommodityDailyPrice]] = {}
        for req in requests:
            if req.provider != self.source:
                raise ValueError(f"Request provider {req.provider!r} does not match fetcher {self.source!r}")
            if req.freq != "1d":
                raise ValueError("GoldApiCommoditiesFetcher only supports freq='1d'")

            prices: List[CommodityDailyPrice] = []
            for day in self._iter_days(start, end):
                price = self._fetch_single_day(req, day)
                if price is not None:
                    prices.append(price)
            out[req] = prices
        return out

    def _iter_days(self, start: datetime, end: datetime):
        cursor = _to_utc(start).date()
        end_date = _to_utc(end).date()
        one = timedelta(days=1)
        while cursor <= end_date:
            yield cursor
            cursor += one

    def _fetch_single_day(self, request: CommodityDailyRequest, day) -> CommodityDailyPrice | None:
        symbol = request.provider_code  # expect e.g., XAU or XAG
        url = f"{self.base_url}/{symbol}/USD/{day.isoformat()}"
        headers = {"x-access-token": self.api_key}
        logger.info("goldapi request url=%s day=%s", url, day)
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as exc:
            logger.warning("goldapi request failed symbol=%s day=%s err=%s", symbol, day, exc)
            return None
        if resp.status_code == 429:
            raise ThrottledError("goldapi HTTP 429", retry_after=60.0)
        if resp.status_code >= 500:
            raise ThrottledError(f"goldapi {resp.status_code}", retry_after=60.0)
        if resp.status_code == 401:
            raise ValueError("goldapi unauthorized: check API key")
        resp.raise_for_status()
        data = resp.json()

        # goldapi returns price under various keys; prefer 'price'
        price_val = None
        for key in ("price", "price_close", "close_price"):
            if isinstance(data, dict) and key in data:
                try:
                    price_val = float(data[key])
                    break
                except Exception:
                    continue

        if price_val is None:
            logger.warning("goldapi unexpected payload symbol=%s keys=%s", symbol, list(data.keys())[:5] if isinstance(data, dict) else type(data))
            return None

        ts = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        asof = _to_utc(self._clock())
        return CommodityDailyPrice(
            instrument_id=request.instrument_id,
            ts_utc=ts,
            price=price_val,
            currency="USD",
            source=self.source,
            version=self.version,
            asof=asof,
        )
