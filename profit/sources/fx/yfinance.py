from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from profit.catalog import FetcherDescription
from profit.catalog.lifecycle import CatalogLifecycleReader
from profit.catalog.refresher import CatalogChecker
from profit.catalog.store import CatalogStore
from profit.sources.equities.yfinance_refresher import YFinanceEquitiesRefresher
from profit.sources.fx.base import FxDailyFetcher, FxRatePoint, FxRequest
from profit.sources.errors import ThrottledError


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class YFinanceFxDailyFetcher(FxDailyFetcher):
    """
    Daily FX fetcher using yfinance symbols (e.g., EURUSD=X).
    """

    def __init__(
        self,
        *,
        store,
        source: str = "yfinance",
        version: str = "v1",
        clock: Callable[[], datetime] | None = None,
        max_window_days: int | None = 30,
        max_batch_size: int | None = 50,
        lifecycle=None,
        catalog_checker=None,
        catalog_path: str | Path | None = None,
        cache_root: str | Path | None = None,
        allow_network: bool = True,
        max_catalog_age_days: float = 1.0,
        **kwargs,
    ) -> None:
        if lifecycle is None or catalog_checker is None:
            cat_path = Path(catalog_path) if catalog_path is not None else Path(store.db_path)
            cat_path.parent.mkdir(parents=True, exist_ok=True)
            cat_store = CatalogStore(cat_path, readonly=False)
            lifecycle = CatalogLifecycleReader(cat_store)
            cache_root = Path(cache_root) if cache_root is not None else cat_path.parent
            catalog_checker = CatalogChecker(
                store=cat_store,
                refresher=YFinanceEquitiesRefresher(
                    cat_store,
                    cache_root=cache_root,
                    include_etf=True,
                    default_mic="XNAS",
                    default_currency="USD",
                    grace_days=1.0,
                ),
                max_age=timedelta(days=max_catalog_age_days),
                allow_network=allow_network,
            )
        super().__init__(
            max_batch_size=max_batch_size,
            lifecycle=lifecycle,
            catalog_checker=catalog_checker,
            **kwargs,
        )
        from profit.sources.fx.coverage_adapter import FxCoverageAdapter

        self.source = source
        self.version = version
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.max_window_days = max_window_days
        self._coverage_store = store
        self._coverage_adapter_cls = FxCoverageAdapter

    def describe(self) -> FetcherDescription:
        return FetcherDescription(
            provider=self.source,
            dataset="fx_rate",
            version=self.version,
            freqs=["1d"],
            fields=["rate"],
            max_window_days=self.max_window_days,
            notes="Daily FX close from yfinance multi-symbol download.",
        )

    def coverage_adapter(self, request: FxRequest):
        return self._coverage_adapter_cls(
            self._coverage_store,
            pair=f"{request.base_ccy}/{request.quote_ccy}",
            source=self.source,
            version=self.version,
        )

    def _fetch_timeseries_chunk_many(
        self, requests: list[FxRequest], start: datetime, end: datetime
    ) -> dict[FxRequest, list[FxRatePoint]]:
        """
        Batch-fetch FX pairs using yfinance's multi-symbol download.
        Returns mapping of request -> list of rate points.
        """
        if not requests:
            return {}

        for req in requests:
            if req.provider != self.source:
                raise ValueError(f"Request provider {req.provider!r} does not match fetcher {self.source!r}")
            if req.freq != "1d":
                raise ValueError("YFinanceFxDailyFetcher only supports freq='1d'")

        try:
            import yfinance as yf  # type: ignore
            import pandas as pd  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "Missing optional dependency 'yfinance'. Install it to use YFinanceFxDailyFetcher."
            ) from exc

        start_utc = _to_utc(start).date()
        end_exclusive = _to_utc(end).date() + timedelta(days=1)

        codes = [req.provider_code for req in requests]

        try:
            df = yf.download(
                codes,
                start=start_utc,
                end=end_exclusive,
                auto_adjust=False,
                actions=False,
                progress=False,
                group_by="ticker",
            )
        except Exception as exc:
            retry_after = None
            status = getattr(exc, "response", None)
            code = getattr(status, "status_code", None)
            if code == 429:
                ra = getattr(status, "headers", {}).get("Retry-After") if status else None
                try:
                    retry_after = float(ra)
                except Exception:
                    retry_after = None
                raise ThrottledError("yfinance HTTP 429", retry_after=retry_after) from exc
            raise

        if df is None:
            return {req: [] for req in requests}

        # Normalize single-ticker frame to MultiIndex columns.
        if not isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = pd.MultiIndex.from_product([[codes[0]], list(df.columns)])

        asof = _to_utc(self._clock())

        def _scalar(val):
            if hasattr(val, "iloc"):
                try:
                    return float(val.iloc[-1])
                except Exception:
                    pass
            try:
                return float(val)
            except Exception as exc:  # pragma: no cover - defensive
                raise TypeError(f"Cannot convert value to float: {val!r}") from exc

        out: dict[FxRequest, list[FxRatePoint]] = {}
        for req in requests:
            code = req.provider_code
            try:
                sub = df[code]
            except Exception:
                out[req] = []
                continue

            if getattr(sub, "empty", False):
                out[req] = []
                continue

            keys = list(sub.index)
            keys.sort()

            points: list[FxRatePoint] = []
            for k in keys:
                ts = getattr(k, "to_pydatetime", lambda: k)()
                if isinstance(ts, datetime):
                    ts_utc = _to_utc(ts)
                else:
                    ts_utc = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)  # type: ignore[attr-defined]

                row = sub.loc[k]
                if hasattr(row, "columns"):
                    row = row.iloc[-1]
                rate = _scalar(row["Close"]) if "Close" in row else _scalar(row.iloc[-1])
                points.append(
                    FxRatePoint(
                        base_ccy=req.base_ccy,
                        quote_ccy=req.quote_ccy,
                        ts_utc=ts_utc,
                        rate=rate,
                        source=self.source,
                        version=self.version,
                        asof=asof,
                    )
                )
            out[req] = points

        return out
