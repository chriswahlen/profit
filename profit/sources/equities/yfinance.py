from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from profit.catalog import FetcherDescription
from profit.sources.equities import EquityDailyBar, EquityDailyBarsRequest, EquitiesDailyFetcher
from profit.sources.errors import ThrottledError


def _to_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


class YFinanceDailyBarsFetcher(EquitiesDailyFetcher):
    """
    Daily equity OHLCV fetcher backed by the `yfinance` library.

    Notes:
    - `yfinance` is an optional dependency; this class imports it lazily.
    - The fetch window is inclusive; yfinance expects an exclusive `end`.
    - This fetcher returns both "raw" and "adjusted" bars (auto_adjust=False/True).
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
        lifecycle,
        **kwargs,
    ) -> None:
        super().__init__(max_batch_size=max_batch_size, lifecycle=lifecycle, **kwargs)
        from profit.sources.equities.coverage_adapter import EquitiesCoverageAdapter

        self.source = source
        self.version = version
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.max_window_days = max_window_days
        self._coverage_store = store
        self._coverage_adapter_cls = EquitiesCoverageAdapter

    def describe(self) -> FetcherDescription:
        return FetcherDescription(
            provider=self.source,
            dataset="equity_ohlcv",
            version=self.version,
            freqs=["1d"],
            fields=[
                "open_raw",
                "high_raw",
                "low_raw",
                "close_raw",
                "volume_raw",
                "open_adj",
                "high_adj",
                "low_adj",
                "close_adj",
                "volume_adj",
            ],
            max_window_days=self.max_window_days,
            notes="Backed by yfinance multi-ticker daily bars; adjusted + raw.",
        )

    def coverage_adapter(self, request: EquityDailyBarsRequest):
        return self._coverage_adapter_cls(
            self._coverage_store,
            instrument_id=request.instrument_id,
            source=self.source,
            version=self.version,
        )

    def _fetch_timeseries_chunk_many(
        self, requests: list[EquityDailyBarsRequest], start: datetime, end: datetime
    ) -> dict[EquityDailyBarsRequest, list[EquityDailyBar]]:
        """
        Batch-fetch daily bars for many equities using yfinance's multi-symbol
        download. Returns a mapping of request -> list of bars covering the
        inclusive [start, end] window.
        """
        if not requests:
            return {}

        for req in requests:
            if req.provider != self.source:
                raise ValueError(f"Request provider {req.provider!r} does not match fetcher {self.source!r}")
            if req.freq != "1d":
                raise ValueError("YFinanceDailyBarsFetcher only supports freq='1d'")

        try:
            import yfinance as yf  # type: ignore
            import pandas as pd  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "Missing optional dependency 'yfinance'. Install it to use YFinanceDailyBarsFetcher."
            ) from exc

        start_utc = _to_utc(start).date()
        end_exclusive = _to_utc(end).date() + timedelta(days=1)

        codes = [req.provider_code for req in requests]
        try:
            raw_df = yf.download(
                codes,
                start=start_utc,
                end=end_exclusive,
                auto_adjust=False,
                actions=False,
                progress=False,
                group_by="ticker",
            )
            adj_df = yf.download(
                codes,
                start=start_utc,
                end=end_exclusive,
                auto_adjust=True,
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

        # Normalize single-ticker frames to MultiIndex shape for uniform handling.
        def _normalize_df(df: pd.DataFrame | None, code: str) -> pd.DataFrame | None:
            if df is None:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                return df
            # Single ticker: wrap columns into a MultiIndex (code, field)
            df_copy = df.copy()
            df_copy.columns = pd.MultiIndex.from_product([[code], list(df.columns)])
            return df_copy

        if raw_df is None or adj_df is None:
            return {req: [] for req in requests}

        raw_df = _normalize_df(raw_df, codes[0])
        adj_df = _normalize_df(adj_df, codes[0])
        if raw_df is None or adj_df is None:
            return {req: [] for req in requests}

        asof = _to_utc(self._clock())

        def _scalar(val):
            if hasattr(val, "iloc"):
                try:
                    return float(val.iloc[-1])
                except Exception:
                    pass
            if isinstance(val, (list, tuple)):
                return float(val[-1])
            try:
                return float(val)
            except TypeError:
                try:
                    return float(list(val)[-1])  # type: ignore[arg-type]
                except Exception as exc:  # pragma: no cover - defensive
                    raise TypeError(f"Cannot convert value to float: {val!r}") from exc

        out: dict[EquityDailyBarsRequest, list[EquityDailyBar]] = {}
        for req in requests:
            code = req.provider_code
            try:
                raw_sub = raw_df[code]
                adj_sub = adj_df[code]
            except Exception:
                out[req] = []
                continue

            if getattr(raw_sub, "empty", False) or getattr(adj_sub, "empty", False):
                out[req] = []
                continue

            keys = list(set(raw_sub.index).intersection(set(adj_sub.index)))
            keys.sort()

            bars: list[EquityDailyBar] = []
            for k in keys:
                ts = getattr(k, "to_pydatetime", lambda: k)()
                if isinstance(ts, datetime):
                    ts_utc = _to_utc(ts)
                else:
                    ts_utc = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)  # type: ignore[attr-defined]

                raw_row = raw_sub.loc[k]
                adj_row = adj_sub.loc[k]
                # Normalize row in case of DataFrame slice returning DataFrame
                if hasattr(raw_row, "columns"):
                    raw_row = raw_row.iloc[-1]
                if hasattr(adj_row, "columns"):
                    adj_row = adj_row.iloc[-1]

                bars.append(
                    EquityDailyBar(
                        instrument_id=req.instrument_id,
                        ts_utc=ts_utc,
                        open_raw=_scalar(raw_row["Open"]),
                        high_raw=_scalar(raw_row["High"]),
                        low_raw=_scalar(raw_row["Low"]),
                        close_raw=_scalar(raw_row["Close"]),
                        volume_raw=_scalar(raw_row["Volume"]),
                        open_adj=_scalar(adj_row["Open"]),
                        high_adj=_scalar(adj_row["High"]),
                        low_adj=_scalar(adj_row["Low"]),
                        close_adj=_scalar(adj_row["Close"]),
                        volume_adj=_scalar(adj_row["Volume"]),
                        source=self.source,
                        version=self.version,
                        asof=asof,
                    )
                )
            out[req] = bars

        return out
