from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from profit.sources.equities import EquityDailyBar, EquityDailyBarsRequest, EquitiesDailyFetcher


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
        *args,
        source: str = "yfinance",
        version: str = "v1",
        clock: Callable[[], datetime] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.source = source
        self.version = version
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _fetch_timeseries_chunk(
        self, request: EquityDailyBarsRequest, start: datetime, end: datetime
    ) -> list[EquityDailyBar]:
        if request.provider != self.source:
            raise ValueError(f"Request provider {request.provider!r} does not match fetcher {self.source!r}")
        if request.freq != "1d":
            raise ValueError("YFinanceDailyBarsFetcher only supports freq='1d'")

        try:
            import yfinance as yf  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "Missing optional dependency 'yfinance'. Install it to use YFinanceDailyBarsFetcher."
            ) from exc

        # yfinance uses an exclusive end. Normalize to date buckets.
        start_utc = _to_utc(start).date()
        end_exclusive = _to_utc(end).date() + timedelta(days=1)

        raw_df = yf.download(
            request.provider_code,
            start=start_utc,
            end=end_exclusive,
            auto_adjust=False,
            actions=False,
            progress=False,
        )
        adj_df = yf.download(
            request.provider_code,
            start=start_utc,
            end=end_exclusive,
            auto_adjust=True,
            actions=False,
            progress=False,
        )

        if raw_df is None or adj_df is None:
            return []
        if getattr(raw_df, "empty", False) or getattr(adj_df, "empty", False):
            return []

        # Join on the date index intersection.
        raw_idx = list(raw_df.index)
        adj_idx = set(adj_df.index)
        keys = [k for k in raw_idx if k in adj_idx]
        keys.sort()

        asof = _to_utc(self._clock())

        def _single_row(df, key):
            row = df.loc[key]
            if hasattr(row, "columns"):
                row = row.iloc[-1]
            return row

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
                # Fallback for dict-like or iterable.
                try:
                    return float(list(val)[-1])  # type: ignore[arg-type]
                except Exception as exc:  # pragma: no cover - defensive
                    raise TypeError(f"Cannot convert value to float: {val!r}") from exc

        out: list[EquityDailyBar] = []
        for k in keys:
            # pandas.Timestamp -> python datetime
            ts = getattr(k, "to_pydatetime", lambda: k)()
            if isinstance(ts, datetime):
                ts_utc = _to_utc(ts)
            else:
                # Assume date-like value.
                ts_utc = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)  # type: ignore[attr-defined]

            raw_row = _single_row(raw_df, k)
            adj_row = _single_row(adj_df, k)
            out.append(
                EquityDailyBar(
                    instrument_id=request.instrument_id,
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
        return out
