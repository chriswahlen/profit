from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

from profit.sources.fx.base import FxDailyFetcher, FxRatePoint, FxRequest


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

    def _fetch_timeseries_chunk(self, request: FxRequest, start: datetime, end: datetime) -> list[FxRatePoint]:
        if request.provider != self.source:
            raise ValueError(f"Request provider {request.provider!r} does not match fetcher {self.source!r}")
        if request.freq != "1d":
            raise ValueError("YFinanceFxDailyFetcher only supports freq='1d'")

        try:
            import yfinance as yf  # type: ignore
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "Missing optional dependency 'yfinance'. Install it to use YFinanceFxDailyFetcher."
            ) from exc

        start_utc = _to_utc(start).date()
        end_exclusive = _to_utc(end).date() + timedelta(days=1)

        df = yf.download(
            request.provider_code,
            start=start_utc,
            end=end_exclusive,
            auto_adjust=False,
            actions=False,
            progress=False,
        )
        if df is None or getattr(df, "empty", False):
            return []

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

        keys = list(df.index)
        keys.sort()

        asof = _to_utc(self._clock())
        out: list[FxRatePoint] = []
        for k in keys:
            ts = getattr(k, "to_pydatetime", lambda: k)()
            if isinstance(ts, datetime):
                ts_utc = _to_utc(ts)
            else:
                ts_utc = datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)  # type: ignore[attr-defined]

            row = df.loc[k]
            # yfinance FX rows often only have 'Close'
            rate = _scalar(row["Close"]) if "Close" in row else _scalar(row.iloc[-1])
            out.append(
                FxRatePoint(
                    base_ccy=request.base_ccy,
                    quote_ccy=request.quote_ccy,
                    ts_utc=ts_utc,
                    rate=rate,
                    source=self.source,
                    version=self.version,
                    asof=asof,
                )
            )
        return out

