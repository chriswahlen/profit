from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, Iterator

from config import Config
from data_sources.entity import EntityStore
from data_sources.market.market_data_store import Candle


class YFinanceProviderAdapter:
    """CandleProvider for Yahoo Finance via yfinance.

    Requires optional dependency: yfinance (and pandas). If not installed, init raises ImportError.
    """

    name = "yfinance"

    def __init__(
        self,
        config: Config,
        entity_store: EntityStore,
        period: str = "max",
        interval: str = "1d",
        auto_adjust: bool = False,
    ):
        try:
            import yfinance as yf  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise ImportError("yfinance is required for YFinanceProviderAdapter") from exc

        self._yf = yf
        self.entity_store = entity_store
        self.period = period
        self.interval = interval
        self.auto_adjust = auto_adjust
        self.logger = logging.getLogger(__name__)

    def fetch(self, entity_ids: Iterable[str]) -> Iterator[Candle]:
        for eid in entity_ids:
            ticker = self._resolve_ticker(eid)
            if not ticker:
                self.logger.warning("Skipping %s: no yfinance ticker mapping", eid)
                continue
            try:
                for candle in self._fetch_ticker(eid, ticker):
                    yield candle
            except Exception:  # noqa: BLE001
                self.logger.exception("Failed to fetch %s via yfinance", ticker)

    # --- internals ---------------------------------------------------------
    def _resolve_ticker(self, canonical_id: str) -> str | None:
        # Prefer explicit provider mapping.
        mapped = self.entity_store.provider_ids_for_entity(canonical_id, provider="yfinance")
        if mapped:
            return mapped[0][1]
        # Fallback: use last segment of canonical id as symbol.
        if ":" in canonical_id:
            return canonical_id.split(":")[-1].upper()
        return None

    def _fetch_ticker(self, canonical_id: str, ticker: str) -> Iterator[Candle]:
        hist = self._yf.Ticker(ticker).history(period=self.period, interval=self.interval, auto_adjust=self.auto_adjust)
        # Expected columns: Open, High, Low, Close, Adj Close (if auto_adjust=False), Volume, Dividends (optional)
        for idx, row in hist.iterrows():
            # idx may be Timestamp or datetime/date
            if isinstance(idx, datetime):
                date_str = idx.strftime("%Y-%m-%d")
            else:
                date_str = str(idx)[:10]
            yield Candle(
                canonical_id=canonical_id,
                start_ts=date_str,
                open=self._to_float(row.get("Open")),
                high=self._to_float(row.get("High")),
                low=self._to_float(row.get("Low")),
                close=self._to_float(row.get("Close")),
                adj_close=self._to_float(row.get("Adj Close")),
                dividend=self._to_float(row.get("Dividends")) if "Dividends" in row else None,
                volume=self._to_float(row.get("Volume")),
                provider=self.name,
            )

    @staticmethod
    def _to_float(value) -> float | None:
        try:
            return float(value)
        except Exception:  # noqa: BLE001
            return None

