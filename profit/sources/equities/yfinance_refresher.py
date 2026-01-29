from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
import requests
import logging

from profit.catalog.refresher import CatalogRefresher
from profit.catalog.store import CatalogStore
from profit.catalog.types import InstrumentRecord

logger = logging.getLogger(__name__)

class YFinanceEquitiesRefresher(CatalogRefresher):
    def __init__(
        self,
        store: CatalogStore,
        *,
        cache_root: Path,
        include_etf: bool = False,
        default_mic: str = "XNAS",
        default_currency: str = "USD",
        grace_days: float = 1.0,
    ) -> None:
        self.store = store
        self.cache_root = cache_root
        self.include_etf = include_etf
        self.default_mic = default_mic
        self.default_currency = default_currency
        self.grace_days = grace_days

    def refresh(self, provider: str, *, allow_network: bool, use_cache_only: bool = False) -> None:
        if provider != "yfinance":
            raise ValueError("YFinanceEquitiesRefresher only supports provider='yfinance'")
        seen_at = datetime.now(timezone.utc)
        symbols_dir = self.cache_root / "symbols" / "yfinance"
        nasdaq_path = symbols_dir / "nasdaqtraded.txt"
        other_path = symbols_dir / "otherlisted.txt"

        self._download(
            "https://www.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqtraded.txt",
            nasdaq_path,
            allow_network=allow_network,
            use_cache_only=use_cache_only,
        )
        self._download(
            "https://www.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt",
            other_path,
            allow_network=allow_network,
            use_cache_only=use_cache_only,
        )

        tickers = []
        tickers.extend(self._parse_symbol_file(nasdaq_path, "NASDAQ Symbol"))
        tickers.extend(self._parse_symbol_file(other_path, "ACT Symbol"))

        if not tickers:
            raise RuntimeError("catalog refresh yfinance produced zero symbols; check downloaded files")
        logger.info("catalog refresh yfinance symbols=%s", len(tickers))
        records = [
            InstrumentRecord(
                instrument_id=f"{sym}|{self.default_mic}",
                instrument_type="equity",
                provider="yfinance",
                provider_code=sym,
                mic=self.default_mic,
                currency=self.default_currency,
                active_from=seen_at,
                active_to=None,
                attrs={},
            )
            for sym in tickers
        ]
        self.store.upsert_instruments(records, last_seen=seen_at)
        self.store.mark_missing_as_inactive(provider="yfinance", seen_at=seen_at, grace=self.grace_days)
        self.store.write_meta(provider="yfinance", refreshed_at=seen_at, source_version=None, row_count=len(tickers))

    def _download(self, url: str, dest: Path, *, allow_network: bool, use_cache_only: bool) -> Path:
        if dest.exists() and use_cache_only:
            return dest
        if not dest.exists() and not allow_network:
            raise RuntimeError(f"Catalog refresh needs network to fetch {url}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        logger.info("catalog download url=%s dest=%s", url, dest)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        return dest

    def _parse_symbol_file(self, path: Path, sym_col: str) -> list[str]:
        tickers: list[str] = []
        with path.open() as fh:
            reader = csv.DictReader(fh, delimiter="|")
            for row in reader:
                sym = (row.get(sym_col) or row.get("Symbol") or "").strip()
                if not sym or sym.upper() == "SYMBOL":
                    continue
                if not self.include_etf and (row.get("ETF") or "").upper() == "Y":
                    continue
                if (row.get("Test Issue") or "").upper() == "Y":
                    continue
                tickers.append(sym)
        return tickers
