from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from profit.catalog import InstrumentRecord
from profit.catalog.store import CatalogStore
from profit.seed_metadata import ensure_seed_metadata, read_seed_metadata, write_seed_metadata


@dataclass(frozen=True)
class SeedResult:
    instruments_written: int


class StooqDailySeeder:
    """
    Seed the Stooq daily dataset into the catalog.
    """

    TYPE_MAP = {
        "currencies": "currency",
        "money market": "money_market",
        "indices": "index",
        "cryptocurrencies": "cryptocurrency",
        "bonds": "bond",
        "stooq stocks indices": "equity",
    }

    CANONICAL_PREFIX = {
        "currencies": "FX",
        "money market": "MM",
        "indices": "INDEX",
        "cryptocurrencies": "CRYPTO",
        "bonds": "BOND",
    }

    EXCHANGE_SUFFIX_MAP = {
        "US": "XNAS",
        "FT": "XLON",
        "L": "XLON",
        "DE": "XFRA",
        "HK": "XHKG",
        "F": "XPAR",
        "SW": "XSWX",
        "AS": "XASE",
        "B": "XLON",
        "V": "XSWX",
        "X": "XAMS",
        "ST": "XSTO",
    }

    def __init__(
        self,
        store: CatalogStore,
        *,
        data_root: Path,
        provider: str = "stooq",
        force: bool = False,
        ttl: timedelta = timedelta(days=7),
    ) -> None:
        self.store = store
        self.data_root = data_root
        self.provider = provider
        self.force = force
        self.ttl = ttl

    def seed(self) -> SeedResult:
        base = self._find_base_path()
        if base is None:
            logging.warning("Stooq dataset base path missing under %s", self.data_root)
            return SeedResult(instruments_written=0)

        ensure_seed_metadata(self.store.conn)
        if not self.force and self._should_skip():
            age = self._last_run_age()
            remaining = max(self.ttl - age, timedelta(0))
            logging.info(
                "Stooq seeder skipped: last_run_age=%s ttl=%s next_refresh_in=%s",
                age,
                self.ttl,
                remaining,
            )
            return SeedResult(instruments_written=0)

        instruments = list(self._discover_instruments(base))
        if not instruments:
            logging.info("Stooq seeder found no instruments under %s", base)
            return SeedResult(instruments_written=0)

        written = self.store.upsert_instruments(instruments)
        logging.info("Stooq seeder wrote %s instruments", written)
        self._bump_metadata()
        return SeedResult(instruments_written=written)

    def _discover_instruments(self, base: Path) -> Iterable[InstrumentRecord]:
        for txt in base.rglob("*.txt"):
            relative = txt.relative_to(base)
            parts = [p.lower() for p in relative.parts[:-1]]
            category = "/".join(parts) if parts else "unknown"
            ticker = txt.stem.upper()
            instrument_id = self._canonical_instrument_id(ticker, parts)
            instrument_type = self._guess_type(parts, ticker)
            attrs = {"category": category, "path": str(txt)}

            yield InstrumentRecord(
                instrument_id=instrument_id,
                instrument_type=instrument_type,
                provider=self.provider,
                provider_code=ticker,
                mic=self._exchange_for_ticker(ticker, parts),
                currency=None,
                active_from=None,
                active_to=None,
                attrs=attrs,
            )

    def _guess_type(self, parts: list[str], ticker: str) -> str:
        if ticker.startswith("^"):
            return "synthetic"
        for part in parts:
            cleaned = part.replace("-", " ")
            if cleaned in self.TYPE_MAP:
                return self.TYPE_MAP[cleaned]
        return "unknown"

    def _canonical_instrument_id(self, ticker: str, parts: list[str]) -> str:
        if "." in ticker:
            base, suffix = ticker.split(".", 1)
            exchange = self.EXCHANGE_SUFFIX_MAP.get(suffix.upper(), suffix.upper())
            return f"{exchange}|{base}"

        for part in reversed(parts):
            if part in self.CANONICAL_PREFIX:
                prefix = self.CANONICAL_PREFIX[part]
                return f"{prefix}|{ticker}"
        return f"STOOQ|{ticker}"

    def _exchange_for_ticker(self, ticker: str, parts: list[str]) -> str:
        if "cryptocurrencies" in parts:
            return "CRYPTO"
        if "." in ticker:
            _, suffix = ticker.split(".", 1)
            return self.EXCHANGE_SUFFIX_MAP.get(suffix.upper(), suffix.upper())
        for part in reversed(parts):
            if part in self.EXCHANGE_SUFFIX_MAP.values():
                return part
        return ""

    def _find_base_path(self) -> Path | None:
        candidates = [
            self.data_root / "market" / "d_world_txt" / "data" / "daily",
            self.data_root / "datasets" / "market" / "d_world_txt" / "data" / "daily",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _should_skip(self) -> bool:
        last = read_seed_metadata(self.store.conn, "stooq_daily")
        if last is None:
            return False
        return datetime.now(timezone.utc) - last < self.ttl

    def _last_run_age(self) -> timedelta:
        last = read_seed_metadata(self.store.conn, "stooq_daily")
        if last is None:
            return timedelta.max
        return datetime.now(timezone.utc) - last

    def _bump_metadata(self) -> None:
        write_seed_metadata(self.store.conn, "stooq_daily", datetime.now(timezone.utc))
