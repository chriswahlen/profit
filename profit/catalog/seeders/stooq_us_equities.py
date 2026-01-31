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


class StooqUsEquitySeeder:
    """
    Seed U.S. equities/ETFs from the Stooq `d_us_txt` dataset.
    """

    MIC_BY_FOLDER = {
        "nyse": "XNYS",
        "nasdaq": "XNAS",
        "nysemkt": "XASE",
    }

    TYPE_BY_FOLDER = {
        "stocks": "equity",
        "etfs": "etf",
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
            logging.warning("Stooq US dataset base path missing under %s", self.data_root)
            return SeedResult(instruments_written=0)

        ensure_seed_metadata(self.store.conn)
        if not self.force and self._should_skip():
            age = self._last_run_age()
            remaining = max(self.ttl - age, timedelta(0))
            logging.info(
                "Stooq US seeder skipped: last_run_age=%s ttl=%s next_refresh_in=%s",
                age,
                self.ttl,
                remaining,
            )
            return SeedResult(instruments_written=0)

        instruments = list(self._discover_instruments(base))
        if not instruments:
            logging.info("Stooq US seeder found no instruments under %s", base)
            return SeedResult(instruments_written=0)

        written = self.store.upsert_instruments(instruments)
        logging.info("Stooq US seeder wrote %s instruments", written)
        self._bump_metadata()
        return SeedResult(instruments_written=written)

    # ------------------------------------------------------------------
    def _discover_instruments(self, base: Path) -> Iterable[InstrumentRecord]:
        for txt in base.rglob("*.txt"):
            relative = txt.relative_to(base)
            # expected: us/<venue> <kind>/<bucket>/TICKER.us.txt
            parts = [p.lower() for p in relative.parts]
            if len(parts) < 2:
                continue
            venue_and_kind = parts[0]  # e.g., "nyse stocks"
            venue_tokens = venue_and_kind.split()
            venue = venue_tokens[0]
            kind = venue_tokens[1] if len(venue_tokens) > 1 else "stocks"
            ticker = txt.stem.upper()  # includes .US

            mic = self.MIC_BY_FOLDER.get(venue, "")
            instrument_type = self.TYPE_BY_FOLDER.get(kind, "equity")

            # canonical id: <MIC>|<TICKER> without .US suffix
            base_ticker = ticker.split(".", 1)[0]
            instrument_id = f"{mic or 'STOOQ'}|{base_ticker}"

            attrs = {
                "category": "/".join(parts[:-1]),
                "path": str(txt),
                "venue": venue,
                "kind": kind,
            }

            yield InstrumentRecord(
                instrument_id=instrument_id,
                instrument_type=instrument_type,
                provider=self.provider,
                provider_code=ticker,
                mic=mic,
                currency="USD",
                active_from=None,
                active_to=None,
                attrs=attrs,
            )

    def _find_base_path(self) -> Path | None:
        candidates = [
            self.data_root / "market" / "d_us_txt" / "data" / "daily" / "us",
            self.data_root / "datasets" / "market" / "d_us_txt" / "data" / "daily" / "us",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _should_skip(self) -> bool:
        last = read_seed_metadata(self.store.conn, "stooq_us")
        if last is None:
            return False
        return datetime.now(timezone.utc) - last < self.ttl

    def _last_run_age(self) -> timedelta:
        last = read_seed_metadata(self.store.conn, "stooq_us")
        if last is None:
            return timedelta.max
        return datetime.now(timezone.utc) - last

    def _bump_metadata(self) -> None:
        write_seed_metadata(self.store.conn, "stooq_us", datetime.now(timezone.utc))
