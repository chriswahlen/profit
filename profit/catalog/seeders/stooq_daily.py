from __future__ import annotations

import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from profit.catalog import InstrumentRecord
from profit.catalog.store import CatalogStore
from profit.catalog.seeders.stooq_common import (
    canonical_instrument_id,
    exchange_for_ticker,
    guess_type,
)
from profit.seed_metadata import ensure_seed_metadata, read_seed_metadata, write_seed_metadata


@dataclass(frozen=True)
class SeedResult:
    instruments_written: int


class StooqDailySeeder:
    """
    Seed the Stooq daily dataset into the catalog.
    """

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
        zip_path = self._find_zip_path()
        if zip_path is None:
            logging.warning("Stooq dataset zip missing; expected one of: %s", ", ".join(map(str, self._zip_candidates())))
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

        instruments = list(self._discover_instruments(zip_path))
        if not instruments:
            logging.info("Stooq seeder found no instruments in %s", zip_path)
            return SeedResult(instruments_written=0)

        written = self.store.upsert_instruments(instruments)
        logging.info("Stooq seeder wrote %s instruments", written)
        self._bump_metadata()
        return SeedResult(instruments_written=written)

    def _discover_instruments(self, zip_path: Path) -> Iterable[InstrumentRecord]:
        with zipfile.ZipFile(zip_path) as zf:
            for info in zf.infolist():
                if not info.filename.lower().endswith(".txt"):
                    continue
                # expected: data/daily/<...>/ticker.txt
                parts = Path(info.filename).parts
                if len(parts) < 4:
                    continue
                rel_parts = [p.lower() for p in parts[2:-1]]  # keep from <region>/<...>
                category = "/".join(rel_parts) if rel_parts else "unknown"
                ticker = Path(info.filename).stem.upper()
                instrument_id = canonical_instrument_id(ticker, rel_parts)
                instrument_type = guess_type(rel_parts, ticker)
                attrs = {"category": category, "path": info.filename}

                yield InstrumentRecord(
                    instrument_id=instrument_id,
                    instrument_type=instrument_type,
                    provider=self.provider,
                    provider_code=ticker,
                    mic=exchange_for_ticker(ticker, rel_parts),
                    currency=None,
                    active_from=None,
                    active_to=None,
                    attrs=attrs,
                )

    def _find_zip_path(self) -> Path | None:
        for candidate in self._zip_candidates():
            if candidate.exists():
                return candidate
        return None

    def _zip_candidates(self) -> list[Path]:
        return [
            self.data_root / "datasets" / "stooq" / "d_world_txt.zip",
            self.data_root / "stooq" / "d_world_txt.zip",
        ]

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
