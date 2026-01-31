from __future__ import annotations

import argparse
from pathlib import Path
import logging
from datetime import timedelta

from profit.cache import FileCache
from profit.catalog import EntityStore
from profit.catalog.seeders import SecCompanyTickerSeeder, OpenExchangeRatesCurrencySeeder
from profit.config import ProfitConfig


def seed_sec(store: EntityStore, cache: FileCache, offline: bool, ttl_days: int) -> None:
    seeder = SecCompanyTickerSeeder(
        cache=cache,
        allow_network=not offline,
        ttl=timedelta(days=ttl_days),
        force=False,
    )
    result = seeder.seed(store)
    logging.info(
        "SEC tickers seeded entities=%s identifiers=%s",
        result.entities_written,
        result.identifiers_written,
    )


def seed_oxr(store: EntityStore, cache: FileCache, offline: bool, ttl_days: int) -> None:
    seeder = OpenExchangeRatesCurrencySeeder(
        cache=cache,
        allow_network=not offline,
        ttl=timedelta(days=ttl_days),
        force=False,
    )
    result = seeder.seed(store)
    logging.info(
        "OXR currencies seeded entities=%s identifiers=%s",
        result.entities_written,
        result.identifiers_written,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all available seeders")
    parser.add_argument("--offline", action="store_true", help="Disable network fetch; require cached data/fetch_fn")
    parser.add_argument("--ttl-days", type=int, default=7, help="Cache TTL for network fetches (default 7)")
    parser.add_argument("--data-root", type=Path, default=None, help="Override PROFIT_DATA_ROOT")
    parser.add_argument("--cache-root", type=Path, default=None, help="Override PROFIT_CACHE_ROOT")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default INFO)")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    data_root = args.data_root or ProfitConfig.resolve_data_root()
    cache_root = args.cache_root or ProfitConfig.resolve_cache_root()

    store = EntityStore(data_root / "entities.sqlite3")
    cache = FileCache(base_dir=cache_root / "seed_cache")

    seed_oxr(store, cache, args.offline, args.ttl_days)
    seed_sec(store, cache, args.offline, args.ttl_days)


if __name__ == "__main__":
    main()
