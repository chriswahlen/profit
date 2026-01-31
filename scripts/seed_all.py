from __future__ import annotations

import argparse
from pathlib import Path
import logging
from datetime import timedelta

from profit.cache import FileCache
from profit.catalog import EntityStore
from profit.catalog.seeders import (
    OpenExchangeRatesCurrencySeeder,
    SecCompanyTickerSeeder,
    StooqDailySeeder,
)
from profit.catalog.store import CatalogStore
from profit.config import ProfitConfig


def seed_sec(store: EntityStore, cache: FileCache, offline: bool, ttl_days: int, force: bool) -> None:
    seeder = SecCompanyTickerSeeder(
        cache=cache,
        allow_network=not offline,
        ttl=timedelta(days=ttl_days),
        force=force,
    )
    result = seeder.seed(store)
    logging.info(
        "SEC tickers seeded entities=%s identifiers=%s",
        result.entities_written,
        result.identifiers_written,
    )


def seed_oxr(store: EntityStore, cache: FileCache, offline: bool, ttl_days: int, force: bool) -> None:
    seeder = OpenExchangeRatesCurrencySeeder(
        cache=cache,
        allow_network=not offline,
        ttl=timedelta(days=ttl_days),
        force=force,
    )
    result = seeder.seed(store)
    logging.info(
        "OXR currencies seeded entities=%s identifiers=%s",
        result.entities_written,
        result.identifiers_written,
    )


def seed_stooq(store: CatalogStore, data_root: Path, *, force: bool, ttl_days: int) -> None:
    seeder = StooqDailySeeder(
        store=store,
        data_root=data_root,
        force=force,
        ttl=timedelta(days=ttl_days),
    )
    result = seeder.seed()
    logging.info("Stooq instruments seeded=%s", result.instruments_written)


def register_stooq_provider(store: EntityStore) -> None:
    store.upsert_providers([("stooq", "Stooq Daily", "Stooq daily dataset download")])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all available seeders")
    parser.add_argument("--offline", action="store_true", help="Disable network fetch; require cached data/fetch_fn")
    parser.add_argument("--ttl-days", type=int, default=7, help="Cache TTL for network fetches (default 7)")
    parser.add_argument("--data-root", type=Path, default=None, help="Override PROFIT_DATA_ROOT")
    parser.add_argument("--cache-root", type=Path, default=None, help="Override PROFIT_CACHE_ROOT")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default INFO)")
    parser.add_argument("--force", action="store_true", help="Force re-seed even when cache is fresh")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    data_root = args.data_root or ProfitConfig.resolve_data_root()
    cache_root = args.cache_root or ProfitConfig.resolve_cache_root()

    profit_db = data_root / "profit.sqlite"
    store = EntityStore(profit_db)
    cache = FileCache(base_dir=cache_root / "seed_cache")
    catalog = CatalogStore(profit_db)

    seed_oxr(store, cache, args.offline, args.ttl_days, force=args.force)
    register_stooq_provider(store)
    seed_stooq(catalog, data_root, force=args.force, ttl_days=args.ttl_days)
    seed_sec(store, cache, args.offline, args.ttl_days, force=args.force)


if __name__ == "__main__":
    main()
