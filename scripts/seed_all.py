from __future__ import annotations

import argparse
import logging
from pathlib import Path
from datetime import timedelta

from profit.cache import FileCache
from profit.catalog.seeders import (
    OpenExchangeRatesCurrencySeeder,
    SecCompanyTickerSeeder,
    StooqDailySeeder,
    StooqUsEquitySeeder,
    StooqUsHistorySeeder,
    StooqWorldHistorySeeder,
)
from profit.config import ProfitConfig
from profit.stores import StoreContainer


def seed_sec(store: EntityStore, catalog: CatalogStore, cache: FileCache, offline: bool, ttl_days: int, force: bool) -> None:
    seeder = SecCompanyTickerSeeder(
        cache=cache,
        allow_network=not offline,
        ttl=timedelta(days=ttl_days),
        force=force,
    )
    result = seeder.seed(store, catalog=catalog)
    logging.info(
        "SEC tickers seeded entities=%s identifiers=%s instrument_links=%s",
        result.entities_written,
        result.identifiers_written,
        result.instrument_links_written,
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


def seed_stooq_us(store: CatalogStore, data_root: Path, *, force: bool, ttl_days: int) -> None:
    seeder = StooqUsEquitySeeder(
        store=store,
        data_root=data_root,
        force=force,
        ttl=timedelta(days=ttl_days),
    )
    result = seeder.seed()
    logging.info("Stooq US instruments seeded=%s", result.instruments_written)


def seed_stooq_us_history(sql_store: ColumnarSqliteStore, data_root: Path, *, force: bool, ttl_days: int) -> None:
    seeder = StooqUsHistorySeeder(
        store=sql_store,
        data_root=data_root,
        force=force,
        ttl=timedelta(days=ttl_days),
    )
    result = seeder.seed()
    logging.info("Stooq US history seeded rows=%s", result.rows_written)


def seed_stooq_world_history(sql_store: ColumnarSqliteStore, data_root: Path, *, force: bool, ttl_days: int) -> None:
    seeder = StooqWorldHistorySeeder(
        store=sql_store,
        data_root=data_root,
        force=force,
        ttl=timedelta(days=ttl_days),
    )
    result = seeder.seed()
    logging.info("Stooq world history seeded rows=%s", result.rows_written)


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
    parser.add_argument(
        "--force-only",
        action="append",
        dest="force_only",
        default=[],
        help="Force only specific seeders (repeatable or comma-separated); overrides TTL just for those.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    data_root = args.data_root or ProfitConfig.resolve_data_root()
    cache_root = args.cache_root or ProfitConfig.resolve_cache_root()

    profit_db = data_root / "profit.sqlite"
    stores = StoreContainer.open(profit_db)
    cache = FileCache(base_dir=cache_root / "seed_cache")

    force_targets = {item.strip().lower() for entry in args.force_only for item in entry.split(",") if item.strip()}
    force_all = args.force

    def _force(name: str) -> bool:
        return force_all or name in force_targets

    seed_oxr(stores.entity, cache, args.offline, args.ttl_days, force=_force("oxr"))
    register_stooq_provider(stores.entity)
    seed_stooq_us(stores.catalog, data_root, force=_force("stooq_us"), ttl_days=args.ttl_days)
    seed_stooq_us_history(stores.columnar, data_root, force=_force("stooq_us_history"), ttl_days=args.ttl_days)
    seed_stooq_world_history(stores.columnar, data_root, force=_force("stooq_world_history"), ttl_days=args.ttl_days)
    seed_stooq(stores.catalog, data_root, force=_force("stooq"), ttl_days=args.ttl_days)
    seed_sec(stores.entity, stores.catalog, cache, args.offline, args.ttl_days, force=_force("sec"))
    stores.close()


if __name__ == "__main__":
    main()
