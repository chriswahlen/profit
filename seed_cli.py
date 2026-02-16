#!/usr/bin/env python3
"""Seeder CLI for Profit data tasks.

Commands:
  seed-regions           Seed canonical regions (countries + states/provinces)
  seed-sec               Seed SEC company tickers/entities
  seed-ticker-defaults   Seed hard-coded US exchange ticker lists (NASDAQ/NYSE/AMEX)
"""

from __future__ import annotations

import argparse
import logging
import sys

from config import Config
from data_sources.data_source_manager import DataSourceManager
from data_sources.entity import EntityType
from scripts.seed_ticker_list import seed, load_tickers

NASDAQ_TICKER_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/refs/heads/main/nasdaq/nasdaq_tickers.txt"
NYSE_TICKER_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/refs/heads/main/nyse/nyse_tickers.txt"
AMEX_TICKER_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/refs/heads/main/amex/amex_tickers.txt"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="seed_cli", description="Profit seeding utilities")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed-regions", help="Seed canonical regions (countries + states/provinces)")
    seed.add_argument(
        "--countries",
        nargs="*",
        help="Optional list of country ISO2 codes to seed (default: all)",
    )

    seed_sec = sub.add_parser("seed-sec", help="Seed SEC company tickers/entities")
    seed_sec.add_argument(
        "--local-json",
        help="Optional local path to company_tickers.json (otherwise fetches from SEC)",
    )

    sub.add_parser("seed-ticker-defaults", help="Seed hard-coded ticker lists for US exchanges (XNAS/XNYS/XASE)")
    sub.add_parser("seed-all", help="Run all seeds: regions -> ticker-defaults -> SEC")

    return parser.parse_args()


def _cmd_seed_regions(args: argparse.Namespace) -> int:
    from scripts.seed_regions import seed_regions

    cfg = Config()
    seed_regions(config=cfg, countries=args.countries)
    return 0


def _cmd_seed_sec(args: argparse.Namespace) -> int:
    from scripts.seed_sec_tickers import main as seed_sec_main
    # Reuse script's entry; pass local path if provided.
    if args.local_json:
        return seed_sec_main([args.local_json])  # type: ignore[arg-type]
    return seed_sec_main([])


def _cmd_seed_ticker_defaults() -> int:
    """Seed a small set of hard-coded ticker lists for quick demos/tests."""

    targets = [
        {
            "name": "nasdaq-xnas-all",
            "mic": "XNAS",
            "entity_type": EntityType.SECURITY,
            "provider": "provider:nasdaq-tickers",
            "source": NASDAQ_TICKER_URL,
        },
        {
            "name": "nyse-xnys-all",
            "mic": "XNYS",
            "entity_type": EntityType.SECURITY,
            "provider": "provider:nyse-tickers",
            "source": NYSE_TICKER_URL,
        },
        {
            "name": "amex-xase-all",
            "mic": "XASE",
            "entity_type": EntityType.SECURITY,
            "provider": "provider:amex-tickers",
            "source": AMEX_TICKER_URL,
        },
    ]

    cfg = Config()
    manager = DataSourceManager(config=cfg)
    entity_store = manager.entity_store  # reuse manager-owned store for consistency

    total_seeded = 0
    for target in targets:
        tickers = load_tickers(target["source"])
        seed(
            mic=target["mic"],
            entity_type=target["entity_type"],
            tickers=tickers,
            provider=target["provider"],
            store=entity_store,
        )
        total_seeded += len(tickers)

    print(f"Seeded {total_seeded} tickers across {len(targets)} presets")
    return 0


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    if args.command == "seed-regions":
        return _cmd_seed_regions(args)
    if args.command == "seed-sec":
        return _cmd_seed_sec(args)
    if args.command == "seed-ticker-defaults":
        return _cmd_seed_ticker_defaults()
    if args.command == "seed-all":
        # Order: regions -> ticker defaults -> SEC
        region_rc = _cmd_seed_regions(argparse.Namespace(countries=None))
        ticker_rc = _cmd_seed_ticker_defaults()
        sec_rc = _cmd_seed_sec(argparse.Namespace(local_json=None))
        return 0 if region_rc == ticker_rc == sec_rc == 0 else 1
    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
