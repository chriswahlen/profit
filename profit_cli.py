#!/usr/bin/env python3
"""
Lightweight CLI for Profit data tasks.

Usage examples:
  ./profit_cli update --data-source redfin
"""

from __future__ import annotations

import argparse
import sys
import logging

from config import Config
from data_sources.data_source_manager import DataSourceManager
from data_sources.entity import EntityType
from scripts.seed_ticker_list import seed, load_tickers

NASDAQ_TICKER_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/refs/heads/main/nasdaq/nasdaq_tickers.txt"
NYSE_TICKER_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/refs/heads/main/nyse/nyse_tickers.txt"
AMEX_TICKER_URL = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/refs/heads/main/amex/amex_tickers.txt"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="profit_cli", description="Profit data utilities")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    update = sub.add_parser("update", help="Update a data source")
    update.add_argument(
        "--data-source",
        default="redfin",
        help="Data source name to update (default: redfin)",
    )
    update.add_argument(
        "--provider",
        nargs="*",
        help="For multi-provider sources (e.g., market), restrict to these provider names (required for market).",
    )
    update.add_argument(
        "--entities",
        nargs="*",
        default=[],
        help="Optional canonical entity IDs to refresh (default: all for batch sources)",
    )

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

    sub.add_parser("seed-ticker-defaults", help="Seed hard-coded ticker lists (e.g., demo XNAS set)")

    return parser.parse_args()


def _cmd_update(args: argparse.Namespace) -> int:
    cfg = Config()
    manager = DataSourceManager(config=cfg)
    try:
        source = manager.get(args.data_source)
    except KeyError as exc:
        print(f"Unknown data source: {args.data_source}", file=sys.stderr)
        return 2

    kwargs = {"entity_ids": args.entities}
    requested_providers = getattr(args, "provider", None)
    if requested_providers is not None:
        kwargs["providers"] = requested_providers
        available = {getattr(p, "name", None) for p in getattr(source, "providers", [])}
        missing = [p for p in requested_providers if p not in available]
        if missing:
            print(f"Requested providers not available for {args.data_source}: {', '.join(missing)}", file=sys.stderr)
            return 2
    # Require provider for market to avoid ambiguity.
    if source.name == "market" and not kwargs.get("providers"):
        print("For data-source market, you must specify --provider (e.g., stooq, yfinance).", file=sys.stderr)
        return 2

    result = source.ensure_up_to_date(**kwargs)
    detail = f" detail={result.detail}" if result.detail else ""
    print(
        f"{args.data_source} updated={result.updated} skipped={result.skipped} failed={result.failed}{detail}"
    )
    return 0 if result.failed == 0 else 1


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
        }
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
    if args.command == "update":
        return _cmd_update(args)
    if args.command == "seed-regions":
        return _cmd_seed_regions(args)
    if args.command == "seed-sec":
        return _cmd_seed_sec(args)
    if args.command == "seed-ticker-defaults":
        return _cmd_seed_ticker_defaults()
    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
