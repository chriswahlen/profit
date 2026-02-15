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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="profit_cli", description="Profit data utilities")
    sub = parser.add_subparsers(dest="command", required=True)

    update = sub.add_parser("update", help="Update a data source")
    update.add_argument(
        "--data-source",
        default="redfin",
        help="Data source name to update (default: redfin)",
    )

    seed = sub.add_parser("seed-regions", help="Seed canonical regions (countries + states/provinces)")
    seed.add_argument(
        "--countries",
        nargs="*",
        help="Optional list of country ISO2 codes to seed (default: all)",
    )

    return parser.parse_args()


def _cmd_update(args: argparse.Namespace) -> int:
    cfg = Config()
    manager = DataSourceManager(config=cfg)
    try:
        source = manager.get(args.data_source)
    except KeyError as exc:
        print(f"Unknown data source: {args.data_source}", file=sys.stderr)
        return 2

    result = source.ensure_up_to_date(entity_ids=[])
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


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    if args.command == "update":
        return _cmd_update(args)
    if args.command == "seed-regions":
        return _cmd_seed_regions(args)
    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
