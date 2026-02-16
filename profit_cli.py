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


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")
    if args.command == "update":
        return _cmd_update(args)
    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
