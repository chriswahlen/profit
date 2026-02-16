#!/usr/bin/env python3
"""Seeder CLI for Profit data tasks.

Commands:
  seed-regions           Seed canonical regions (countries + states/provinces)
  seed-sec               Seed SEC company tickers/entities
  seed-equities          Seed FinanceDatabase equities metadata
  seed-cryptos           Seed FinanceDatabase crypto metadata
  seed-exchanges         Seed exchange (market venue) entities
  seed-currencies        Seed ISO 4217 currencies
  seed-all               Run currencies -> regions -> exchanges -> sec
"""

from __future__ import annotations

import argparse
import logging
import sys

from config import Config


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

    seed_fd = sub.add_parser("seed-equities", help="Seed from FinanceDatabase CSV exports")
    seed_fd.add_argument("--csv", required=True, help="Path to FinanceDatabase CSV (e.g., equities.csv)")
    seed_fd.add_argument(
        "--asset-class",
        default="equities",
        choices=["equities"],
        help="Asset class to load (currently supports equities)",
    )
    seed_fd.add_argument("--limit", type=int, help="Optional row limit for testing")

    sub.add_parser("seed-exchanges", help="Seed exchange (market venue) entities")
    seed_crypto = sub.add_parser("seed-cryptos", help="Seed FinanceDatabase crypto metadata")
    seed_crypto.add_argument("--csv", required=True, help="Path to FinanceDatabase crypto CSV (e.g., cryptos.csv)")
    seed_crypto.add_argument("--limit", type=int, help="Optional row limit for testing")
    sub.add_parser("seed-currencies", help="Seed ISO 4217 currencies")
    sub.add_parser("seed-all", help="Run all seeds: currencies -> regions -> exchanges -> SEC")

    return parser.parse_args()


def _cmd_seed_regions(args: argparse.Namespace) -> int:
    from scripts.seed_regions import seed_regions

    cfg = Config()
    seed_regions(config=cfg, countries=args.countries)
    return 0


def _cmd_seed_sec(args: argparse.Namespace) -> int:
    from scripts.seed_sec_tickers import main as seed_sec_main
    if args.local_json:
        return seed_sec_main([args.local_json])  # type: ignore[arg-type]
    return seed_sec_main([])


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    if args.command == "seed-regions":
        return _cmd_seed_regions(args)
    if args.command == "seed-sec":
        return _cmd_seed_sec(args)
    if args.command == "seed-equities":
        from scripts.seed_equities import main as fd_main
        fd_args = ["--csv", args.csv, "--asset-class", args.asset_class]
        if args.limit:
            fd_args += ["--limit", str(args.limit)]
        return fd_main(fd_args)
    if args.command == "seed-cryptos":
        from scripts.seed_cryptos import main as crypto_main
        crypto_args = ["--csv", args.csv]
        if args.limit:
            crypto_args += ["--limit", str(args.limit)]
        return crypto_main(crypto_args)
    if args.command == "seed-exchanges":
        from scripts.seed_exchanges import main as seed_ex_main
        return seed_ex_main([])
    if args.command == "seed-currencies":
        from scripts.seed_currencies import seed_currencies
        seed_currencies(config=Config())
        return 0
    if args.command == "seed-all":
        from scripts.seed_currencies import seed_currencies
        status = 0
        try:
            seed_currencies(config=Config())
        except Exception:
            status = 1
        region_rc = _cmd_seed_regions(argparse.Namespace(countries=None))
        from scripts.seed_exchanges import main as seed_ex_main
        exch_rc = seed_ex_main([])
        sec_rc = _cmd_seed_sec(argparse.Namespace(local_json=None))
        return 0 if status == region_rc == exch_rc == sec_rc == 0 else 1

    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
