#!/usr/bin/env python3
"""Seeder CLI for Profit data tasks.

Commands:
  seed-regions           Seed canonical regions (countries + states/provinces)
  seed-sec               Seed SEC company tickers/entities
  seed-equities          Seed FinanceDatabase equities metadata
  seed-cryptos           Seed FinanceDatabase crypto metadata
  seed-exchanges         Seed exchange (market venue) entities
  seed-etfs             Seed FinanceDatabase ETF metadata
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

    sub.add_parser("seed-exchanges", help="Seed exchange (market venue) entities")
    sub.add_parser("seed-currencies", help="Seed ISO 4217 currencies")

    # Financial Database imports
    seed_fd = sub.add_parser("seed-equities", help="Seed from FinanceDatabase CSV exports")
    seed_fd.add_argument("--limit", type=int, help="Optional row limit for testing")
    seed_crypto = sub.add_parser("seed-cryptos", help="Seed FinanceDatabase crypto metadata")
    seed_crypto.add_argument("--limit", type=int, help="Optional row limit for testing")
    seed_etf = sub.add_parser("seed-etfs", help="Seed FinanceDatabase ETF metadata")
    seed_etf.add_argument("--limit", type=int, help="Optional row limit for testing")
    seed_indices = sub.add_parser("seed-indices", help="Seed FinanceDatabase index metadata")
    seed_indices.add_argument("--limit", type=int, help="Optional row limit for testing")
    seed_mm = sub.add_parser("seed-moneymarkets", help="Seed FinanceDatabase money market metadata")
    seed_mm.add_argument("--limit", type=int, help="Optional row limit for testing")
    seed_funds = sub.add_parser("seed-funds", help="Seed FinanceDatabase fund metadata")
    seed_funds.add_argument("--limit", type=int, help="Optional row limit for testing")

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
        fd_args: list[str] = []
        if args.limit:
            fd_args += ["--limit", str(args.limit)]
        return fd_main(fd_args)
    if args.command == "seed-cryptos":
        from scripts.seed_cryptos import main as crypto_main
        crypto_args: list[str] = []
        if args.limit:
            crypto_args += ["--limit", str(args.limit)]
        return crypto_main(crypto_args)
    if args.command == "seed-etfs":
        from scripts.seed_etfs import main as etf_main
        etf_args: list[str] = []
        if args.limit:
            etf_args += ["--limit", str(args.limit)]
        return etf_main(etf_args)
    if args.command == "seed-indices":
        from scripts.seed_indices import main as idx_main
        idx_args: list[str] = []
        if args.limit:
            idx_args += ["--limit", str(args.limit)]
        return idx_main(idx_args)
    if args.command == "seed-moneymarkets":
        from scripts.seed_moneymarkets import main as mm_main
        mm_args: list[str] = []
        if args.limit:
            mm_args += ["--limit", str(args.limit)]
        return mm_main(mm_args)
    if args.command == "seed-funds":
        from scripts.seed_funds import main as fund_main
        fund_args: list[str] = []
        if args.limit:
            fund_args += ["--limit", str(args.limit)]
        return fund_main(fund_args)
    if args.command == "seed-exchanges":
        from scripts.seed_exchanges import main as seed_ex_main
        return seed_ex_main([])
    if args.command == "seed-currencies":
        from scripts.seed_currencies import seed_currencies
        seed_currencies(config=Config())
        return 0
    if args.command == "seed-all":
        from scripts.seed_currencies import seed_currencies
        from scripts.seed_cryptos import main as crypto_main
        from scripts.seed_etfs import main as etf_main
        from scripts.seed_indices import main as idx_main
        from scripts.seed_moneymarkets import main as mm_main
        from scripts.seed_funds import main as fund_main
        from scripts.seed_equities import main as equities_main
        status = 0
        try:
            seed_currencies(config=Config())
        except Exception:
            status = 1
        region_rc = _cmd_seed_regions(argparse.Namespace(countries=None))
        from scripts.seed_exchanges import main as seed_ex_main
        exch_rc = seed_ex_main([])
        crypto_rc = crypto_main([])
        etf_rc = etf_main([])
        idx_rc = idx_main([])
        mm_rc = mm_main([])
        fund_rc = fund_main([])
        equities_rc = equities_main([])
        sec_rc = _cmd_seed_sec(argparse.Namespace(local_json=None))
        return 0 if status == region_rc == exch_rc == crypto_rc == etf_rc == idx_rc == mm_rc == fund_rc == sec_rc == 0 else 1

    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
