#!/usr/bin/env python3
"""Fetcher CLI for Profit network data tasks.

Commands:
  fetch-edgar            Fetch SEC EDGAR submissions for CIKs
"""

from __future__ import annotations

import argparse
import logging
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fetch_cli", description="Profit fetch utilities")
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    edgar = sub.add_parser("fetch-edgar", help="Fetch SEC EDGAR submissions for one or more CIKs")
    edgar.add_argument("ciks", nargs="+", help="One or more CIKs to fetch/update")
    edgar.add_argument(
        "--pause-s",
        type=float,
        default=0.0,
        help="Optional pause between requests (seconds). Default: 0.0",
    )
    edgar.add_argument(
        "--process-filings",
        action="store_true",
        help="Also download each filing's XML/TXT and ingest EDGAR XBRL facts.",
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    if args.command == "fetch-edgar":
        from scripts.fetch_edgar import main as fetch_edgar_main

        # Delegate argument parsing to the underlying script to keep behavior consistent.
        argv: list[str] = []
        if args.pause_s:
            argv += ["--pause-s", str(args.pause_s)]
        argv += list(args.ciks)
        if args.process_filings:
            argv.append("--process-filings")
        return fetch_edgar_main(argv)

    print(f"Unknown command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
