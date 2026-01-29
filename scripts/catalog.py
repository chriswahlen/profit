from __future__ import annotations

from argparse import ArgumentParser
import logging
from pathlib import Path
from typing import Sequence

from profit.catalog import CatalogService, CatalogStore
from profit.config import ensure_profit_conf_loaded, get_catalog_db_path


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Query the instrument catalog (read-only).")
    parser.add_argument("--query", "-q", default=None, help="Substring match against provider_code or instrument_id.")
    parser.add_argument("--provider", default=None, help="Filter by provider code (e.g., yfinance, goldapi).")
    parser.add_argument("--limit", type=int, default=20, help="Max rows to return (default: 20).")
    parser.add_argument("--offset", type=int, default=0, help="Row offset for paging.")
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=None,
        help="Path to catalog SQLite DB (default: PROFIT_DATA_ROOT/catalog.sqlite3).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING...). Default: INFO",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    ensure_profit_conf_loaded()
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    db_path = get_catalog_db_path(args=args)
    store = CatalogStore(db_path, readonly=True)
    service = CatalogService(store)

    rows = service.search_instruments(
        query=args.query,
        provider=args.provider,
        limit=args.limit,
        offset=args.offset,
    )

    if not rows:
        print("No matching instruments.")
        return

    print(f"Found {len(rows)} instruments (offset {args.offset}):")
    for r in rows:
        active_to = r.active_to.isoformat() if r.active_to else "open"
        print(
            f"- {r.provider}:{r.provider_code} -> {r.instrument_id} "
            f"({r.instrument_type}, mic={r.mic or '-'}, ccy={r.currency or '-'}, "
            f"active {r.active_from.isoformat()} … {active_to})"
        )


if __name__ == "__main__":
    main()
