from __future__ import annotations

import csv
import logging
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from profit.catalog import CatalogService, CatalogStore, InstrumentRecord
from profit.config import ensure_profit_conf_loaded, get_catalog_db_path


MAJOR_FX_PAIRS = [
    "EURUSD",
    "USDJPY",
    "GBPUSD",
    "USDCHF",
    "AUDUSD",
    "USDCAD",
    "NZDUSD",
    "EURJPY",
    "EURGBP",
    "EURCHF",
]


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Load provider instrument metadata into the catalog.")
    sub = parser.add_subparsers(dest="provider", required=True)

    # yfinance equities
    p_eq = sub.add_parser("yfinance-equities", help="Load equities from a CSV symbol list.")
    p_eq.add_argument("--csv", required=True, type=Path, help="CSV with ticker column.")
    p_eq.add_argument("--ticker-col", default="Symbol", help="Column name for ticker (default: Symbol).")
    p_eq.add_argument("--mic-col", default=None, help="Optional column for MIC/venue.")
    p_eq.add_argument("--currency-col", default=None, help="Optional column for currency.")
    p_eq.add_argument("--default-mic", default="XNAS", help="Fallback MIC when column missing (default: XNAS).")
    p_eq.add_argument("--default-currency", default="USD", help="Fallback currency when column missing.")

    # yfinance fx
    p_fx = sub.add_parser("yfinance-fx", help="Load FX pairs.")
    p_fx.add_argument(
        "--pairs",
        default=None,
        help="Comma-separated list of pairs like EURUSD; defaults to a major-pairs set.",
    )
    p_fx.add_argument(
        "--pairs-file",
        type=Path,
        default=None,
        help="Optional file with one pair per line; overrides --pairs if provided.",
    )
    p_fx.add_argument(
        "--provider-code-suffix",
        default="=X",
        help="Suffix appended to pair for provider_code (default: =X for yfinance).",
    )

    # goldapi
    sub.add_parser("goldapi", help="Load gold/silver instrument rows for goldapi provider.")

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


def _now() -> datetime:
    return datetime.now(timezone.utc)


def load_yfinance_equities(args, store: CatalogStore) -> int:
    ticker_col = args.ticker_col
    mic_col = args.mic_col
    currency_col = args.currency_col

    rows: list[InstrumentRecord] = []
    with args.csv.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ticker = (row.get(ticker_col) or "").strip()
            if not ticker:
                continue
            mic = (row.get(mic_col) or "").strip() if mic_col else None
            currency = (row.get(currency_col) or "").strip() if currency_col else None
            mic = mic or args.default_mic
            currency = currency or args.default_currency
            instrument_id = f"{ticker}|{mic}"
            rows.append(
                InstrumentRecord(
                    instrument_id=instrument_id,
                    instrument_type="equity",
                    provider="yfinance",
                    provider_code=ticker,
                    mic=mic,
                    currency=currency,
                    active_from=_now(),
                    active_to=None,
                    attrs={},
                )
            )
    return store.upsert_instruments(rows)


def _load_pairs_from_args(args) -> list[str]:
    if args.pairs_file:
        text = args.pairs_file.read_text().splitlines()
        return [line.strip().upper() for line in text if line.strip()]
    if args.pairs:
        return [p.strip().upper() for p in args.pairs.split(",") if p.strip()]
    return MAJOR_FX_PAIRS


def load_yfinance_fx(args, store: CatalogStore) -> int:
    pairs = _load_pairs_from_args(args)
    now = _now()
    records = []
    for pair in pairs:
        if len(pair) < 6:
            continue
        base = pair[:3]
        quote = pair[3:6]
        instrument_id = f"{base}/{quote}"
        provider_code = f"{pair}{args.provider_code_suffix}"
        records.append(
            InstrumentRecord(
                instrument_id=instrument_id,
                instrument_type="fx_pair",
                provider="yfinance",
                provider_code=provider_code,
                mic=None,
                currency=None,
                active_from=now,
                active_to=None,
                attrs={"base": base, "quote": quote},
            )
        )
    return store.upsert_instruments(records)


def load_goldapi(_args, store: CatalogStore) -> int:
    now = _now()
    rows = [
        InstrumentRecord(
            instrument_id="XAU|LBMA",
            instrument_type="commodity",
            provider="goldapi",
            provider_code="XAU",
            mic=None,
            currency="USD",
            active_from=now,
            active_to=None,
            attrs={"name": "gold"},
        ),
        InstrumentRecord(
            instrument_id="XAG|LBMA",
            instrument_type="commodity",
            provider="goldapi",
            provider_code="XAG",
            mic=None,
            currency="USD",
            active_from=now,
            active_to=None,
            attrs={"name": "silver"},
        ),
    ]
    return store.upsert_instruments(rows)


def main(argv: Sequence[str] | None = None) -> None:
    ensure_profit_conf_loaded()
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    db_path = get_catalog_db_path(args=args)
    store = CatalogStore(db_path, readonly=False)
    service = CatalogService(store)

    if args.provider == "yfinance-equities":
        written = load_yfinance_equities(args, store)
    elif args.provider == "yfinance-fx":
        written = load_yfinance_fx(args, store)
    elif args.provider == "goldapi":
        written = load_goldapi(args, store)
    else:
        parser.error(f"Unknown provider {args.provider}")
        return

    print(f"Upserted {written} instrument rows into catalog at {db_path}")


if __name__ == "__main__":
    main()
