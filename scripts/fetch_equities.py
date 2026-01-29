from __future__ import annotations

from argparse import ArgumentParser
import logging
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from typing import Sequence

from profit.cache import ColumnarSqliteStore, FileCache
from profit.config import ensure_profit_conf_loaded, get_cache_root, get_columnar_db_path, add_common_cli_args
from profit.sources.equities import (
    ColumnarOhlcvConfig,
    ColumnarOhlcvWriter,
    DAY_US,
    EquityDailyBarsRequest,
    YFinanceDailyBarsFetcher,
)


DATE_FMT = "%Y-%m-%d"
DATE_FMT_HELP = DATE_FMT.replace("%", "%%")


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, DATE_FMT).replace(tzinfo=timezone.utc)


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Fetch daily equity bars and store them in ColumnarSqliteStore.")
    parser.add_argument("--ticker", "-t", required=False, help="Provider symbol (e.g., AAPL)")
    parser.add_argument("--mic", default="XNAS", help="MIC/venue code for the instrument (used in internal ID)")
    parser.add_argument(
        "--start",
        required=False,
        help=f"Inclusive start date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--end",
        required=False,
        help=f"Inclusive end date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--read-fields",
        nargs="+",
        default=["close_raw"],
        help="Field names to read back after ingestion (default: close_raw)",
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Print fetcher capabilities and exit.",
    )
    add_common_cli_args(parser, cache_help_subdir="fetcher", default_store_filename="columnar.sqlite3")
    return parser


def _print_points(
    store: ColumnarSqliteStore,
    dataset: str,
    instrument_id: str,
    field: str,
    start: datetime,
    end: datetime,
) -> None:
    series_id = store.get_series_id(
        instrument_id=instrument_id,
        dataset=dataset,
        field=field,
        step_us=DAY_US,
    )
    if series_id is None:
        print(f"No series for {field} (dataset={dataset})")
        return

    points = store.read_points(
        series_id,
        start=start,
        end=end,
        include_sentinel=False,
    )
    if not points:
        print(f"No points for {field} in requested window.")
        return

    print(f"Stored {len(points)} points for {field}:")
    for ts, value in points:
        print(f"  {ts.date().isoformat()} {value:.6f}")


def main(argv: Sequence[str] | None = None) -> None:
    ensure_profit_conf_loaded()

    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    base_cache_dir = Path(get_cache_root(args=args))
    base_cache_dir.mkdir(parents=True, exist_ok=True)

    yf_cache_dir = base_cache_dir / "yfinance"
    os.environ.setdefault("YFINANCE_CACHE_DIR", str(yf_cache_dir))
    yf_cache_dir.mkdir(parents=True, exist_ok=True)

    store = ColumnarSqliteStore(db_path=get_columnar_db_path(args=args))
    cfg = ColumnarOhlcvConfig()
    dataset = cfg.dataset_name(source="yfinance", version="v1")
    cache = FileCache(base_dir=base_cache_dir / "fetcher")
    fetcher = YFinanceDailyBarsFetcher(cache=cache, store=store)

    if args.describe:
        desc = fetcher.describe()
        print("Fetcher capabilities:")
        print(f"  provider   : {desc.provider}")
        print(f"  dataset    : {desc.dataset}")
        print(f"  version    : {desc.version}")
        print(f"  freqs      : {', '.join(desc.freqs)}")
        print(f"  fields     : {', '.join(desc.fields)}")
        print(f"  max_window : {desc.max_window_days}")
        if desc.notes:
            print(f"  notes      : {desc.notes}")
        return

    for name in ("ticker", "start", "end"):
        if getattr(args, name) is None:
            parser.error(f"--{name} is required unless --describe is used")

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if start > end:
        parser.error("--start must be <= --end")

    request = EquityDailyBarsRequest(
        instrument_id=f"{args.ticker}|{args.mic}",
        provider="yfinance",
        provider_code=args.ticker,
        freq="1d",
    )

    print(f"Ensuring coverage for {args.ticker} {start.date()} → {end.date()} via yfinance...")
    fetcher.timeseries_fetch_many([request], start, end)

    for field in args.read_fields:
        _print_points(store, dataset, request.instrument_id, field, start, end)


if __name__ == "__main__":
    main()
