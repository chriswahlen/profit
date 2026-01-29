from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from typing import Sequence

from profit.cache import ColumnarSqliteStore, FileCache
from profit.cache.file_cache import _default_cache_dir
from profit.config import ensure_profit_conf_loaded, get_data_root
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
    parser.add_argument("--ticker", "-t", required=True, help="Provider symbol (e.g., AAPL)")
    parser.add_argument("--mic", default="XNAS", help="MIC/venue code for the instrument (used in internal ID)")
    parser.add_argument(
        "--start",
        required=True,
        help=f"Inclusive start date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help=f"Inclusive end date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Directory for yfinance cache (default: PROFIT_CACHE_* + '/fetcher')",
    )
    parser.add_argument(
        "--store-path",
        type=Path,
        default=None,
        help="Path to ColumnarSqliteStore (default: PROFIT_CACHE_* + '/columnar.sqlite3')",
    )
    parser.add_argument(
        "--read-fields",
        nargs="+",
        default=["close_raw"],
        help="Field names to read back after ingestion (default: close_raw)",
    )
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


def main() -> None:
    ensure_profit_conf_loaded()

    parser = _build_parser()
    args = parser.parse_args()

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

    base_cache_dir = args.cache_dir or Path(
        os.environ.get("PROFIT_CACHE_DIR")
        or os.environ.get("PROFIT_CACHE_ROOT")
        or _default_cache_dir()
    )
    base_cache_dir = Path(base_cache_dir)
    base_cache_dir.mkdir(parents=True, exist_ok=True)

    yf_cache_dir = base_cache_dir / "yfinance"
    os.environ.setdefault("YFINANCE_CACHE_DIR", str(yf_cache_dir))
    yf_cache_dir.mkdir(parents=True, exist_ok=True)

    cache = FileCache(base_dir=base_cache_dir / "fetcher")
    fetcher = YFinanceDailyBarsFetcher(cache=cache, max_window_days=30)
    print(f"Fetching {args.ticker} bars {start.date()} → {end.date()} via yfinance...")
    bars = fetcher.timeseries_fetch(request, start, end)

    if not bars:
        print("Provider returned no bars.")
        return

    store_path = args.store_path or get_data_root() / "columnar.sqlite3"
    store = ColumnarSqliteStore(db_path=store_path)
    writer = ColumnarOhlcvWriter(store)
    counts = writer.write_daily_bars(bars)
    print("Written fields:")
    for field, count in counts.items():
        print(f"  {field}: {count} points")

    cfg = ColumnarOhlcvConfig()
    dataset = cfg.dataset_name(source=bars[0].source, version=bars[0].version)
    for field in args.read_fields:
        _print_points(store, dataset, request.instrument_id, field, start, end)


if __name__ == "__main__":
    main()
