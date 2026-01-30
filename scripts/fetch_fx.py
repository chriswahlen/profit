from __future__ import annotations

from argparse import ArgumentParser
import logging
from datetime import datetime, timedelta, timezone

from profit.cache import ColumnarSqliteStore, FileCache
from profit.config import (
    ProfitConfig,
    ensure_profit_conf_loaded,
    add_common_cli_args,
    apply_runtime_env,
)
from profit.sources.fx import (
    ColumnarFxConfig,
    ColumnarFxWriter,
    DAY_US,
    FxRequest,
    YFinanceFxDailyFetcher,
)


DATE_FMT = "%Y-%m-%d"
DATE_FMT_HELP = DATE_FMT.replace("%", "%%")


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, DATE_FMT).replace(tzinfo=timezone.utc)


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Fetch daily FX rates and store them in ColumnarSqliteStore.")
    parser.add_argument("--base", required=False, help="Base currency (e.g., EUR)")
    parser.add_argument("--quote", required=False, help="Quote currency (e.g., USD)")
    parser.add_argument(
        "--provider-code",
        default=None,
        help="Provider symbol (default: BASEQUOTE=X for yfinance)",
    )
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
        "--read-back",
        action="store_true",
        help="Read back inserted rows and print them.",
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Print fetcher capabilities and exit.",
    )
    add_common_cli_args(
        parser,
        cache_help_subdir="fx_fetcher",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    ensure_profit_conf_loaded()
    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = ProfitConfig.from_args(args)
    apply_runtime_env(cfg)

    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    base_cache_dir = cfg.cache_root
    base_cache_dir.mkdir(parents=True, exist_ok=True)
    yf_cache_dir = base_cache_dir / "yfinance"
    yf_cache_dir.mkdir(parents=True, exist_ok=True)

    store_path = cfg.store_path
    store = ColumnarSqliteStore(store_path)
    cfg = ColumnarFxConfig()
    dataset = cfg.dataset_name(source="yfinance", version="v1")
    cache = FileCache(base_dir=base_cache_dir / "fx_fetcher")
    fetcher = YFinanceFxDailyFetcher(
        cfg=cfg,
        cache=cache,
        store=store,
        catalog_path=store_path,
        cache_root=base_cache_dir,
        allow_network=True,
        include_etf=True,
    )

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

    # Validate required args for fetch mode
    for name in ("base", "quote", "start", "end"):
        if getattr(args, name) is None:
            parser.error(f"--{name} is required unless --describe is used")

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if start > end:
        parser.error("--start must be <= --end")

    provider_code = args.provider_code or f"{args.base}{args.quote}=X"

    req = FxRequest(
        base_ccy=args.base.upper(),
        quote_ccy=args.quote.upper(),
        provider="yfinance",
        provider_code=provider_code,
    )

    pair = f"{req.base_ccy}/{req.quote_ccy}"

    print(f"Ensuring coverage for FX {req.base_ccy}/{req.quote_ccy} {start.date()} → {end.date()} via yfinance...")
    fetcher.timeseries_fetch_many([req], start, end)

    if args.read_back:
        series_id = store.get_series_id(
            instrument_id=pair,
            dataset=dataset,
            field="rate",
            step_us=DAY_US,
        )
        if series_id is None:
            print("No series found for read-back.")
            return
        pts = store.read_points(series_id, start=start, end=end, include_sentinel=False)
        print(f"Read back {len(pts)} points:")
        for ts, rate in pts:
            print(f"  {ts.date().isoformat()} rate={rate}")


if __name__ == "__main__":
    main()
