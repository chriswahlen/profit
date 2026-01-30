from __future__ import annotations

import logging
from argparse import ArgumentParser
from datetime import datetime, timezone

from profit.cache import FileCache, SqliteStore
from profit.config import ProfitConfig, apply_runtime_env, get_setting, add_common_cli_args
from profit.sources.fundamentals import ensure_sec_fundamentals_schemas, read_asof
from profit.sources.fundamentals.sec.fetcher import SecEdgarConfig, SecEdgarFundamentalsFetcher
from profit.sources.fundamentals.models import FundamentalsRequest


def _build_parser() -> ArgumentParser:
    p = ArgumentParser(description="Fetch SEC fundamentals (per-accession skeleton).")
    add_common_cli_args(p, cache_help_subdir="fundamentals", default_store_filename="columnar.sqlite3")
    p.add_argument("--cik", required=True, help="CIK (10 digits, zero-padded).")
    p.add_argument(
        "--start",
        required=True,
        help="Start filed/accepted date (YYYY-MM-DD).",
    )
    p.add_argument(
        "--end",
        required=True,
        help="End filed/accepted date (YYYY-MM-DD, inclusive).",
    )
    p.add_argument(
        "--forms",
        default="10-K,10-K/A,10-Q,10-Q/A,20-F,20-F/A,40-F,40-F/A,8-K,8-K/A",
        help="Comma-separated form types to include.",
    )
    p.add_argument(
        "--user-agent",
        required=False,
        default=None,
        help="User-Agent for SEC requests (default from SEC_USER_AGENT env/.profit.conf).",
    )
    p.add_argument("--email", default=None, help="Optional contact email for SEC requests.")
    p.add_argument(
        "--asof",
        default=None,
        help="Optional as-of timestamp (YYYY-MM-DD) to demo read_asof after fetch.",
    )
    # --log-level added by add_common_cli_args
    return p


def _parse_date(val: str) -> datetime:
    return datetime.fromisoformat(val).replace(tzinfo=timezone.utc)


def main(argv=None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    cfg = ProfitConfig.from_args(args)
    apply_runtime_env(cfg)

    ensure_sec_fundamentals_schemas(SqliteStore(cfg.store_path))

    user_agent = args.user_agent or get_setting("SEC_USER_AGENT", "PROFIT_SEC_USER_AGENT", default=None)
    email = args.email or get_setting("SEC_EMAIL", "PROFIT_SEC_EMAIL", default=None)
    if not user_agent:
        raise RuntimeError("User-Agent required. Set --user-agent or SEC_USER_AGENT in ~/.profit.conf or env.")

    edgar_cfg = SecEdgarConfig(user_agent=user_agent, email=email)
    fetcher = SecEdgarFundamentalsFetcher(
        cfg=cfg,
        edgar_cfg=edgar_cfg,
        cache=FileCache(),
        allow_network=True,
    )

    req = FundamentalsRequest(
        instrument_id=f"equity:US:CIK:{args.cik}",
        provider="sec",
        provider_code=args.cik,
        start=_parse_date(args.start),
        end=_parse_date(args.end),
        forms=tuple(f.strip() for f in args.forms.split(",") if f.strip()),
    )

    logging.info("fetch start cik=%s start=%s end=%s", args.cik, args.start, args.end)
    fetcher.timeseries_fetch_many([req], req.start, req.end)
    logging.info("fetch done")

    if args.asof:
        asof_dt = _parse_date(args.asof)
        rows = read_asof(SqliteStore(cfg.store_path), instrument_id=req.instrument_id, asof=asof_dt)
        logging.info("read_asof rows=%s", len(rows))
        for row in rows[:5]:
            logging.info("fact: %s", row)


if __name__ == "__main__":
    main()
