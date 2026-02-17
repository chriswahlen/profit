#!/usr/bin/env python3
"""Fetch EDGAR submissions from the SEC and store in `edgar.sqlite`.

This is a fetcher (networked) tool, not a bulk seeder. It downloads the
submissions JSON for each requested CIK (and any paged filings JSON referenced
by that payload) and upserts into the fixed EDGAR schema.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from config import Config
from data_sources.edgar.common import SEC_UA_ENV, normalize_cik
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.edgar.http import FetchFn
from data_sources.edgar.sec_edgar import EdgarSubmissionsFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchEdgarResults:
    updated: int
    failed: int


def fetch_submissions(
    *,
    config: Config,
    ciks: Iterable[str],
    user_agent: str,
    fetch_fn: FetchFn | None = None,
    pause_s: float = 0.0,
) -> FetchEdgarResults:
    store = EdgarDataStore(config)
    fetcher = EdgarSubmissionsFetcher(user_agent=user_agent, fetch_fn=fetch_fn)

    updated = failed = 0
    batch: list[tuple[str, str | None, datetime, str]] = []
    flush_n = 25

    # Materialize once so we can log count reliably without double-iterating.
    cik_list = [normalize_cik(c) for c in ciks]

    started_at = datetime.now(timezone.utc)
    logger.info("Starting EDGAR submissions fetch ciks=%d", len(cik_list))
    logger.info("Fetching EDGAR submissions for %d CIK(s)", len(cik_list))

    try:
        for idx, cik in enumerate(cik_list, start=1):
            try:
                res = fetcher.fetch(cik)
                fetched_at = datetime.now(timezone.utc)
                payload_str = json.dumps(res.raw, ensure_ascii=True)
                batch.append((cik, res.entity_name, fetched_at, payload_str))
            except Exception:
                failed += 1
                logger.exception("Failed to fetch submissions for cik=%s", cik)

            if pause_s:
                time.sleep(pause_s)

            if idx % 10 == 0:
                logger.info("Fetched %d/%d CIK(s) (failures=%d)", idx, len(cik_list), failed)

            if len(batch) >= flush_n:
                updated += store.upsert_submissions_rows(batch)
                batch.clear()

        if batch:
            updated += store.upsert_submissions_rows(batch)
    finally:
        store.close()

    elapsed_s = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info("Finished EDGAR submissions fetch updated=%d failed=%d elapsed_s=%.1f", updated, failed, elapsed_s)
    return FetchEdgarResults(updated=updated, failed=failed)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fetch_edgar", description="Fetch EDGAR submissions for CIKs")
    parser.add_argument("ciks", nargs="+", help="One or more CIKs to fetch/update")
    parser.add_argument(
        "--pause-s",
        type=float,
        default=0.0,
        help="Optional pause between requests (seconds). Default: 0.0",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    cfg = Config()
    ua = cfg.get_key(SEC_UA_ENV)
    if not ua:
        logger.error("%s must be set (env or config) with contact email per SEC policy", SEC_UA_ENV)
        return 2

    res = fetch_submissions(config=cfg, ciks=args.ciks, user_agent=ua, pause_s=float(args.pause_s))
    return 0 if res.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
