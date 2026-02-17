#!/usr/bin/env python3
"""Seed EDGAR submissions for a list of CIKs from a bulk submissions zip.

This populates `edgar.sqlite` (not `edgar.sqlite3`) using the fixed legacy
schema used by the EDGAR store. The input bundle is expected to contain files
named:
  - CIK##########.json
  - CIK##########-submissions-###.json (optional pages)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import Config
from data_sources.edgar.common import normalize_cik
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.edgar.submissions_zip import read_submissions_from_zip

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="seed_edgar", description="Seed EDGAR submissions from a bulk zip")
    parser.add_argument(
        "--submissions-zip",
        default="incoming/datasets/edgar/submissions.zip",
        help="Path to submissions.zip bundle (default: incoming/datasets/edgar/submissions.zip)",
    )
    parser.add_argument("ciks", nargs="+", help="One or more CIKs (digits only ok; will be normalized)")
    return parser.parse_args(argv)


def seed_submissions(*, config: Config, submissions_zip: Path, ciks: list[str]) -> tuple[int, int]:
    store = EdgarDataStore(config)
    updated = failed = 0
    batch: list[tuple[str, str | None, datetime, str]] = []
    flush_n = 200

    try:
        logger.info("Opening EDGAR submissions bundle %s", submissions_zip)
        for idx, raw_cik in enumerate(ciks, start=1):
            cik = normalize_cik(raw_cik)
            try:
                entries = read_submissions_from_zip(submissions_zip, cik)
                if not entries:
                    logger.info("No submissions entry found in bundle for cik=%s", cik)
                    continue
                main = entries[0]
                payload = dict(main.payload)
                payload["__profit2_paged_payloads"] = [e.payload for e in entries[1:]]
                fetched_at = max((e.fetched_at for e in entries), default=datetime.now(timezone.utc))
                entity_name = payload.get("name") if isinstance(payload.get("name"), str) else None
                batch.append((cik, entity_name, fetched_at, json.dumps(payload, ensure_ascii=True)))
            except Exception:
                failed += 1
                logger.exception("Failed to read submissions from zip for cik=%s", cik)

            if idx % 100 == 0:
                logger.info("Prepared %d CIK(s) for upsert (failures=%d)", idx, failed)

            if len(batch) >= flush_n:
                updated += store.upsert_submissions_rows(batch)
                batch.clear()

        if batch:
            updated += store.upsert_submissions_rows(batch)
    finally:
        store.close()

    logger.info("Seeded EDGAR submissions updated=%d failed=%d", updated, failed)
    return updated, failed


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    zip_path = Path(args.submissions_zip)
    if not zip_path.exists():
        logger.error("Submissions zip not found: %s", zip_path)
        return 2

    cfg = Config()
    _, failed = seed_submissions(config=cfg, submissions_zip=zip_path, ciks=list(args.ciks))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

