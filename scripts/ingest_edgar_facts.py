#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable

from profit.catalog import EntityStore
from profit.catalog.identifier_utils import resolve_cik_from_identifier
from profit.config import ProfitConfig
from profit.edgar import EdgarDatabase
from profit.edgar.xml_parser import parse_xbrl
from profit.sources.edgar.common import normalize_accession, normalize_cik


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the EdgarDatabase.ingest_xbrl_facts helper for a CIK.")
    parser.add_argument(
        "identifier",
        help="CIK, ticker, or catalog identifier for the company you want to ingest.",
    )
    parser.add_argument(
        "--accession",
        help="Optional accession filter (can be partial).",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        help="Override PROFIT_DATA_ROOT / ~/.profit.conf value.",
    )
    parser.add_argument(
        "--profit-db",
        type=Path,
        help="Override the catalog database path (default DATA_ROOT/profit.sqlite).",
    )
    parser.add_argument(
        "--edgar-db",
        type=Path,
        help="Override the EDGAR asset database path (default DATA_ROOT/edgar.sqlite3).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Remove existing facts/markers for each accession before ingesting.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate parsing without writing to the XBRL tables.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default INFO).",
    )
    return parser.parse_args()


def _resolve_data_root(args: argparse.Namespace) -> Path:
    return args.data_root or ProfitConfig.resolve_data_root()


def _resolve_profit_db(data_root: Path, override: Path | None) -> Path:
    candidates: list[Path] = []
    if override:
        candidates.append(override)
    candidates.append(data_root / "profit.sqlite")
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    raise SystemExit(f"profit catalog not found; checked {', '.join(str(p) for p in candidates)}")


def _resolve_cik(identifier: str, profit_db: Path) -> str:
    store = EntityStore(profit_db, readonly=True)
    try:
        cik = resolve_cik_from_identifier(store, identifier)
    finally:
        store.close()
    if not cik:
        raise SystemExit(f"Unable to resolve CIK for {identifier!r} using {profit_db}")
    return cik


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
    )

    data_root = _resolve_data_root(args)
    profit_db = _resolve_profit_db(data_root, args.profit_db)
    edgar_db_path = args.edgar_db or data_root / "edgar.sqlite3"
    edgar_db = EdgarDatabase(edgar_db_path)
    edgar_db.ensure_fact_marker_table()

    cik = _resolve_cik(args.identifier, profit_db)
    logging.info("ingest_edgar_facts start cik=%s edgar_db=%s", cik, edgar_db_path)

    cursor = edgar_db.conn.execute(
        "SELECT accession FROM edgar_accession WHERE cik = ? ORDER BY accession",
        (normalize_cik(cik),),
    )
    accessions = [row["accession"] for row in cursor.fetchall()]
    if args.accession:
        filter_norm = normalize_accession(args.accession)
        accessions = [acc for acc in accessions if filter_norm in normalize_accession(acc)]

    for accession in accessions:
        if args.force:
            edgar_db.clear_xbrl_fact_marker(cik, accession)
            edgar_db.reset_xbrl_accession(accession)

        files = edgar_db.get_accession_files(accession)
        xml_files = [name for name in files if name.lower().endswith(".xml")]
        if not xml_files:
            logging.warning("no XML files for accession=%s", accession)
            continue

        for filename in xml_files:
            payload = edgar_db.get_file(accession, filename)
            if payload is None:
                logging.warning("missing payload for accession=%s file=%s", accession, filename)
                continue
            if args.dry_run:
                parsed = parse_xbrl(payload)
                logging.info(
                    "dry-run would ingest %s facts for accession=%s file=%s",
                    len(parsed.facts),
                    accession,
                    filename,
                )
                continue
            try:
                count = edgar_db.ingest_xbrl_facts(cik, accession, payload)
            except ValueError as exc:
                logging.error("failed to ingest accession=%s file=%s err=%s", accession, filename, exc)
                continue
            logging.info("ingested %s facts for accession=%s file=%s", count, accession, filename)
            break


if __name__ == "__main__":
    main()
