from __future__ import annotations

"""Extract numeric facts from stored EDGAR XBRL files into company_finance_fact."""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from profit.catalog import EntityStore
from profit.catalog.types import FinanceFactRecord
from profit.config import ProfitConfig
from profit.edgar import EdgarDatabase
from profit.edgar.xbrl_extract import extract_finance_facts
from profit.sources.edgar.common import normalize_accession, normalize_cik, should_skip_accession_file
from profit.sources.edgar.sec_edgar import SEC_PROVIDER_ID


def _profit_cfg(args) -> ProfitConfig:
    data_root = args.data_root or ProfitConfig.resolve_data_root()
    cache_root = args.cache_root or ProfitConfig.resolve_cache_root()
    store_path = args.store_path or data_root / "profit.sqlite"
    return ProfitConfig(
        data_root=data_root,
        cache_root=cache_root,
        store_path=store_path,
        log_level=args.log_level,
        refresh_catalog=False,
    )


def _iter_accessions(db: EdgarDatabase, cik: str | None = None, accession: str | None = None) -> Iterable[str]:
    cur = db.conn.cursor()
    if accession:
        cur.execute(
            "SELECT accession FROM edgar_accession WHERE accession = ?",
            (normalize_accession(accession),),
        )
    elif cik:
        cur.execute(
            "SELECT accession FROM edgar_accession WHERE cik = ? ORDER BY accession",
            (normalize_cik(cik),),
        )
    else:
        cur.execute("SELECT accession FROM edgar_accession ORDER BY accession")
    for row in cur.fetchall():
        yield row[0]


def _resolve_entity(store: EntityStore, cik: str) -> str | None:
    return store.find_entity_by_identifier(scheme="sec:cik", value=normalize_cik(cik))


def _load_source_url(db: EdgarDatabase, accession: str) -> dict[str, str | None]:
    info = db.get_accession_files_info(accession)
    return {name: url for name, url in info}


def _process_accession(
    *,
    accession: str,
    cik: str,
    db: EdgarDatabase,
    store: EntityStore,
    asof: datetime,
    dry_run: bool,
) -> int:
    provider_entity_id = normalize_cik(cik)
    entity_id = _resolve_entity(store, provider_entity_id)
    if not entity_id:
        logging.warning("missing entity for cik=%s; skip accession=%s", provider_entity_id, accession)
        return 0

    name_to_url = _load_source_url(db, accession)
    filenames = db.get_accession_files(accession)
    written = 0
    facts: list[FinanceFactRecord] = []
    for name in filenames:
        if should_skip_accession_file(accession, name):
            continue
        lower = name.lower()
        if not lower.endswith(".xml"):
            continue
        payload = db.get_file(accession, name)
        if payload is None:
            continue
        facts.extend(
            extract_finance_facts(
                xml_bytes=payload,
                cik=provider_entity_id,
                accession=normalize_accession(accession),
                entity_id=entity_id,
                provider_id=SEC_PROVIDER_ID,
                source_file=name,
                source_url=name_to_url.get(name),
                asof=asof,
                provider_entity_id=provider_entity_id,
            )
        )

    if not facts:
        return 0

    if dry_run:
        logging.info("dry-run accession=%s facts=%s", accession, len(facts))
        for f in facts:
            logging.info(
                "FACT accession=%s file=%s key=%s period_end=%s units=%s value=%s attrs=%s",
                accession,
                f.report_id,
                f.report_key,
                f.period_end.isoformat(),
                f.units,
                f.value,
                f.attrs,
            )
        return len(facts)

    written = store.upsert_finance_facts(facts)
    logging.info("written facts=%s accession=%s", written, accession)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract EDGAR XBRL facts into company_finance_fact")
    parser.add_argument("--cik", help="Filter to a specific CIK")
    parser.add_argument("--accession", help="Filter to a specific accession")
    parser.add_argument("--data-root", type=Path, help="Override PROFIT_DATA_ROOT")
    parser.add_argument("--store-path", type=Path, help="Override profit.sqlite path")
    parser.add_argument("--cache-root", type=Path, help="Unused; present for symmetry")
    parser.add_argument("--edgar-db", type=Path, help="Path to edgar.sqlite3 (default DATA_ROOT/edgar.sqlite3)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--limit", type=int, help="Limit number of accessions processed")
    parser.add_argument("--dry-run", action="store_true", help="Parse only; do not write to company_finance_fact")

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    cfg = _profit_cfg(args)
    edgar_db_path = args.edgar_db or cfg.data_root / "edgar.sqlite3"
    edgar_db = EdgarDatabase(edgar_db_path)
    store = EntityStore(cfg.store_path)

    asof = datetime.now(timezone.utc)
    count = 0
    for accession in _iter_accessions(edgar_db, cik=args.cik, accession=args.accession):
        count += 1
        if args.limit and count > args.limit:
            break
        written = _process_accession(
            accession=accession,
            cik=args.cik or edgar_db.conn.execute("SELECT cik FROM edgar_accession WHERE accession = ?", (accession,)).fetchone()[0],
            db=edgar_db,
            store=store,
            asof=asof,
            dry_run=args.dry_run,
        )
        logging.info("accession=%s facts_written=%s", accession, written)

    edgar_db.close()
    store.close()


if __name__ == "__main__":
    main()
