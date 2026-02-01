from __future__ import annotations

"""Extract numeric facts from stored EDGAR XBRL files into company_finance_fact."""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Tuple, List, Set

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


def _iter_accessions(
    db: EdgarDatabase, cik: str | None = None, accession: str | None = None, *, force: bool = False
) -> Iterator[Tuple[str, str]]:
    """
    Pull accessions for the (optional) cik/accession filters, then drop any
    already marked in edgar_fact_extract unless force=True.
    """
    cur = db.conn.cursor()

    # Base list of candidate accessions (keep raw strings as stored for file lookups).
    if accession:
        cur.execute(
            "SELECT accession, cik FROM edgar_accession WHERE accession = ?",
            (accession,),
        )
    elif cik:
        cur.execute(
            "SELECT accession, cik FROM edgar_accession WHERE cik = ? ORDER BY accession",
            (normalize_cik(cik),),
        )
    else:
        cur.execute("SELECT accession, cik FROM edgar_accession ORDER BY accession")
    candidates: List[Tuple[str, str]] = [(row[0], row[1]) for row in cur.fetchall()]

    if force:
        for acc, ck in candidates:
            yield acc, normalize_cik(ck)
        return

    # Gather processed markers (by cik, normalized accession)
    processed: Set[Tuple[str, str]] = set()
    if cik:
        cur.execute(
            "SELECT accession FROM edgar_fact_extract WHERE cik = ?",
            (normalize_cik(cik),),
        )
        processed = {(normalize_cik(cik), normalize_accession(row[0])) for row in cur.fetchall()}
    else:
        cur.execute("SELECT cik, accession FROM edgar_fact_extract")
        processed = {(normalize_cik(row[0]), normalize_accession(row[1])) for row in cur.fetchall()}

    for acc, ck in candidates:
        norm_key = (normalize_cik(ck), normalize_accession(acc))
        if norm_key in processed:
            continue
        yield acc, normalize_cik(ck)


def _resolve_entity(store: EntityStore, cik: str) -> str | None:
    return store.find_entity_by_identifier(scheme="sec:cik", value=normalize_cik(cik))


def _ensure_marker_table(db: EdgarDatabase) -> None:
    db.conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edgar_fact_extract (
            cik TEXT NOT NULL,
            accession TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            fact_count INTEGER,
            note TEXT,
            PRIMARY KEY (cik, accession)
        )
        """
    )
    db.conn.commit()


def _has_processed(db: EdgarDatabase, cik: str, accession: str) -> bool:
    cur = db.conn.execute(
        "SELECT 1 FROM edgar_fact_extract WHERE cik = ? AND accession = ?",
        (normalize_cik(cik), normalize_accession(accession)),
    )
    return cur.fetchone() is not None


def _mark_processed(db: EdgarDatabase, cik: str, accession: str, fact_count: int, note: str | None) -> None:
    db.conn.execute(
        """
        INSERT INTO edgar_fact_extract (cik, accession, processed_at, fact_count, note)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(cik, accession) DO UPDATE SET
            processed_at=excluded.processed_at,
            fact_count=excluded.fact_count,
            note=excluded.note
        """,
        (normalize_cik(cik), normalize_accession(accession), datetime.now(timezone.utc).isoformat(), fact_count, note),
    )
    db.conn.commit()


def _load_source_url(db: EdgarDatabase, accession: str) -> dict[str, str | None]:
    info = db.get_accession_files_info(accession)
    return {name: url for name, url in info}


def _forms_for_cik(db: EdgarDatabase, cik: str) -> dict[str, str]:
    """Return accession->form map for a cik."""
    cur = db.conn.execute("SELECT payload FROM edgar_submissions WHERE cik = ? LIMIT 1", (normalize_cik(cik),))
    row = cur.fetchone()
    if not row:
        return {}
    try:
        import json

        data = json.loads(row["payload"])
    except Exception:
        return {}
    filings = data.get("filings") or {}
    recent = filings.get("recent") or {}
    accessions = recent.get("accessionNumber") or []
    forms = recent.get("form") or []
    mapping: dict[str, str] = {}
    for idx, acc in enumerate(accessions):
        norm_acc = normalize_accession(acc)
        form = forms[idx] if idx < len(forms) else None
        mapping[norm_acc] = form or "UNKNOWN"
    return mapping


def _filed_at_map(db: EdgarDatabase, cik: str) -> dict[str, datetime | None]:
    """Return accession->filed_at (acceptanceDateTime) map for a cik."""
    cur = db.conn.execute("SELECT payload FROM edgar_submissions WHERE cik = ? LIMIT 1", (normalize_cik(cik),))
    row = cur.fetchone()
    if not row:
        return {}
    try:
        import json

        data = json.loads(row["payload"])
    except Exception:
        return {}
    filings = (data.get("filings") or {}).get("recent") or {}
    accessions = filings.get("accessionNumber") or []
    acceptance = filings.get("acceptanceDateTime") or []
    filed_map: dict[str, datetime | None] = {}
    for idx, acc in enumerate(accessions):
        norm_acc = normalize_accession(acc)
        ts_raw = acceptance[idx] if idx < len(acceptance) else None
        dt_val: datetime | None = None
        if ts_raw:
            try:
                ts_norm = ts_raw.replace(" ", "T")
                dt_val = datetime.fromisoformat(ts_norm).replace(tzinfo=timezone.utc)
            except Exception:
                dt_val = None
        filed_map[norm_acc] = dt_val
    return filed_map


def _process_accession(
    *,
    accession: str,
    cik: str,
    db: EdgarDatabase,
    store: EntityStore,
    asof: datetime,
    filed_at: datetime | None,
    amendment_flag: bool | None,
    dry_run: bool,
    force: bool,
    form_map: dict[str, str],
) -> int:
    provider_entity_id = normalize_cik(cik)
    entity_id = _resolve_entity(store, provider_entity_id)
    if not entity_id:
        logging.warning("missing entity for cik=%s; skip accession=%s", provider_entity_id, accession)
        return 0

    if not force and _has_processed(db, provider_entity_id, accession):
        logging.info("skip accession=%s (already processed); use --force to reprocess", accession)
        return 0

    report_id = form_map.get(normalize_accession(accession), "UNKNOWN")
    name_to_url = _load_source_url(db, accession)
    filenames = db.get_accession_files(accession)
    written = 0
    facts: list[FinanceFactRecord] = []
    xml_seen = 0
    for name in filenames:
        if should_skip_accession_file(accession, name):
            continue
        lower = name.lower()
        if not lower.endswith(".xml"):
            continue
        xml_seen += 1
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
                provider_entity_id=provider_entity_id,
                report_id=report_id,
                source_file=name,
                source_url=name_to_url.get(name),
                asof=asof,
                filed_at=filed_at,
                amendment_flag=amendment_flag,
            )
        )

    if not facts:
        if xml_seen == 0:
            logging.info("accession=%s has no XML files after filtering (files=%s)", accession, len(filenames))
        else:
            logging.info("accession=%s yielded no parseable numeric facts (files=%s xml_files=%s)", accession, len(filenames), xml_seen)
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

    try:
        written = store.upsert_finance_facts(facts)
    except Exception as exc:
        logging.error(
            "failed to write facts accession=%s cik=%s count=%s error=%s",
            accession,
            cik,
            len(facts),
            exc,
        )
        raise
    _mark_processed(db, provider_entity_id, accession, written, report_id)
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
    parser.add_argument("--force", action="store_true", help="Reprocess even if accession was already extracted")

    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(message)s",
        force=True,  # ensure output even if logging was configured elsewhere
    )

    cfg = _profit_cfg(args)
    edgar_db_path = args.edgar_db or cfg.data_root / "edgar.sqlite3"
    edgar_db = EdgarDatabase(edgar_db_path)
    _ensure_marker_table(edgar_db)
    store = EntityStore(cfg.store_path)

    logging.info(
        "extract_edgar_facts start cik=%s accession=%s dry_run=%s force=%s limit=%s edgar_db=%s store=%s",
        args.cik,
        args.accession,
        args.dry_run,
        args.force,
        args.limit,
        edgar_db_path,
        cfg.store_path,
    )

    asof = datetime.now(timezone.utc)
    count = 0
    processed_any = False
    form_cache: dict[str, dict[str, str]] = {}
    filed_cache: dict[str, dict[str, datetime | None]] = {}
    for accession, cik in _iter_accessions(edgar_db, cik=args.cik, accession=args.accession, force=args.force):
        count += 1
        if args.limit and count > args.limit:
            break
        if cik not in form_cache:
            form_cache[cik] = _forms_for_cik(edgar_db, cik)
        if cik not in filed_cache:
            filed_cache[cik] = _filed_at_map(edgar_db, cik)
        report_id = form_cache.get(cik, {}).get(normalize_accession(accession), "UNKNOWN")
        amendment_flag = None
        base_form = report_id
        if report_id and report_id.endswith("/A"):
            amendment_flag = True
            base_form = report_id[:-2]
        elif report_id:
            amendment_flag = False

        written = _process_accession(
            accession=accession,
            cik=cik,
            db=edgar_db,
            store=store,
            asof=asof,
            filed_at=filed_cache.get(cik, {}).get(normalize_accession(accession)),
            amendment_flag=amendment_flag,
            dry_run=args.dry_run,
            force=args.force,
            form_map=form_cache.get(cik, {}),
        )
        if args.dry_run or written > 0:
            logging.info("accession=%s facts_written=%s", accession, written)
        else:
            logging.info("accession=%s facts_written=%s", accession, written)
        processed_any = True

    if not processed_any:
        logging.info(
            "No accessions processed (filters=%s,%s, force=%s, limit=%s). "
            "Likely already extracted or missing entity/files; try --force or relax filters.",
            args.cik,
            args.accession,
            args.force,
            args.limit,
        )

    edgar_db.close()
    store.close()


if __name__ == "__main__":
    main()
