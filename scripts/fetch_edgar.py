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
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Protocol

from config import Config
from data_sources.edgar.accession_reader import AccessionIndex, EdgarAccessionReader
from data_sources.edgar.common import SEC_UA_ENV, normalize_accession, normalize_cik, should_skip_accession_file
from data_sources.edgar.edgar_data_source import EdgarDataSource, SEC_PROVIDER
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.edgar.http import FetchFn
from data_sources.edgar.sec_edgar import EdgarSubmissions, EdgarSubmissionsFetcher
from data_sources.edgar.zip_utils import expand_zip_archive
from data_sources.entity import EntityStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchEdgarResults:
    updated: int
    failed: int
    submissions: tuple[EdgarSubmissions, ...]


def fetch_submissions(
    *,
    config: Config,
    ciks: Iterable[str],
    user_agent: str,
    fetch_fn: FetchFn | None = None,
    pause_s: float = 0.0,
    keep_store_open: bool = False,
) -> tuple[FetchEdgarResults, EdgarDataStore | None]:
    store = EdgarDataStore(config)
    fetcher = EdgarSubmissionsFetcher(user_agent=user_agent, fetch_fn=fetch_fn)

    updated = failed = 0
    batch: list[tuple[str, str | None, datetime, str]] = []
    submissions_found: list[EdgarSubmissions] = []
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
                submissions_found.append(res)
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
        if not keep_store_open:
            store.close()

    elapsed_s = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info("Finished EDGAR submissions fetch updated=%d failed=%d elapsed_s=%.1f", updated, failed, elapsed_s)
    return FetchEdgarResults(updated=updated, failed=failed, submissions=tuple(submissions_found)), store if keep_store_open else None


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fetch_edgar", description="Fetch EDGAR submissions for CIKs")
    parser.add_argument("ciks", nargs="+", help="One or more CIKs to fetch/update")
    parser.add_argument(
        "--pause-s",
        type=float,
        default=0.0,
        help="Optional pause between requests (seconds). Default: 0.0",
    )
    parser.add_argument(
        "--process-accessions",
        action="store_true",
        help="After fetching submissions, run ensure_up_to_date for those entities.",
    )
    parser.add_argument(
        "--process-filings",
        action="store_true",
        help="Download each filing's XML/TXT (and zipped XBRL packages) and ingest XBRL facts.",
    )
    parser.add_argument(
        "--submissions-zip",
        type=Path,
        default=Path("incoming/datasets/edgar/submissions.zip"),
        help="Path to submissions.zip consumed by the ingestion path (default: incoming/datasets/edgar/submissions.zip).",
    )
    parser.add_argument(
        "--verbose-accessions",
        action="store_true",
        help="When combined with --process-accessions, log each accession number as it is queued.",
    )
    return parser.parse_args(argv)


class AccessionReader(Protocol):
    def fetch_index(self, cik: str, accession: str) -> AccessionIndex:
        ...

    def fetch_file(self, cik: str, accession: str, filename: str) -> bytes:
        ...


_XML_START_PATTERN = re.compile(rb"<\?xml|<xbrli|<xbrl", re.IGNORECASE)


@dataclass(frozen=True)
class FilingIngestResults:
    accessions: int
    files: int
    facts: int
    failed: int


def _is_xbrl_filename(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(".xml") or lower.endswith(".txt")


def _trim_to_xml(payload: bytes) -> bytes:
    match = _XML_START_PATTERN.search(payload)
    if match:
        return payload[match.start() :]
    return payload


def _prepare_payload_for_ingest(payload: bytes) -> bytes:
    return _trim_to_xml(payload)


def _handle_value_error(
    *,
    exc: ValueError,
    name: str,
    accession: str,
    zipped: bool,
) -> bool:
    """Return True if the error was expected and already logged."""
    text = str(exc)
    label = "zipped file" if zipped else "file"
    if "no valid contexts" in text:
        logger.info("No contexts found in %s %s for accession=%s; skipping", label, name, accession)
        return True
    if "invalid XML" in text:
        logger.info("Invalid XML in %s %s for accession=%s; skipping", label, name, accession)
        return True
    return False


def ingest_recent_filings(
    *,
    submissions: Iterable[EdgarSubmissions],
    store: EdgarDataStore,
    user_agent: str,
    config: Config | None = None,
    pause_s: float = 0.0,
    accession_reader: AccessionReader | None = None,
    log_each_accession: bool = False,
) -> FilingIngestResults:
    reader = accession_reader or EdgarAccessionReader(user_agent=user_agent, config=config)
    accessions_processed = files_downloaded = facts_inserted = failures = 0

    for submission in submissions:
        cik_norm = normalize_cik(submission.cik)
        for filing in submission.recent_filings:
            try:
                accession = normalize_accession(filing.accession_number)
            except Exception:
                failures += 1
                logger.exception("Invalid accession %r for CIK %s", filing.accession_number, cik_norm)
                continue

            if store.has_processed_xbrl_facts(cik_norm, accession):
                logger.debug("Skipping already processed accession %s for CIK %s", accession, cik_norm)
                continue

            if log_each_accession:
                logger.info(
                    "Processing accession %s for CIK %s form=%s date=%s",
                    accession,
                    cik_norm,
                    filing.form,
                    filing.filing_date,
                )

            try:
                index = reader.fetch_index(cik_norm, accession)
            except Exception:
                failures += 1
                logger.exception("Failed to fetch index for accession=%s CIK=%s", accession, cik_norm)
                continue

            if pause_s:
                time.sleep(pause_s)

            file_names: list[str] = []
            for entry in index.files:
                if not isinstance(entry, Mapping):
                    continue
                name = entry.get("name")
                if isinstance(name, str):
                    file_names.append(name)

            store.record_accession_index(
                cik_norm,
                accession,
                index.base_url,
                file_names,
                fetched_at=datetime.now(timezone.utc),
            )

            processed_any = False
            for entry in index.files:
                if not isinstance(entry, Mapping):
                    continue
                name = entry.get("name")
                if not isinstance(name, str):
                    continue
                if should_skip_accession_file(accession, name):
                    continue

                try:
                    payload = reader.fetch_file(cik_norm, accession, name)
                except Exception:
                    failures += 1
                    logger.exception("Failed to fetch file %s for accession=%s", name, accession)
                    continue

                files_downloaded += 1
                source_url = f"{index.base_url}{name}" if index.base_url else None
                store.store_file(accession, name, payload, source_url=source_url)

                name_lower = name.lower()
                if name_lower.endswith(".zip"):
                    extracted = expand_zip_archive(accession, payload)
                    for inner_name, inner_payload in sorted(extracted.items()):
                        if not _is_xbrl_filename(inner_name):
                            continue
                        try:
                            sanitized_inner = _prepare_payload_for_ingest(inner_payload)
                            facts_inserted += store.ingest_xbrl_facts(cik_norm, accession, sanitized_inner)
                        except ValueError as exc:
                            failures += 1
                            if _handle_value_error(exc=exc, name=inner_name, accession=accession, zipped=True):
                                continue
                            logger.exception(
                                "Failed to ingest zipped file %s for accession=%s", inner_name, accession
                            )
                            continue
                        except Exception:
                            failures += 1
                            logger.exception(
                                "Failed to ingest zipped file %s for accession=%s", inner_name, accession
                            )
                            continue
                        processed_any = True
                elif _is_xbrl_filename(name):
                    try:
                        sanitized_payload = _prepare_payload_for_ingest(payload)
                        facts_inserted += store.ingest_xbrl_facts(cik_norm, accession, sanitized_payload)
                    except ValueError as exc:
                        failures += 1
                        if _handle_value_error(exc=exc, name=name, accession=accession, zipped=False):
                            continue
                        logger.exception("Failed to ingest file %s for accession=%s", name, accession)
                        continue
                    except Exception:
                        failures += 1
                        logger.exception("Failed to ingest file %s for accession=%s", name, accession)
                        continue
                    processed_any = True

                if pause_s:
                    time.sleep(pause_s)

            if processed_any:
                accessions_processed += 1
            else:
                logger.debug("No XML/TXT files processed for accession=%s", accession)

            if pause_s:
                time.sleep(pause_s)

    return FilingIngestResults(
        accessions=accessions_processed,
        files=files_downloaded,
        facts=facts_inserted,
        failed=failures,
    )


def _entity_ids_for_ciks(store: EntityStore, ciks: list[str]) -> list[str]:
    normalized = [normalize_cik(c) for c in ciks]
    cursor = store.connection.cursor()
    entity_ids: list[str] = []
    for cik in normalized:
        cursor.execute(
            """
            SELECT entity_id
            FROM provider_entity_map
            WHERE provider = ?
              AND provider_entity_id = ?
              AND (active_to IS NULL OR active_to > datetime('now'))
            LIMIT 1;
            """,
            (SEC_PROVIDER, cik),
        )
        row = cursor.fetchone()
        if row:
            entity_ids.append(row[0])
        else:
            logger.warning("No canonical entity found for cik=%s", cik)
    return entity_ids


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    cfg = Config()
    ua = cfg.get_key(SEC_UA_ENV)
    if not ua:
        logger.error("%s must be set (env or config) with contact email per SEC policy", SEC_UA_ENV)
        return 2

    keep_store_open = args.process_accessions or args.process_filings
    res, store_for_ingest = fetch_submissions(
        config=cfg,
        ciks=args.ciks,
        user_agent=ua,
        pause_s=float(args.pause_s),
        keep_store_open=keep_store_open,
    )

    total_failures = res.failed
    entity_store: EntityStore | None = None
    try:
        if args.process_filings:
            if not store_for_ingest:
                logger.error("Cannot download filings without an EDGAR store; re-run with --process-accessions or --process-filings without closing the store")
                total_failures += 1
            elif not res.submissions:
                logger.info("No submissions fetched; skipping filing ingestion")
            else:
                filings_result = ingest_recent_filings(
                    submissions=res.submissions,
                    store=store_for_ingest,
                    user_agent=ua,
                    config=cfg,
                    pause_s=float(args.pause_s),
                    accession_reader=None,
                    log_each_accession=args.verbose_accessions,
                )
                total_failures += filings_result.failed
                logger.info(
                    "Processed %d accessions, downloaded %d files, ingested %d facts (failures=%d)",
                    filings_result.accessions,
                    filings_result.files,
                    filings_result.facts,
                    filings_result.failed,
                )

        if args.process_accessions:
            if not store_for_ingest:
                logger.error("Cannot run ensure_up_to_date without an EDGAR store")
            else:
                entity_store = EntityStore(cfg)
                edgar_source = EdgarDataSource(
                    cfg,
                    entity_store=entity_store,
                    store=store_for_ingest,
                    submissions_zip_path=args.submissions_zip,
                )
                if args.verbose_accessions:
                    edgar_source.set_log_accessions(True)
                entity_ids = _entity_ids_for_ciks(entity_store, args.ciks)
                if entity_ids:
                    ingested = edgar_source.ensure_up_to_date(entity_ids)
                    total_failures += ingested.failed
                    logger.info(
                        "Processed accessions for %d entities (updated=%d failed=%d)",
                        len(entity_ids),
                        ingested.updated,
                        ingested.failed,
                    )
                else:
                    logger.warning("No entity mappings found for provided CIKs; skipping ingestion")
    finally:
        if entity_store:
            entity_store.close()
        if store_for_ingest:
            store_for_ingest.close()
    return 0 if total_failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
