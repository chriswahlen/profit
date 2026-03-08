from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from config import Config
from data_sources.data_source import DataSource
from data_sources.data_store import DataSourceUpdateResults
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.edgar.submissions_zip import SubmissionsZipEntry, read_submissions_from_zip
from data_sources.entity import EntityStore

logger = logging.getLogger(__name__)

SEC_PROVIDER = "provider:edgar"


class EdgarDataSource(DataSource):
    def __init__(
        self,
        config: Config,
        *,
        entity_store: EntityStore,
        store: EdgarDataStore,
        submissions_zip_path: Path | None = None,
    ):
        super().__init__(name="edgar", summary="SEC EDGAR submissions + XBRL facts", config=config, entity_store=entity_store)
        self.store = store
        self.submissions_zip_path = submissions_zip_path or Path("incoming/datasets/edgar/submissions.zip")
        self._log_accessions = False

    def describe_detailed(self, *, indent: str = "  ") -> str:
        lines = [f"{indent}Storage: sqlite at {self.store.db_path}"]
        lines.append(self.store.describe_detailed(indent=indent))
        return "\n".join(lines)

    def ensure_up_to_date(self, entity_ids: Iterable[str]) -> DataSourceUpdateResults:
        ciks = self._resolve_ciks(entity_ids)
        if not ciks:
            return DataSourceUpdateResults(detail="No SEC CIK mappings found for requested entities")

        submissions_zip = self.submissions_zip_path
        if not submissions_zip.exists():
            return DataSourceUpdateResults(detail=f"Missing submissions bundle: {submissions_zip}")

        logger.info("Opening EDGAR submissions bundle %s", submissions_zip)
        updated = failed = 0
        batch: list[tuple[str, str | None, datetime, str]] = []
        flush_n = 200

        for idx, cik in enumerate(sorted(ciks), start=1):
            try:
                entries = read_submissions_from_zip(submissions_zip, cik)
                if not entries:
                    continue
                if self._log_accessions:
                    for acc in _accessions_from_entries(entries):
                        logger.info("Queued accession %s for CIK %s", acc, cik)
                main = entries[0]
                payload = dict(main.payload)
                payload["__profit2_paged_payloads"] = [e.payload for e in entries[1:]]
                fetched_at = max((e.fetched_at for e in entries), default=datetime.now(timezone.utc))
                entity_name = payload.get("name") if isinstance(payload.get("name"), str) else None
                batch.append((cik, entity_name, fetched_at, json.dumps(payload, ensure_ascii=True)))
            except Exception:
                failed += 1
                logger.exception("Failed to read submissions from zip for cik=%s", cik)

            if idx % 250 == 0:
                logger.info("Prepared %d CIK(s) for upsert (failures=%d)", idx, failed)

            if len(batch) >= flush_n:
                updated += self.store.upsert_submissions_rows(batch)
                batch.clear()

        if batch:
            updated += self.store.upsert_submissions_rows(batch)

        logger.info("EDGAR submissions ingest finished updated=%d failed=%d", updated, failed)
        return DataSourceUpdateResults(updated=updated, failed=failed)

    def set_log_accessions(self, value: bool) -> None:
        self._log_accessions = value

    def _resolve_ciks(self, entity_ids: Iterable[str]) -> set[str]:
        ciks: set[str] = set()
        for eid in entity_ids:
            for provider, provider_id in self.entity_store.provider_ids_for_entity(eid, provider=SEC_PROVIDER):
                if provider == SEC_PROVIDER and provider_id:
                    ciks.add(provider_id)
        return ciks


def _accessions_from_entries(entries: list[SubmissionsZipEntry]) -> list[str]:
    accessions: list[str] = []
    for entry in entries:
        payload = entry.payload
        recent = _recent_filings(payload)
        accessions.extend(_safe_list(recent.get("accessionNumber")))
    return accessions


def _recent_filings(payload: Mapping[str, object]) -> Mapping[str, object]:
    filings = payload.get("filings") or {}
    if not isinstance(filings, Mapping):
        return {}
    recent = filings.get("recent") or {}
    if not isinstance(recent, Mapping):
        return {}
    return recent


def _safe_list(val: object) -> list[str]:
    if isinstance(val, list):
        return [str(item) for item in val if item]
    return []
