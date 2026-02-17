from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping, Sequence

from data_sources.edgar.common import SEC_UA_ENV, normalize_cik, normalize_accession
from data_sources.edgar.http import FetchFn, fetch_with_retry

logger = logging.getLogger(__name__)

SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik}.json"
FILINGS_FIELDS = ("accessionNumber", "form", "filingDate", "primaryDocument", "reportDate")


@dataclass(frozen=True)
class EdgarFiling:
    accession_number: str
    filing_date: date
    form: str
    primary_document: str | None
    report_date: date | None


@dataclass(frozen=True)
class EdgarSubmissions:
    cik: str
    entity_name: str | None
    recent_filings: Sequence[EdgarFiling]
    raw: Mapping[str, object]


class EdgarSubmissionsFetcher:
    """Fetch and parse SEC submissions JSON for a single CIK (plus paged filings)."""

    def __init__(
        self,
        *,
        user_agent: str,
        fetch_fn: FetchFn | None = None,
    ) -> None:
        if not user_agent:
            raise RuntimeError(f"{SEC_UA_ENV} must be set for SEC requests")
        self.user_agent = user_agent
        self.fetch_fn = fetch_fn

    def fetch(self, cik: str | int) -> EdgarSubmissions:
        cik_norm = normalize_cik(cik)
        url = SUBMISSIONS_URL_TMPL.format(cik=cik_norm)
        headers = {"User-Agent": self.user_agent, "Accept": "application/json"}

        payload = fetch_with_retry(url, headers=headers, fetch_fn=self.fetch_fn)
        data = json.loads(payload)

        pages = _fetch_paged_filings(base_url=url, base_payload=data, headers=headers, fetch_fn=self.fetch_fn)
        # Preserve full raw payload (base + any paged filings) for storage.
        # This mirrors the bulk-zip ingestion behavior.
        if isinstance(data, dict) and pages:
            data["__profit2_paged_payloads"] = pages
        filings = _parse_filings_with_pages([data] + pages)
        logger.info("edgar submissions fetched cik=%s count=%s", cik_norm, len(filings))
        return EdgarSubmissions(
            cik=cik_norm,
            entity_name=data.get("name") if isinstance(data, Mapping) else None,
            recent_filings=filings,
            raw=data,
        )


def _fetch_paged_filings(
    *,
    base_url: str,
    base_payload: Mapping[str, object],
    headers: Mapping[str, str],
    fetch_fn: FetchFn | None,
) -> list[Mapping[str, object]]:
    filings = base_payload.get("filings") or {}
    files = filings.get("files") or []
    if not isinstance(files, list) or not files:
        return []
    prefix = base_url.rsplit("/", 1)[0] + "/"
    pages: list[Mapping[str, object]] = []
    for entry in files:
        if not isinstance(entry, Mapping):
            continue
        name = entry.get("name")
        if not name:
            continue
        page_url = prefix + str(name)
        page_payload = fetch_with_retry(page_url, headers=headers, fetch_fn=fetch_fn)
        pages.append(json.loads(page_payload))
    return pages


def _parse_filings_with_pages(payloads: list[Mapping[str, object]]) -> list[EdgarFiling]:
    parsed: list[EdgarFiling] = []
    seen: set[str] = set()
    for data in payloads:
        filings = data.get("filings") or {}
        if not isinstance(filings, Mapping):
            continue
        recent = filings.get("recent") or {}
        if not isinstance(recent, Mapping):
            continue

        accessions = _safe_list(recent.get("accessionNumber"))
        forms = _safe_list(recent.get("form"))
        filing_dates = _safe_list(recent.get("filingDate"))
        primary_docs = _safe_list(recent.get("primaryDocument"))
        report_dates = _safe_list(recent.get("reportDate"))

        count = min(len(accessions), len(forms), len(filing_dates))
        for idx in range(count):
            acc = accessions[idx]
            norm_acc = normalize_accession(acc)
            if norm_acc in seen:
                continue
            seen.add(norm_acc)
            parsed.append(
                EdgarFiling(
                    accession_number=acc,
                    form=forms[idx],
                    filing_date=_parse_yyyymmdd(filing_dates[idx]),
                    primary_document=primary_docs[idx] if idx < len(primary_docs) else None,
                    report_date=_parse_optional_date(report_dates, idx),
                )
            )
    return parsed


def _parse_yyyymmdd(val: str) -> date:
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"invalid date string: {val}") from exc


def _parse_optional_date(values: Sequence[str], idx: int) -> date | None:
    if idx >= len(values):
        return None
    raw = values[idx]
    if not raw:
        return None
    return _parse_yyyymmdd(raw)


def _safe_list(val: object) -> list[str]:
    if isinstance(val, list):
        return [str(v) for v in val]
    return []
