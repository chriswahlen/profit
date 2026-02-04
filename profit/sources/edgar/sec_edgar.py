from __future__ import annotations

"""EDGAR submissions fetcher (single-CIK, recent filings).

This module provides a thin wrapper around the SEC ``submissions`` endpoint
and will be expanded to handle incremental inserts. The fetcher is written to
the repository's fetcher standards (caching, retry via BatchFetcher, and
provider-aware logging).
"""

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Mapping, Sequence

from profit.cache import FileCache
from profit.config import ProfitConfig, get_setting
from profit.sources.batch_fetcher import BatchFetcher
from profit.sources.types import Fingerprintable
from profit.utils.url_fetcher import FetchFn, TemporaryFetchError, fetch_url
from .common import SEC_UA_ENV, normalize_cik, normalize_accession

logger = logging.getLogger(__name__)

MAX_TEMP_RETRIES = 20
BACKOFF_INITIAL = 0.5
BACKOFF_MAX = 30.0


SEC_PROVIDER_ID = "sec:edgar"
SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik}.json"
DEFAULT_TTL = timedelta(days=1)
FILINGS_FIELDS = ("accessionNumber", "form", "filingDate", "primaryDocument", "reportDate")


def _fetch_with_retry(
    url: str,
    *,
    cache: FileCache,
    ttl: timedelta,
    allow_network: bool,
    headers: Mapping[str, str],
    fetch_fn: FetchFn | None,
) -> bytes:
    attempts = 0
    while True:
        attempts += 1
        try:
            return fetch_url(
                url,
                cache=cache,
                ttl=ttl,
                allow_network=allow_network,
                headers=headers,
                fetch_fn=fetch_fn,
            )
        except TemporaryFetchError as exc:
            if attempts >= MAX_TEMP_RETRIES:
                logger.error("edgar fetch retries exhausted url=%s status=%s", url, exc.status)
                raise
            delay = min(BACKOFF_INITIAL * 2 ** (attempts - 1), BACKOFF_MAX)
            logger.warning(
                "temporary edgar fetch failure url=%s status=%s attempt=%d; sleeping %.1fs",
                url,
                exc.status,
                attempts,
                delay,
            )
            time.sleep(delay)


def _normalize_cik(raw: str | int) -> str:
    digits = re.sub(r"\D", "", str(raw))
    if not digits:
        raise ValueError("CIK must include at least one digit")
    if len(digits) > 10:
        digits = digits[-10:]
    return digits.zfill(10)


@dataclass(frozen=True)
class EdgarSubmissionsRequest(Fingerprintable):
    cik: str
    provider: str = SEC_PROVIDER_ID
    provider_code: str | None = None

    def __post_init__(self):
        norm = normalize_cik(self.cik)
        object.__setattr__(self, "cik", norm)
        if self.provider_code is None:
            object.__setattr__(self, "provider_code", norm)

    def fingerprint(self) -> str:
        return f"edgar:submissions:{self.cik}"


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


class EdgarSubmissionsFetcher(BatchFetcher[EdgarSubmissionsRequest, EdgarSubmissions]):
    """Fetch the SEC submissions payload for a single CIK.

    The SEC API requires a descriptive User-Agent; this fetcher reads it from
    ``PROFIT_SEC_USER_AGENT`` (same as the SEC seeder) or an explicit
    ``user_agent`` argument.
    """

    def __init__(
        self,
        *,
        cfg: ProfitConfig,
        cache: FileCache,
        lifecycle,
        catalog_checker,
        ttl: timedelta = DEFAULT_TTL,
        offline: bool = False,
        max_attempts: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 5.0,
        fetch_fn: FetchFn | None = None,
        user_agent: str | None = None,
    ) -> None:
        super().__init__(
            cfg=cfg,
            cache=cache,
            ttl=ttl,
            offline=offline,
            max_attempts=max_attempts,
            backoff_factor=backoff_factor,
            max_backoff=max_backoff,
            lifecycle=lifecycle,
            catalog_checker=catalog_checker,
        )
        ua = user_agent or get_setting(SEC_UA_ENV)
        if not ua:
            raise RuntimeError(f"{SEC_UA_ENV} must be set for SEC requests")
        self.user_agent = ua
        self.fetch_fn = fetch_fn

    def _download_bulk(self, request: EdgarSubmissionsRequest) -> EdgarSubmissions:  # type: ignore[override]
        print("DOWNLOAD BULK")
        url = SUBMISSIONS_URL_TMPL.format(cik=request.cik)
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }
        payload = _fetch_with_retry(
            url,
            cache=self.cache,
            ttl=self.ttl,
            allow_network=not self.offline,
            headers=headers,
            fetch_fn=self.fetch_fn,
        )
        data = json.loads(payload)

        page_payloads = _fetch_paged_filings(
            base_url=url,
            base_payload=data,
            cache=self.cache,
            ttl=self.ttl,
            allow_network=not self.offline,
            headers=headers,
            fetch_fn=self.fetch_fn,
        )

        filings = _parse_filings_with_pages([data] + page_payloads)
        logger.info(
            "edgar submissions fetched provider=%s cik=%s count=%s",
            SEC_PROVIDER_ID,
            request.cik,
            len(filings),
        )
        return EdgarSubmissions(
            cik=request.cik,
            entity_name=data.get("name"),
            recent_filings=filings,
            raw=data,
        )


def _fetch_paged_filings(base_url: str, base_payload: Mapping[str, object], *, cache, ttl, allow_network: bool, headers, fetch_fn) -> list[Mapping[str, object]]:
    """
    Fetch paged filings listed under filings.files (e.g., CIKxxxx-001.json).
    Returns a list of decoded JSON payloads; errors per page propagate.
    """
    filings = base_payload.get("filings") or {}
    files = filings.get("files") or []
    if not isinstance(files, list) or not files:
        logger.info("edgar submissions has no paged filings files for %s", base_url)
        return []
    # Derive directory prefix from base_url.
    prefix = base_url.rsplit("/", 1)[0] + "/"
    pages: list[Mapping[str, object]] = []
    fetched = 0
    for entry in files:
        if not isinstance(entry, Mapping):
            continue
        name = entry.get("name")
        if not name:
            continue
        logger.info("edgar submissions fetching page file name=%s base=%s", name, base_url)
        page_url = prefix + str(name)
        page_payload = _fetch_with_retry(
            page_url,
            cache=cache,
            ttl=ttl,
            allow_network=allow_network,
            headers=headers,
            fetch_fn=fetch_fn,
        )
        pages.append(json.loads(page_payload))
        fetched += 1
    logger.info("edgar submissions fetched %s paged filing file(s) for %s", fetched, base_url)
    return pages


def _parse_filings_with_pages(payloads: list[Mapping[str, object]]) -> list[EdgarFiling]:
    """
    Combine filings from the main submissions JSON plus any paged files listed
    under filings.files. Keep first occurrence of an accession (newest first).
    """
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
    except Exception as exc:  # pragma: no cover - defensive guard
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
