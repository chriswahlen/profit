from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from profit.cache import FileCache, SqliteStore
from profit.cache.file_cache import CacheMissError
from profit.catalog.lifecycle import CatalogLifecycleReader
from profit.catalog.refresher import CatalogChecker
from profit.catalog.store import CatalogStore
from profit.config import ProfitConfig
from profit.sources.base_fetcher import BaseFetcher
from profit.sources.errors import ThrottledError
from profit.sources.fundamentals.models import FundamentalsRequest, FactRow, FilingRow
from profit.sources.fundamentals.schemas import ensure_sec_fundamentals_schemas
from profit.sources.fundamentals.writer import write_facts, write_filings
from profit.sources.fundamentals.sec.parse import parse_xbrl_json

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SecEdgarConfig:
    user_agent: str
    email: str | None = None
    cache_ttl: timedelta = timedelta(hours=6)
    allow_network: bool = True
    submissions_base: str = "https://data.sec.gov/submissions/"


class SecEdgarFundamentalsFetcher(BaseFetcher[FundamentalsRequest, list[FactRow]]):
    """
    Per-accession EDGAR fundamentals fetcher (skeleton, JSON-first).
    """

    def __init__(
        self,
        *,
        cfg: ProfitConfig,
        edgar_cfg: SecEdgarConfig,
        cache: FileCache | None = None,
        lifecycle=None,
        catalog_checker=None,
        catalog_path: str | Path | None = None,
        allow_network: bool = True,
        clock: Callable[[], datetime] | None = None,
        **kwargs,
    ) -> None:
        self.edgar_cfg = edgar_cfg
        self._allow_network = allow_network
        self._clock = clock or (lambda: datetime.now(timezone.utc))

        if lifecycle is None or catalog_checker is None:
            cat_path = Path(catalog_path) if catalog_path is not None else Path(cfg.store_path)
            cat_store = CatalogStore(cat_path, readonly=False)
            lifecycle = CatalogLifecycleReader(cat_store)
            catalog_checker = CatalogChecker(
                store=cat_store,
                refresher=_NoopRefresher(),
                max_age=timedelta(days=365),
                allow_network=False,
            )

        super().__init__(
            cfg=cfg,
            cache=cache,
            lifecycle=lifecycle,
            catalog_checker=catalog_checker,
            max_window_days=None,
            max_batch_size=1,
            **kwargs,
        )

        ensure_sec_fundamentals_schemas(SqliteStore(Path(cfg.store_path)))

    # ------------------------------------------------------------------
    def _fetch_timeseries_chunk_many(
        self, requests: list[FundamentalsRequest], start: datetime, end: datetime
    ) -> dict[FundamentalsRequest, list[FactRow]]:
        out: dict[FundamentalsRequest, list[FactRow]] = {}
        for req in requests:
            facts = self._fetch_one(req)
            out[req] = facts
        return out

    # ------------------------------------------------------------------
    def _fetch_one(self, req: FundamentalsRequest) -> list[FactRow]:
        """
        Fetch filings for a single CIK window. Currently stubs fact parsing.
        """
        cik = req.provider_code
        store = SqliteStore(Path(self.cfg.store_path))
        ensure_sec_fundamentals_schemas(store)

        existing = _existing_accessions(store, cik)
        filings = list(_list_filings(req, edgar_cfg=self.edgar_cfg, cache=self.cache, allow_network=self._allow_network))
        new_filings = [f for f in filings if f.accession not in existing]

        if not new_filings:
            logger.info("sec fundamentals: nothing new cik=%s", cik)
            return []

        write_filings(store, (f.__dict__ for f in new_filings))

        facts: list[FactRow] = []
        for filing in new_filings:
            try:
                facts.extend(self._download_and_parse_filing(filing))
            except ThrottledError:
                raise
            except Exception as exc:
                logger.exception("sec fundamentals: parse failed accession=%s cik=%s", filing.accession, cik, exc_info=exc)
                continue

        if facts:
            write_facts(store, (f.__dict__ for f in facts))
        logger.info("sec fundamentals: wrote filings=%s facts=%s cik=%s", len(new_filings), len(facts), cik)
        return facts

    def _download_and_parse_filing(self, filing: FilingRow) -> list[FactRow]:
        """
        Download the per-accession XBRL JSON (if available) and parse to FactRows.

        URL pattern: https://data.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{accession_with_dashes}-xbrl.json
        """
        cache = self.cache if isinstance(self.cache, FileCache) else None
        edgar_cfg = self.edgar_cfg
        if cache is None:
            cache = FileCache(ttl=edgar_cfg.cache_ttl)

        key = f"sec_xbrljson_{filing.accession}"
        raw: bytes | None = None
        try:
            entry = cache.get(key, ttl=edgar_cfg.cache_ttl)
            raw = entry.value
        except CacheMissError:
            raw = None

        if raw is None:
            if not self._allow_network and not edgar_cfg.allow_network:
                raise RuntimeError("network disabled and xbrl json not cached")
            cik_no_lead = filing.provider_code.lstrip("0")
            acc_dash = "{}-{}-{}".format(filing.accession[:10], filing.accession[10:12], filing.accession[12:])
            url = f"https://data.sec.gov/Archives/edgar/data/{cik_no_lead}/{filing.accession}/{acc_dash}-xbrl.json"
            logger.info("sec fundamentals: fetch xbrl json accession=%s url=%s", filing.accession, url)
            try:
                raw = _http_get(url, edgar_cfg=edgar_cfg)
            except HTTPError as exc:
                if exc.code == 404:
                    logger.warning("sec fundamentals: xbrl json missing accession=%s url=%s", filing.accession, url)
                    return []
                raise
            try:
                cache.set(key, raw)
            except Exception:
                pass

        payload = json.loads(raw)
        facts = list(
            parse_xbrl_json(
                data=payload,
                instrument_id=filing.instrument_id,
                cik=filing.provider_code,
                accession=filing.accession,
                form=filing.form,
                filed_at=filing.filed_at,
                accepted_at=filing.accepted_at,
                known_at=filing.known_at,
                asof=filing.asof,
            )
        )
        return facts


# --- helpers -----------------------------------------------------------


def _http_get(url: str, *, edgar_cfg: SecEdgarConfig) -> bytes:
    headers = {"User-Agent": edgar_cfg.user_agent}
    if edgar_cfg.email:
        headers["From"] = edgar_cfg.email
    req = Request(url, headers=headers)
    try:
        with urlopen(req) as resp:
            return resp.read()
    except Exception as exc:
        status = getattr(exc, "code", None)
        if status == 429:
            raise ThrottledError("SEC HTTP 429", retry_after=None) from exc
        raise


class _NoopRefresher:
    def refresh(self, provider: str, *, allow_network: bool, use_cache_only: bool = False) -> None:
        return None


def _existing_accessions(store: SqliteStore, cik: str) -> set[str]:
    try:
        rows = store.read(
            "fundamentals_filing:sec:v1",
            columns=["accession"],
            where="provider = :p AND provider_code = :c",
            params={"p": "sec", "c": cik},
        )
    except Exception:
        return set()
    out: set[str] = set()
    for r in rows:
        if isinstance(r, dict):
            out.add(r.get("accession") or "")
        else:
            # fallback for tuple/row cases
            try:
                out.add(r[0])
            except Exception:
                continue
    return {x for x in out if x}


def _list_filings(req: FundamentalsRequest, *, edgar_cfg: SecEdgarConfig, cache: FileCache | None, allow_network: bool) -> Iterable[FilingRow]:
    """
    List filings from the SEC submissions JSON and filter by form + filed date window.
    """
    if cache is None:
        cache = FileCache(ttl=edgar_cfg.cache_ttl)

    key = f"sec_submissions_{req.provider_code}"
    data: bytes | None = None
    try:
        entry = cache.get(key, ttl=edgar_cfg.cache_ttl)
        data = entry.value
    except CacheMissError:
        data = None

    if data is None:
        if not allow_network and not edgar_cfg.allow_network:
            raise RuntimeError("network disabled and submissions not cached")
        url = f"{edgar_cfg.submissions_base}CIK{req.provider_code}.json"
        logger.info("sec fetch submissions cik=%s url=%s", req.provider_code, url)
        data = _http_get(url, edgar_cfg=edgar_cfg)
        try:
            cache.set(key, data)
        except Exception:
            pass

    payload = json.loads(data)
    recent = payload.get("filings", {}).get("recent", {})
    accns = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    filed = recent.get("filingDate", [])
    accepted = recent.get("acceptanceDateTime", [])
    report = recent.get("reportDate", [])

    now = datetime.now(timezone.utc)
    for acc, form, filed_str, accepted_str, report_str in zip(accns, forms, filed, accepted, report):
        form = form.strip()
        if form not in req.forms:
            continue
        try:
            filed_at = datetime.fromisoformat(filed_str).replace(tzinfo=timezone.utc)
        except Exception:
            continue
        if filed_at < req.start or filed_at > req.end:
            continue
        accepted_at = None
        try:
            if accepted_str:
                accepted_at = datetime.fromisoformat(accepted_str.replace(" ", "T")).astimezone(timezone.utc)
        except Exception:
            accepted_at = None
        known_at = accepted_at or filed_at
        report_end = None
        try:
            if report_str:
                report_end = datetime.fromisoformat(report_str).replace(tzinfo=timezone.utc)
        except Exception:
            report_end = None
        is_amendment = form.endswith("/A")
        yield FilingRow(
            provider="sec",
            provider_code=req.provider_code,
            instrument_id=req.instrument_id,
            accession=acc.replace("-", ""),
            form=form,
            filed_at=filed_at,
            accepted_at=accepted_at,
            known_at=known_at,
            report_period_end=report_end,
            is_amendment=is_amendment,
            asof=now,
            attrs={},
        )
