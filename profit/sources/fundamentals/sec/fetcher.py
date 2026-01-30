from __future__ import annotations

import json
import hashlib
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.error import URLError
import zipfile
import io

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
from profit.sources.fundamentals.models import FactRow

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
                    return _fallback_zip_or_companyfacts(filing, edgar_cfg, cache, self._allow_network)
                raise
            except RuntimeError:
                logger.warning("sec fundamentals: network unavailable for xbrl json accession=%s; using fallback", filing.accession)
                return _fallback_zip_or_companyfacts(filing, edgar_cfg, cache, self._allow_network)
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
    except HTTPError as exc:
        # Let caller handle 404/429/etc.
        raise
    except URLError as exc:
        raise RuntimeError(f"Network unavailable for SEC request: {url}") from exc
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


def _fallback_companyfacts(filing: FilingRow, edgar_cfg: SecEdgarConfig, cache: FileCache | None, allow_network: bool) -> list[FactRow]:
    """
    Fallback: use companyfacts API and filter facts for this accession.
    """
    if cache is None:
        cache = FileCache(ttl=edgar_cfg.cache_ttl)
    key = f"sec_companyfacts_{filing.provider_code}"
    raw: bytes | None = None
    try:
        raw = cache.get(key, ttl=edgar_cfg.cache_ttl).value
    except Exception:
        raw = None
    if raw is None:
        if not allow_network and not edgar_cfg.allow_network:
            logger.warning("sec fundamentals: companyfacts unavailable (offline) accession=%s", filing.accession)
            return []
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{filing.provider_code}.json"
        logger.info("sec fundamentals: fetch companyfacts cik=%s url=%s", filing.provider_code, url)
        raw = _http_get(url, edgar_cfg=edgar_cfg)
        try:
            cache.set(key, raw)
        except Exception:
            pass

    payload = json.loads(raw)
    facts_out: list[FactRow] = []
    facts = payload.get("facts", {})
    acc_no_dashes = filing.accession

    for ns, tags in facts.items():
        for tag, body in tags.items():
            units = body.get("units", {})
            for unit_name, obs_list in units.items():
                for obs in obs_list:
                    if obs.get("accn", "").replace("-", "") != acc_no_dashes:
                        continue
                    end_str = obs.get("end")
                    if not end_str:
                        continue
                    try:
                        end_dt = datetime.fromisoformat(end_str).replace(tzinfo=timezone.utc)
                    except Exception:
                        continue
                    start_dt = None
                    start_str = obs.get("start")
                    if start_str:
                        try:
                            start_dt = datetime.fromisoformat(start_str).replace(tzinfo=timezone.utc)
                        except Exception:
                            start_dt = None
                    dims = obs.get("dimensions") or {}
                    dims_key = "|".join(f"{k}={v}" for k, v in sorted(dims.items()))
                    dims_json = (
                        json.dumps(
                            [{"axis": k, "member": v} for k, v in sorted(dims.items())],
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                        if dims
                        else "[]"
                    )
                    dims_hash = "" if not dims_key else hashlib.sha256(dims_key.encode("utf-8")).hexdigest()[:16]
                    val = obs.get("val")
                    value_kind = "number"
                    value_num = None
                    value_text = None
                    try:
                        value_num = float(val)
                    except Exception:
                        value_kind = "text"
                        value_text = str(val)
                    facts_out.append(
                        FactRow(
                            instrument_id=filing.instrument_id,
                            provider="sec",
                            provider_code=filing.provider_code,
                            accession=filing.accession,
                            form=filing.form,
                            filed_at=filing.filed_at,
                            accepted_at=filing.accepted_at,
                            known_at=filing.known_at,
                            asof=filing.asof,
                            tag_qname=f"{ns}:{tag}",
                            period_start=start_dt,
                            period_end=end_dt,
                            unit=unit_name,
                            currency=None,
                            dims_json=dims_json,
                            dims_key=dims_key,
                            dims_hash=dims_hash,
                            value_kind=value_kind,
                            value_num=value_num,
                            value_text=value_text,
                            statement=None,
                            line_item_code=None,
                            decimals=None,
                            attrs=None,
                        )
                    )

    if facts_out:
        logger.info(
            "sec fundamentals: fallback companyfacts facts=%s accession=%s cik=%s",
            len(facts_out),
            filing.accession,
            filing.provider_code,
        )
    else:
        logger.warning("sec fundamentals: companyfacts empty for accession=%s", filing.accession)
    return facts_out


def _fallback_zip_or_companyfacts(
    filing: FilingRow, edgar_cfg: SecEdgarConfig, cache: FileCache | None, allow_network: bool
) -> list[FactRow]:
    """
    Try to locate an xbrl.zip via index.json; if found, download and attempt basic JSON parse if a json file exists in the zip.
    Otherwise, fall back to companyfacts.
    """
    zip_bytes = _maybe_fetch_zip(filing, edgar_cfg, cache, allow_network)
    if zip_bytes:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                json_names = [n for n in zf.namelist() if n.lower().endswith(".json")]
                if not json_names:
                    facts_xml = _parse_xml_from_zip(zf, filing)
                    if facts_xml:
                        logger.info(
                            "sec fundamentals: parsed %s facts from xml instance (zip) accession=%s",
                            len(facts_xml),
                            filing.accession,
                        )
                        return facts_xml
                    logger.info(
                        "sec fundamentals: xbrl zip has no json; will fall back to companyfacts accession=%s",
                        filing.accession,
                    )
                for name in json_names:
                    try:
                        payload = json.loads(zf.read(name))
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
                        if facts:
                            logger.info(
                                "sec fundamentals: parsed %s facts from zip json name=%s accession=%s",
                                len(facts),
                                name,
                                filing.accession,
                            )
                            return facts
                    except Exception:
                        continue
        except Exception:
            logger.warning("sec fundamentals: zip fallback parse failed accession=%s", filing.accession)

    return _fallback_companyfacts(filing, edgar_cfg, cache, allow_network)


def _maybe_fetch_zip(
    filing: FilingRow, edgar_cfg: SecEdgarConfig, cache: FileCache | None, allow_network: bool
) -> bytes | None:
    if cache is None:
        cache = FileCache(ttl=edgar_cfg.cache_ttl)
    key = f"sec_zip_{filing.accession}"
    try:
        return cache.get(key, ttl=edgar_cfg.cache_ttl).value
    except Exception:
        pass

    if not allow_network and not edgar_cfg.allow_network:
        return None

    # Fetch index.json for the accession to locate xbrl zip
    cik_no_lead = filing.provider_code.lstrip("0")
    acc_dash = "{}-{}-{}".format(filing.accession[:10], filing.accession[10:12], filing.accession[12:])
    index_url = f"https://data.sec.gov/Archives/edgar/data/{cik_no_lead}/{filing.accession}/index.json"
    logger.info("sec fundamentals: fetch index json accession=%s url=%s", filing.accession, index_url)
    try:
        idx_raw = _http_get(index_url, edgar_cfg=edgar_cfg)
        idx = json.loads(idx_raw)
        files = idx.get("directory", {}).get("item", [])
        zip_name = None
        for f in files:
            name = f.get("name", "").lower()
            if name.endswith("xbrl.zip"):
                zip_name = f["name"]
                break
        if not zip_name:
            logger.info("sec fundamentals: no xbrl.zip listed in index.json accession=%s", filing.accession)
            return None
        zip_url = f"https://data.sec.gov/Archives/edgar/data/{cik_no_lead}/{filing.accession}/{zip_name}"
        logger.info("sec fundamentals: fetch xbrl zip accession=%s url=%s", filing.accession, zip_url)
        zip_bytes = _http_get(zip_url, edgar_cfg=edgar_cfg)
        try:
            cache.set(key, zip_bytes)
        except Exception:
            pass
        return zip_bytes
    except HTTPError as exc:
        if exc.code == 404:
            logger.info("sec fundamentals: index.json not found accession=%s", filing.accession)
            return None
        raise
    except RuntimeError:
        logger.warning("sec fundamentals: network unavailable for index/zip accession=%s", filing.accession)
        return None
    except Exception:
        return None


def _parse_xml_from_zip(zf: zipfile.ZipFile, filing: FilingRow) -> list[FactRow]:
    """
    Very minimal XML instance parser: extracts facts, period, unit, and explicit dimensions.
    """
    # Choose an instance file heuristic: first *.xml that is not a linkbase.
    candidates = [n for n in zf.namelist() if n.lower().endswith(".xml") and "cal" not in n.lower() and "def" not in n.lower() and "lab" not in n.lower() and "pre" not in n.lower()]
    if not candidates:
        return []
    try:
        xml_bytes = zf.read(candidates[0])
    except Exception:
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []

    nsmap = _nsmap(root)
    contexts = _parse_contexts(root, nsmap)
    units = _parse_units(root, nsmap)

    facts_out: list[FactRow] = []
    for elem in root:
        if "}" not in elem.tag:
            continue
        ns, local = elem.tag[1:].split("}", 1)
        tag_qname = f"{ns}:{local}"
        ctx_ref = elem.attrib.get("contextRef")
        if not ctx_ref or ctx_ref not in contexts:
            continue
        period_start, period_end, dims_key, dims_json, dims_hash = contexts[ctx_ref]
        unit_ref = elem.attrib.get("unitRef")
        unit = units.get(unit_ref, "unitless") if unit_ref else "unitless"
        val_text = elem.text
        value_kind = "number"
        value_num = None
        value_text = None
        try:
            value_num = float(val_text)
        except Exception:
            value_kind = "text"
            value_text = val_text or ""

        facts_out.append(
            FactRow(
                instrument_id=filing.instrument_id,
                provider="sec",
                provider_code=filing.provider_code,
                accession=filing.accession,
                form=filing.form,
                filed_at=filing.filed_at,
                accepted_at=filing.accepted_at,
                known_at=filing.known_at,
                asof=filing.asof,
                tag_qname=tag_qname,
                period_start=period_start,
                period_end=period_end,
                unit=unit,
                currency=None,
                dims_json=dims_json,
                dims_key=dims_key,
                dims_hash=dims_hash,
                value_kind=value_kind,
                value_num=value_num,
                value_text=value_text,
                statement=None,
                line_item_code=None,
                decimals=None,
                attrs=None,
            )
        )
    if facts_out:
        logger.info("sec fundamentals: parsed %s facts from xml instance accession=%s", len(facts_out), filing.accession)
    return facts_out


def _nsmap(root: ET.Element) -> dict[str, str]:
    nsmap: dict[str, str] = {}
    for k, v in root.attrib.items():
        if k.startswith("xmlns:"):
            nsmap[k.split(":", 1)[1]] = v
    return nsmap


def _parse_contexts(root: ET.Element, nsmap: dict[str, str]):
    contexts = {}
    for ctx in root.findall(".//{http://www.xbrl.org/2003/instance}context"):
        ctx_id = ctx.attrib.get("id")
        if not ctx_id:
            continue
        period = ctx.find("{http://www.xbrl.org/2003/instance}period")
        start_dt = None
        end_dt = None
        if period is not None:
            inst = period.find("{http://www.xbrl.org/2003/instance}instant")
            if inst is not None and inst.text:
                end_dt = datetime.fromisoformat(inst.text).replace(tzinfo=timezone.utc)
            start_el = period.find("{http://www.xbrl.org/2003/instance}startDate")
            end_el = period.find("{http://www.xbrl.org/2003/instance}endDate")
            if start_el is not None and start_el.text:
                start_dt = datetime.fromisoformat(start_el.text).replace(tzinfo=timezone.utc)
            if end_el is not None and end_el.text:
                end_dt = datetime.fromisoformat(end_el.text).replace(tzinfo=timezone.utc)
        dims = []
        for seg in ctx.findall(".//{http://www.xbrl.org/2003/instance}segment"):
            for exp in seg.findall(".//{http://xbrl.org/2006/xbrldi}explicitMember"):
                axis = exp.attrib.get("dimension")
                member = exp.text
                if axis and member:
                    dims.append((axis, member))
        dims_key = "|".join(f"{a}={m}" for a, m in sorted(dims))
        dims_json = (
            json.dumps([{"axis": a, "member": m} for a, m in sorted(dims)], separators=(",", ":"), sort_keys=True)
            if dims
            else "[]"
        )
        dims_hash = "" if not dims_key else hashlib.sha256(dims_key.encode("utf-8")).hexdigest()[:16]
        contexts[ctx_id] = (start_dt, end_dt or start_dt, dims_key, dims_json, dims_hash)
    return contexts


def _parse_units(root: ET.Element, nsmap: dict[str, str]):
    units = {}
    for unit in root.findall(".//{http://www.xbrl.org/2003/instance}unit"):
        unit_id = unit.attrib.get("id")
        if not unit_id:
            continue
        measure = unit.find("{http://www.xbrl.org/2003/instance}measure")
        if measure is not None and measure.text:
            units[unit_id] = measure.text
    return units


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
