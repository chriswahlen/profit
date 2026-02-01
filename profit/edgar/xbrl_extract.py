from __future__ import annotations

"""Lightweight XBRL-to-FinanceFact transformer.

This module walks an XBRL instance document and emits normalized finance
facts suitable for insertion into ``company_finance_fact``. It is intentionally
minimal: we only parse contexts, units, and numeric facts, and we ignore
presentation/calculation/linkbase data.
"""

import logging
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional, Tuple
from xml.etree import ElementTree as ET

from profit.catalog.types import FinanceFactRecord
from profit.edgar.xml_parser import ParsedXbrl, parse_xbrl

logger = logging.getLogger(__name__)
INVALID_CONTEXT_LOG_LIMIT = 20


# --- Parsed helpers ---------------------------------------------------------


@dataclass(frozen=True)
class ParsedContext:
    id: str
    period_start: datetime | None
    period_end: datetime | None
    period_type: str  # "instant" or "duration" or "unknown"


@dataclass(frozen=True)
class ParsedUnit:
    id: str
    measures: List[str]

# --- Dimensions helpers -----------------------------------------------------

def _normalize_qname(raw: str | None) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    return raw


def _canonicalize_typed_member(elem: ET.Element) -> str:
    text = (elem.text or "").strip()
    if text:
        return text
    serialized = ET.tostring(elem, encoding="utf-8", method="xml")
    return "HASH:" + hashlib.sha1(serialized).hexdigest()


def _context_dimensions(root: ET.Element) -> Dict[str, tuple[str, bool, str]]:
    """
    Build ctx_id -> (dimensions_sig, is_consolidated, canonical_dim_string).

    - is_consolidated: True when segment is absent or empty; False when segment exists and has dimensional content.
    - dimensions_sig: SHA1 of sorted dimension tokens; empty string when consolidated with no dimensions.
    - canonical_dim_string: pipe-joined tokens or hash marker for unparsed segment content.
    """
    ctx_dims: Dict[str, tuple[str, bool, str]] = {}
    for ctx in root.findall(".//{http://www.xbrl.org/2003/instance}context"):
        ctx_id = ctx.get("id")
        if not ctx_id:
            continue
        segment = ctx.find("{http://www.xbrl.org/2003/instance}entity/{http://www.xbrl.org/2003/instance}segment")
        if segment is None:
            ctx_dims[ctx_id] = ("", True, "")
            continue

        tokens: list[str] = []
        explicit_members = segment.findall(".//{http://xbrl.org/2006/xbrldi}explicitMember")
        typed_members = segment.findall(".//{http://xbrl.org/2006/xbrldi}typedMember")

        for em in explicit_members:
            axis = _normalize_qname(em.get("dimension"))
            member = _normalize_qname((em.text or "").strip())
            if axis and member:
                tokens.append(f"{axis}={member}")

        for tm in typed_members:
            axis = _normalize_qname(tm.get("dimension"))
            if not axis:
                continue
            typed_val = _canonicalize_typed_member(tm)
            tokens.append(f"{axis}=TYPED({typed_val})")

        if not tokens and len(list(segment)) == 0:
            ctx_dims[ctx_id] = ("", True, "")
            continue

        if not tokens and len(list(segment)) > 0:
            ser = ET.tostring(segment, encoding="utf-8", method="xml")
            canonical_dim_string = "SEGMENT_HASH:" + hashlib.sha1(ser).hexdigest()
            dim_sig = hashlib.sha1(canonical_dim_string.encode()).hexdigest()
            ctx_dims[ctx_id] = (dim_sig, False, canonical_dim_string)
            continue

        tokens = sorted(tokens)
        canonical_dim_string = "|".join(tokens)
        dim_sig = hashlib.sha1(canonical_dim_string.encode()).hexdigest() if canonical_dim_string else ""
        ctx_dims[ctx_id] = (dim_sig, False, canonical_dim_string)
    return ctx_dims

# --- Parsing helpers -------------------------------------------------------


def _parse_date(val: str | None) -> datetime | None:
    if not val:
        return None
    # XBRL should be date-only, but some vendors emit dateTime; normalize to date.
    try:
        dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(val, "%Y-%m-%d")
        except Exception:
            return None
    date_part = dt.date()
    return datetime.combine(date_part, datetime.min.time(), tzinfo=timezone.utc)


def parse_contexts(root: ET.Element) -> Dict[str, ParsedContext]:
    contexts: Dict[str, ParsedContext] = {}
    ns_sep = "}"
    invalid: list[str] = []
    for elem in root.findall(".//{http://www.xbrl.org/2003/instance}context"):
        ctx_id = elem.get("id")
        if not ctx_id:
            continue
        period_node = elem.find("{http://www.xbrl.org/2003/instance}period")
        period_start: datetime | None = None
        period_end: datetime | None = None
        period_type = "unknown"
        if period_node is not None:
            inst = period_node.find("{http://www.xbrl.org/2003/instance}instant")
            start = period_node.find("{http://www.xbrl.org/2003/instance}startDate")
            end = period_node.find("{http://www.xbrl.org/2003/instance}endDate")
            if inst is not None and inst.text:
                period_type = "instant"
                period_end = _parse_date(inst.text.strip())
            elif start is not None and end is not None:
                period_type = "duration"
                period_start = _parse_date(start.text.strip()) if start.text else None
                period_end = _parse_date(end.text.strip()) if end.text else None
        if period_end is None:
            invalid.append(ctx_id)
            continue
        contexts[ctx_id] = ParsedContext(id=ctx_id, period_start=period_start, period_end=period_end, period_type=period_type)
    if invalid:
        logger.warning(
            "xbrl contexts missing usable period_end count=%s examples=%s",
            len(invalid),
            invalid[:INVALID_CONTEXT_LOG_LIMIT],
        )
    return contexts


def parse_units(root: ET.Element) -> Dict[str, ParsedUnit]:
    units: Dict[str, ParsedUnit] = {}
    for elem in root.findall(".//{http://www.xbrl.org/2003/instance}unit"):
        unit_id = elem.get("id")
        if not unit_id:
            continue
        measures: List[str] = []
        for meas in elem.findall("{http://www.xbrl.org/2003/instance}measure"):
            if meas.text:
                measures.append(meas.text.strip())
        if measures:
            units[unit_id] = ParsedUnit(id=unit_id, measures=measures)
    return units


# --- Unit normalization -----------------------------------------------------


def _normalize_unit(measures: Iterable[str]) -> str | None:
    # Simple mapping: ISO 4217 currencies, shares, pure.
    for m in measures:
        lower = m.lower()
        if lower.startswith("iso4217:"):
            return m.split(":", 1)[1].upper()
        if lower.endswith(":shares"):
            return "shares"
        if lower.endswith(":pure"):
            return "pure"
    # fallback to first measure text to avoid losing data
    first = next(iter(measures), None)
    return first


# --- Public API -------------------------------------------------------------


def extract_finance_facts(
    *,
    xml_bytes: bytes,
    cik: str,
    accession: str,
    entity_id: str,
    provider_id: str,
    provider_entity_id: str,
    report_id: str,
    source_file: str,
    source_url: str | None,
    asof: datetime,
    filed_at: datetime | None,
    amendment_flag: bool | None,
) -> list[FinanceFactRecord]:
    """Transform an XBRL instance into FinanceFactRecord rows.

    - ``report_id`` is the form type (e.g., 10-K, 10-Q, 8-K).
    - ``report_key`` is the local tag name; namespace URI is captured in attrs.
    """

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:  # pragma: no cover - validated upstream
        logger.warning("invalid XML skipped accession=%s file=%s err=%s", accession, source_file, exc)
        return []

    def _has_xbrl(elem: ET.Element) -> bool:
        tag = elem.tag.lower()
        if tag.endswith("xbrl"):
            return True
        for child in elem.iter():
            if child is elem:
                continue
            if child.tag.lower().endswith("xbrl"):
                return True
        return False

    if not _has_xbrl(root):
        logger.info("skip non-xbrl xml accession=%s file=%s", accession, source_file)
        return []

    contexts = parse_contexts(root)
    ctx_dims = _context_dimensions(root)
    units = parse_units(root)
    parsed = parse_xbrl(xml_bytes)

    facts: list[FinanceFactRecord] = []
    for fact in parsed.facts:
        ctx = contexts.get(fact.context_ref or "")
        if ctx is None or ctx.period_end is None:
            logger.debug("skip fact name=%s missing context_ref=%s", fact.name, fact.context_ref)
            continue  # cannot place the fact in time
        dim_sig, is_cons, canonical_dims = ctx_dims.get(fact.context_ref or "", ("", True, ""))
        unit = units.get(fact.unit_ref or "")
        normalized_unit = _normalize_unit(unit.measures) if unit else None
        if normalized_unit is None:
            continue

        attrs = dict(fact.attrs)
        # Capture provenance and timing hints
        attrs.update(
            {
                "context_period_type": ctx.period_type,
                # Do not duplicate fields already stored as columns.
                "source_file": source_file,
                "source_url": source_url,
                "namespace": attrs.get("xmlns") or None,
                "dimensions_canonical": canonical_dims or None,
            }
        )
        # Strip attrs that are None/empty to keep payload lean.
        attrs = {k: v for k, v in attrs.items() if v not in (None, "")}

        record = FinanceFactRecord(
            entity_id=entity_id,
            provider_id=provider_id,
            provider_entity_id=provider_entity_id,
            record_id=accession,
            report_id=report_id,
            report_key=fact.name,
            period_start=ctx.period_start,
            period_end=ctx.period_end,
            units=normalized_unit,
            value=fact.value,
            decimals=int(fact.decimals) if fact.decimals and fact.decimals.strip("-").isdigit() else None,
            dimensions_sig=dim_sig,
            is_consolidated=is_cons,
            amendment_flag=amendment_flag,
            filed_at=filed_at,
            asof=asof,
            attrs=attrs,
        )
        facts.append(record)

    return facts
