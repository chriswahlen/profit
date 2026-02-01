from __future__ import annotations

"""Lightweight XBRL-to-FinanceFact transformer.

This module walks an XBRL instance document and emits normalized finance
facts suitable for insertion into ``company_finance_fact``. It is intentionally
minimal: we only parse contexts, units, and numeric facts, and we ignore
presentation/calculation/linkbase data.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional
from xml.etree import ElementTree as ET

from profit.catalog.types import FinanceFactRecord
from profit.edgar.xml_parser import ParsedXbrl, parse_xbrl

logger = logging.getLogger(__name__)


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


# --- Parsing helpers -------------------------------------------------------


def _parse_date(val: str | None) -> datetime | None:
    if not val:
        return None
    try:
        # XBRL dates are date-only; store as UTC midnight to keep comparisons sane.
        return datetime.strptime(val, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_contexts(root: ET.Element) -> Dict[str, ParsedContext]:
    contexts: Dict[str, ParsedContext] = {}
    ns_sep = "}"
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
        contexts[ctx_id] = ParsedContext(id=ctx_id, period_start=period_start, period_end=period_end, period_type=period_type)
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
    source_file: str,
    source_url: str | None,
    asof: datetime,
    provider_entity_id: str,
) -> list[FinanceFactRecord]:
    """Transform an XBRL instance into FinanceFactRecord rows.

    - ``report_id`` is the XBRL filename.
    - ``report_key`` is the local tag name; namespace URI is captured in attrs.
    """

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:  # pragma: no cover - validated upstream
        logger.warning("invalid XML skipped file=%s err=%s", source_file, exc)
        return []

    contexts = parse_contexts(root)
    units = parse_units(root)
    parsed = parse_xbrl(xml_bytes)

    facts: list[FinanceFactRecord] = []
    for fact in parsed.facts:
        ctx = contexts.get(fact.context_ref or "")
        if ctx is None or ctx.period_end is None:
            continue  # cannot place the fact in time
        unit = units.get(fact.unit_ref or "")
        normalized_unit = _normalize_unit(unit.measures) if unit else None
        if normalized_unit is None:
            continue

        attrs = dict(fact.attrs)
        # Capture provenance and timing hints
        attrs.update(
            {
                "context_period_type": ctx.period_type,
                "context_period_start": ctx.period_start.isoformat() if ctx.period_start else None,
                "source_file": source_file,
                "source_url": source_url,
                "namespace": attrs.get("xmlns") or None,
            }
        )

        record = FinanceFactRecord(
            entity_id=entity_id,
            provider_id=provider_id,
            provider_entity_id=provider_entity_id,
            record_id=accession,
            report_id=source_file,
            report_key=fact.name,
            period_end=ctx.period_end,
            units=normalized_unit,
            value=fact.value,
            asof=asof,
            attrs=attrs,
        )
        facts.append(record)

    return facts

