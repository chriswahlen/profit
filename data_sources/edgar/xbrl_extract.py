from __future__ import annotations

"""XBRL parsing helpers used by the EDGAR ingestion pipeline."""

import hashlib
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)
INVALID_CONTEXT_LOG_LIMIT = 20


@dataclass(frozen=True)
class ParsedContext:
    id: str
    entity_scheme: str | None
    entity_id: str | None
    period_start: datetime | None
    period_end: datetime | None
    period_type: str  # "instant" or "duration" or "unknown"


@dataclass(frozen=True)
class ParsedUnit:
    id: str
    measures: List[str]


@dataclass(frozen=True)
class ParsedDimension:
    axis: str
    member: str | None
    typed_value: str | None


def _normalize_qname(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.strip()


def _canonicalize_typed_member(elem: ET.Element) -> str:
    text = (elem.text or "").strip()
    if text:
        return text
    serialized = ET.tostring(elem, encoding="utf-8", method="xml")
    return "HASH:" + hashlib.sha1(serialized).hexdigest()


def parse_context_dimensions(root: ET.Element) -> Dict[str, list[ParsedDimension]]:
    dims: dict[str, list[ParsedDimension]] = defaultdict(list)
    for ctx in root.findall(".//{http://www.xbrl.org/2003/instance}context"):
        ctx_id = ctx.get("id")
        if not ctx_id:
            continue
        for section in ("segment", "scenario"):
            container = ctx.find(f"{{http://www.xbrl.org/2003/instance}}{section}")
            if container is None:
                continue
            dims[ctx_id].extend(_collect_dimensions(container))
    return dims


def _collect_dimensions(container: ET.Element) -> list[ParsedDimension]:
    entries: list[ParsedDimension] = []
    for member in container.findall(".//{http://xbrl.org/2006/xbrldi}explicitMember"):
        axis = _normalize_qname(member.get("dimension"))
        member_val = _normalize_qname((member.text or "").strip())
        if axis and member_val:
            entries.append(ParsedDimension(axis=axis, member=member_val, typed_value=None))
    for member in container.findall(".//{http://xbrl.org/2006/xbrldi}typedMember"):
        axis = _normalize_qname(member.get("dimension"))
        if not axis:
            continue
        entries.append(ParsedDimension(axis=axis, member=None, typed_value=_canonicalize_typed_member(member)))
    return entries


def _parse_date(val: str | None) -> datetime | None:
    if not val:
        return None
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
    invalid: list[str] = []
    for elem in root.findall(".//{http://www.xbrl.org/2003/instance}context"):
        ctx_id = elem.get("id")
        if not ctx_id:
            continue
        entity_node = elem.find("{http://www.xbrl.org/2003/instance}entity")
        identifier_node = (
            entity_node.find("{http://www.xbrl.org/2003/instance}identifier") if entity_node is not None else None
        )
        entity_scheme = identifier_node.get("scheme") if identifier_node is not None else None
        entity_id = (identifier_node.text or "").strip() if identifier_node is not None and identifier_node.text else None
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
        contexts[ctx_id] = ParsedContext(
            id=ctx_id,
            entity_scheme=entity_scheme,
            entity_id=entity_id,
            period_start=period_start,
            period_end=period_end,
            period_type=period_type,
        )
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


def normalize_unit(measures: Iterable[str]) -> str | None:
    for m in measures:
        lower = m.lower()
        if lower.startswith("iso4217:"):
            return m.split(":", 1)[1].upper()
        if lower.endswith(":shares"):
            return "shares"
        if lower.endswith(":pure"):
            return "pure"
    return next(iter(measures), None)

