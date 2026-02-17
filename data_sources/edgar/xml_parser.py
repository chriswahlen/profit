from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

HTML_SKIP_TAGS = {"b", "i", "span", "strong", "em", "u", "a", "p", "div", "br", "ul", "ol", "li"}


@dataclass(frozen=True)
class ParsedFact:
    name: str
    context_ref: str | None
    unit_ref: str | None
    value: float
    decimals: str | None
    id: str | None
    attrs: dict[str, str]
    lexical_value: str
    is_nil: int


@dataclass(frozen=True)
class ParsedXbrl:
    facts: list[ParsedFact]
    unparsed: list[dict[str, str]]


def _parse_float(text: str | None) -> Optional[float]:
    if text is None:
        return None
    try:
        return float(text)
    except Exception:
        return None


def parse_xbrl(xml_bytes: bytes, *, root: ET.Element | None = None) -> ParsedXbrl:
    """Minimal fact extractor for XBRL instance docs.

    We intentionally only parse numeric facts (float-able) and keep a small
    `unparsed` list for debugging / future expansion.
    """
    if root is None:
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as exc:
            raise ValueError(f"invalid XML: {exc}") from exc

    facts: list[ParsedFact] = []
    unparsed: list[dict[str, str]] = []

    for elem in root.iter():
        if elem is root:
            continue
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        attrs = {k: v for k, v in elem.attrib.items()}
        if tag.lower() in HTML_SKIP_TAGS:
            continue

        text_raw = "".join(elem.itertext()).strip()
        raw_xml = ET.tostring(elem, encoding="unicode", method="xml")
        inner_html = ""
        pattern = rf"<[^>]+?>(.*)</{re.escape(tag)}>$"
        match = re.search(pattern, raw_xml, re.DOTALL)
        if match:
            inner_html = match.group(1).strip()
        if not inner_html:
            inner_html = text_raw

        unit_ref = attrs.get("unitRef")
        decimals = attrs.get("decimals")
        context_ref = attrs.get("contextRef")
        fact_id = attrs.get("id")
        nil_attr = attrs.get("xsi:nil") or attrs.get("{http://www.w3.org/2001/XMLSchema-instance}nil")
        is_nil = 1 if nil_attr and nil_attr.lower() in {"1", "true"} else 0
        lexical_value = inner_html or text_raw

        val = _parse_float(text_raw)
        if val is not None:
            facts.append(
                ParsedFact(
                    name=tag,
                    context_ref=context_ref,
                    unit_ref=unit_ref,
                    value=val,
                    decimals=decimals,
                    id=fact_id,
                    attrs=attrs,
                    lexical_value=lexical_value,
                    is_nil=is_nil,
                )
            )
        else:
            if text_raw:
                unparsed.append({"tag": tag, "text": text_raw, "attrs": json.dumps(attrs, ensure_ascii=True)})

    return ParsedXbrl(facts=facts, unparsed=unparsed)

