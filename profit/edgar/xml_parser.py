from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Dict, List, Optional, Tuple
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


def parse_xbrl(xml_bytes: bytes) -> ParsedXbrl:
    """
    Minimal XBRL extractor:
    - Finds numeric facts (xbrli:context/xbrli:unit aware) where text parses to float.
    - Anything numeric-looking that fails parse or is not numeric is recorded in `unparsed`.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ValueError(f"invalid XML: {exc}") from exc

    facts: list[ParsedFact] = []
    unparsed: list[dict[str, str]] = []

    # Simple heuristic: numeric facts have a "unitRef" or "decimals" attribute and text that parses to float.
    for elem in root.iter():
        if elem is root:
            continue
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        attrs = {k: v for k, v in elem.attrib.items()}
        text_raw = "".join(elem.itertext()).strip()
        if tag.lower() in HTML_SKIP_TAGS:
            continue
        text = text_raw
        inner_html = ""
        raw_xml = ET.tostring(elem, encoding="unicode", method="xml")
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

        val = _parse_float(text)
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
                )
            )
        else:
            if text:
                unparsed.append({"tag": tag, "text": text, "attrs": json.dumps(attrs)})

    return ParsedXbrl(facts=facts, unparsed=unparsed)
