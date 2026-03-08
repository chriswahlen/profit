from __future__ import annotations

import json
import logging
import re
# ... existing imports ...
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
    precision: int | None
    sign: int | None
    value_text: str
    footnote_html: str | None


@dataclass(frozen=True)
class ParsedXbrl:
    facts: list[ParsedFact]
    unparsed: list[dict[str, str]]


def _attr_value(attrs: dict[str, str], name: str) -> str | None:
    target = name.lower()
    for key, value in attrs.items():
        candidate = key.split("}")[-1].lower()
        if candidate == target:
            return value
    return None


def _parse_int_attr(attrs: dict[str, str], name: str) -> int | None:
    raw = _attr_value(attrs, name)
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    if stripped.lstrip("+-").isdigit():
        return int(stripped)
    return None


def _parse_sign(attrs: dict[str, str], text_raw: str) -> int | None:
    raw = _attr_value(attrs, "sign")
    if raw:
        stripped = raw.strip()
        if stripped.lstrip("+-").isdigit():
            return int(stripped)
        lowered = stripped.lower()
        if lowered.startswith("neg"):
            return -1
        if lowered.startswith("pos"):
            return 1
    if text_raw.startswith("-"):
        return -1
    if text_raw.startswith("+"):
        return 1
    return None


def _extract_footnote_html(elem: ET.Element) -> str | None:
    footnotes: list[str] = []
    for descendant in elem.findall(".//*"):
        tag = descendant.tag.split("}")[-1].lower()
        if tag == "footnote":
            footnotes.append(ET.tostring(descendant, encoding="unicode", method="xml").strip())
    return "\n".join(footnotes) if footnotes else None


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
                    precision=_parse_int_attr(attrs, "precision"),
                    sign=_parse_sign(attrs, text_raw),
                    value_text=lexical_value,
                    footnote_html=_extract_footnote_html(elem),
                )
            )
        else:
            if text_raw:
                unparsed.append({"tag": tag, "text": text_raw, "attrs": json.dumps(attrs, ensure_ascii=True)})

    return ParsedXbrl(facts=facts, unparsed=unparsed)
