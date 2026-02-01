from __future__ import annotations

import re
from xml.etree import ElementTree as ET

from profit.utils.html_to_md import html_to_markdown


def _looks_like_html(text: str) -> bool:
    return "<" in text and ">" in text


def markdown_textblocks(xml_bytes: bytes) -> bytes:
    """
    Walk XML and markdown-ify any element whose text looks like HTML or whose tag name
    contains 'textblock'. Returns serialized XML bytes.
    """
    root = ET.fromstring(xml_bytes)
    for elem in root.iter():
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if "textblock" not in tag.lower():
            continue

        # Build inner HTML (including children) for conversion.
        inner_parts = []
        if elem.text:
            inner_parts.append(elem.text)
        for child in list(elem):
            inner_parts.append(ET.tostring(child, encoding="unicode", method="html"))
        inner_html = "".join(inner_parts).strip()
        if not inner_html:
            continue

        md = html_to_markdown(inner_html)
        attrs = dict(elem.attrib)
        elem.clear()
        elem.attrib.update(attrs)
        elem.text = md

    return ET.tostring(root, encoding="utf-8", method="xml")
