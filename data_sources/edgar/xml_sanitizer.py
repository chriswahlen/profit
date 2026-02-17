from __future__ import annotations

from xml.etree import ElementTree as ET

from data_sources.edgar.html_to_md import html_to_markdown


def markdown_textblocks(xml_bytes: bytes) -> bytes:
    """Markdown-ify `*TextBlock*` elements in an XBRL instance.

    These nodes often contain HTML for narrative disclosures; converting to
    text keeps the content searchable and prevents accidental HTML rendering
    downstream.
    """
    root = ET.fromstring(xml_bytes)
    for elem in root.iter():
        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        if "textblock" not in tag.lower():
            continue

        inner_parts: list[str] = []
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

