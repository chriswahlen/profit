from __future__ import annotations

from profit.utils.html_to_md import html_to_markdown
import re


def convert_html_to_markdown_bytes(name: str, payload: bytes) -> bytes:
    """
    If ``name`` looks like an HTML document (.htm/.html), convert to markdown and
    return UTF-8 bytes; otherwise return the payload unchanged.
    """
    lower = name.lower()
    if not lower.endswith((".htm", ".html")):
        return payload
    try:
        text = payload.decode("utf-8", errors="ignore")
    except Exception:
        return payload
    if "<document>" in text.lower() and "<type>xml" in text.lower():
        md = _convert_document_html(text)
        if not md.strip():  # fallback if parsing failed
            md = _convert_document_text_only(text)
            if not md.strip():
                md = html_to_markdown(text)
    else:
        md = html_to_markdown(text)
    return md.encode("utf-8")


_DOC_PATTERN = re.compile(r"<document>(.*?)</document>", re.IGNORECASE | re.DOTALL)
_FIELD_PATTERN = re.compile(r"<(?P<field>[^>]+)>(?P<value>.*?)</(?P=field)>", re.IGNORECASE | re.DOTALL)


def _convert_document_html(text: str) -> str:
    parts: list[str] = []
    for match in _DOC_PATTERN.finditer(text):
        doc = match.group(1)
        fields: dict[str, str] = {}
        for field_match in _FIELD_PATTERN.finditer(doc):
            field = field_match.group("field").strip().lower()
            value = field_match.group("value").strip()
            fields[field] = value
        if fields.get("type", "").upper() != "XML":
            continue
        filename = fields.get("filename", "unknown")
        description = fields.get("description", "")
        sequence = fields.get("sequence", "")
        text_body = fields.get("text", "")
        text_md = html_to_markdown(text_body or "")
        header = f"### {filename} (sequence {sequence})"
        parts.append(header)
        if description:
            parts.append(f"**Description:** {description}")
        parts.append(text_md)
        parts.append("---")
    return "\n".join(parts).strip()


def _convert_document_text_only(text: str) -> str:
    match = _DOC_PATTERN.search(text)
    if not match:
        return ""
    doc = match.group(1)
    text_match = re.search(r"<text>(.*)</text>", doc, re.IGNORECASE | re.DOTALL)
    if not text_match:
        return ""
    inner = text_match.group(1)
    return html_to_markdown(inner)
