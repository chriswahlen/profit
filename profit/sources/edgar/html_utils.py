from __future__ import annotations

from profit.utils.html_to_md import html_to_markdown


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
    md = html_to_markdown(text)
    return md.encode("utf-8")
