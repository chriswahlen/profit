from __future__ import annotations

from profit.sources.edgar import convert_html_to_markdown_bytes


def test_convert_html_to_markdown_bytes_converts_htm():
    html = b"<html><body><b>Bold</b></body></html>"
    out = convert_html_to_markdown_bytes("file.htm", html)
    assert b"**Bold**" in out
    assert out != html


def test_convert_html_to_markdown_bytes_passthrough_non_html():
    payload = b"plain text"
    out = convert_html_to_markdown_bytes("file.txt", payload)
    assert out == payload
