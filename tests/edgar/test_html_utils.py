from __future__ import annotations

from pathlib import Path
import os

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


def test_convert_html_document_sections():
    html = b"""
    <DOCUMENT>
      <TYPE>XML</TYPE>
      <SEQUENCE>50</SEQUENCE>
      <FILENAME>R39.htm</FILENAME>
      <DESCRIPTION>IDEA: XBRL DOCUMENT</DESCRIPTION>
      <TEXT><p>Line1</p><p>Line2</p></TEXT>
    </DOCUMENT>
    """
    out = convert_html_to_markdown_bytes("R39.htm", html).decode()
    assert "### R39.htm" in out
    assert "**Description:** IDEA: XBRL DOCUMENT" in out
    assert "Line1" in out and "Line2" in out


def test_convert_html_document_sections_fallback_on_empty():
    html = b"<DOCUMENT><TYPE>XML</TYPE><SEQUENCE>1</SEQUENCE><FILENAME>Empty.htm</FILENAME><DESCRIPTION>Empty</DESCRIPTION><TEXT></TEXT></DOCUMENT>"
    out = convert_html_to_markdown_bytes("Empty.htm", html).decode()
    assert out  # not empty


def test_convert_real_html_fixture():
    fixture_path = Path("tests/fixtures/edgar/sample_r8.htm")
    payload = fixture_path.read_bytes()
    out = convert_html_to_markdown_bytes("R8.htm", payload).decode()
    assert "Summary of Significant Accounting Policies" in out
    assert "<document>" not in out.lower()


def test_convert_real_html_matches_expected_fixture():
    html_path = Path("tests/fixtures/edgar/sample_r8.htm")
    expected_path = Path("tests/fixtures/edgar/sample_r8.md")
    payload = html_path.read_bytes()
    out = convert_html_to_markdown_bytes("R8.htm", payload).decode()

    def normalize_md(md: str) -> str:
        return "\n".join(line.rstrip() for line in md.splitlines() if line.strip())

    if os.getenv("UPDATE_EDGAR_HTML_MD") == "1":
        expected_path.write_text(normalize_md(out) + "\n")

    expected = expected_path.read_text()
    assert normalize_md(out) == normalize_md(expected)


def test_convert_real_r5_matches_expected_fixture():
    html_path = Path("tests/fixtures/edgar/sample_r5.htm")
    expected_path = Path("tests/fixtures/edgar/sample_r5.md")
    payload = html_path.read_bytes()
    out = convert_html_to_markdown_bytes("R5.htm", payload).decode()

    def normalize_md(md: str) -> str:
        return "\n".join(line.rstrip() for line in md.splitlines() if line.strip())

    if os.getenv("UPDATE_EDGAR_HTML_MD") == "1":
        expected_path.write_text(normalize_md(out) + "\n")

    expected = expected_path.read_text()
    assert normalize_md(out) == normalize_md(expected)
