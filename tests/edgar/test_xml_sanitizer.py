from __future__ import annotations

from profit.edgar.xml_sanitizer import markdown_textblocks


def test_markdown_textblocks_converts_html():
    xml = b"""
    <root>
      <us-gaap:SomeTextBlock xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31">Line1<br/>Line2</us-gaap:SomeTextBlock>
    </root>
    """
    out = markdown_textblocks(xml).decode()
    assert "Line1" in out
    assert "Line2" in out
    assert "<br" not in out.lower()
    assert "SomeTextBlock" in out


def test_markdown_textblocks_skips_plain():
    xml = b"<root><val>123</val></root>"
    out = markdown_textblocks(xml).decode()
    assert "123" in out
    assert "<val>123</val>" in out
