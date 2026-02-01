from __future__ import annotations

from profit.edgar.xml_parser import parse_xbrl, ParsedFact


SIMPLE_XBRL = b"""
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance">
  <us-gaap:Assets xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31" contextRef="c1" unitRef="USD" decimals="-6">12345.67</us-gaap:Assets>
  <us-gaap:Liabilities xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31" contextRef="c1" unitRef="USD" decimals="-6">890.1</us-gaap:Liabilities>
  <nonNumeric>Not a number</nonNumeric>
</xbrli:xbrl>
"""

HTML_XBRL = b"""
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance">
  <us-gaap:Revenue xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31" contextRef="c2" unitRef="USD" decimals="-6">5000</us-gaap:Revenue>
  <notes>See <b>details</b> in <i>Appendix</i>.</notes>
</xbrli:xbrl>
"""

TEXTBLOCK_XBRL = b"""
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance">
  <us-gaap:SomeTextBlock xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31"><p>Line1</p><p>Line2</p></us-gaap:SomeTextBlock>
</xbrli:xbrl>
"""

def test_parse_xbrl_extracts_numeric_facts():
    parsed = parse_xbrl(SIMPLE_XBRL)

    assert len(parsed.facts) == 2
    names = {f.name for f in parsed.facts}
    assert names == {"Assets", "Liabilities"}

    assets = next(f for f in parsed.facts if f.name == "Assets")
    assert assets.context_ref == "c1"
    assert assets.unit_ref == "USD"
    assert assets.value == 12345.67

    assert len(parsed.unparsed) == 1
    assert parsed.unparsed[0]["tag"] == "nonNumeric"


def test_parse_xbrl_converts_html_unparsed():
    parsed = parse_xbrl(HTML_XBRL)
    assert len(parsed.facts) == 1
    assert len(parsed.unparsed) == 1
    # Without HTML conversion, tags are stripped to plain text.
    assert "See details in Appendix." in parsed.unparsed[0]["text"]


def test_parse_xbrl_converts_textblock_html():
    parsed = parse_xbrl(TEXTBLOCK_XBRL)
    assert len(parsed.unparsed) == 1
    # Raw inner HTML retained
    assert "Line1" in parsed.unparsed[0]["text"]
