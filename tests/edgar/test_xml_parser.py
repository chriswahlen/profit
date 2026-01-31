from __future__ import annotations

from profit.edgar.xml_parser import parse_xbrl, ParsedFact


SIMPLE_XBRL = b"""
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance">
  <us-gaap:Assets xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31" contextRef="c1" unitRef="USD" decimals="-6">12345.67</us-gaap:Assets>
  <us-gaap:Liabilities xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31" contextRef="c1" unitRef="USD" decimals="-6">890.1</us-gaap:Liabilities>
  <nonNumeric>Not a number</nonNumeric>
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
