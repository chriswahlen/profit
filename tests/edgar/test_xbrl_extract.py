from __future__ import annotations

from datetime import datetime, timezone

from profit.edgar.xbrl_extract import extract_finance_facts
from profit.sources.edgar.sec_edgar import SEC_PROVIDER_ID


SAMPLE = b"""
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:us-gaap="http://fasb.org/us-gaap/2020-01-31">
  <xbrli:context id="c1">
    <xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period>
  </xbrli:context>
  <xbrli:unit id="USD"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
  <us-gaap:Assets contextRef="c1" unitRef="USD" decimals="-6">12345.0</us-gaap:Assets>
</xbrli:xbrl>
"""


def test_extract_finance_facts_basic():
    facts = extract_finance_facts(
        xml_bytes=SAMPLE,
        cik="0000000001",
        accession="0000000001-25-000001",
        entity_id="ent1",
        provider_id=SEC_PROVIDER_ID,
        source_file="a.xml",
        source_url="http://example/a.xml",
        asof=datetime(2026, 1, 1, tzinfo=timezone.utc),
        provider_entity_id="0000000001",
    )

    assert len(facts) == 1
    fact = facts[0]
    assert fact.report_key == "Assets"
    assert fact.units == "USD"
    assert fact.period_end.year == 2025
    assert fact.value == 12345.0
    assert fact.attrs["context_period_type"] == "instant"
    assert fact.attrs["source_file"] == "a.xml"

