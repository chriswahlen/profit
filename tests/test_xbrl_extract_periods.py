from datetime import datetime, timezone

import pytest

from profit.edgar.xbrl_extract import extract_finance_facts


def _build_xml():
    return b"""
    <xbrli:xbrl xmlns:xbrli=\"http://www.xbrl.org/2003/instance\">
      <xbrli:context id=\"ctx_inst\">
        <xbrli:entity><xbrli:identifier scheme=\"CIK\">0001</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:instant>2024-12-31</xbrli:instant></xbrli:period>
      </xbrli:context>
      <xbrli:context id=\"ctx_dur\">
        <xbrli:entity><xbrli:identifier scheme=\"CIK\">0001</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate><xbrli:endDate>2024-03-31</xbrli:endDate></xbrli:period>
      </xbrli:context>
      <xbrli:context id=\"ctx_bad\">
        <xbrli:entity><xbrli:identifier scheme=\"CIK\">0001</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:startDate>2024-01-01</xbrli:startDate></xbrli:period>
      </xbrli:context>
      <xbrli:unit id=\"USD\"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
      <revenue contextRef=\"ctx_dur\" unitRef=\"USD\" decimals=\"0\">100</revenue>
      <cash contextRef=\"ctx_inst\" unitRef=\"USD\" decimals=\"-3\">200</cash>
      <badfact contextRef=\"missing\" unitRef=\"USD\">1</badfact>
      <badfact2 contextRef=\"ctx_bad\" unitRef=\"USD\">2</badfact2>
    </xbrli:xbrl>
    """


def test_extract_periods_duration_and_instant():
    facts = extract_finance_facts(
        xml_bytes=_build_xml(),
        cik="0001",
        accession="0001-01",
        entity_id="entity:1",
        provider_id="sec:edgar",
        provider_entity_id="0001",
        report_id="10-Q",
        source_file="file.xml",
        source_url=None,
        asof=datetime(2025, 1, 1, tzinfo=timezone.utc),
        filed_at=datetime(2024, 12, 31, 16, 32, 14, tzinfo=timezone.utc),
    )
    assert len(facts) == 2
    dur = next(f for f in facts if f.report_key == "revenue")
    inst = next(f for f in facts if f.report_key == "cash")
    assert dur.period_start.date().isoformat() == "2024-01-01"
    assert dur.period_end.date().isoformat() == "2024-03-31"
    assert inst.period_start is None
    assert inst.period_end.date().isoformat() == "2024-12-31"
    assert dur.decimals == 0
    assert inst.decimals == -3
    assert dur.filed_at.isoformat() == "2024-12-31T16:32:14+00:00"


def test_extract_skips_invalid_and_missing_contexts(caplog):
    caplog.set_level("WARNING")
    facts = extract_finance_facts(
        xml_bytes=_build_xml(),
        cik="0001",
        accession="0001-01",
        entity_id="entity:1",
        provider_id="sec:edgar",
        provider_entity_id="0001",
        report_id="10-Q",
        source_file="file.xml",
        source_url=None,
        asof=datetime(2025, 1, 1, tzinfo=timezone.utc),
        filed_at=None,
    )
    assert len(facts) == 2  # bad context and missing context are skipped
    assert any("contexts missing usable period_end" in rec.message for rec in caplog.records)


def test_parse_datetime_strings_truncates_to_date():
    xml = b"""
    <xbrli:xbrl xmlns:xbrli=\"http://www.xbrl.org/2003/instance\">
      <xbrli:context id=\"ctx_inst\">
        <xbrli:entity><xbrli:identifier scheme=\"CIK\">0001</xbrli:identifier></xbrli:entity>
        <xbrli:period><xbrli:instant>2024-12-31T23:59:59Z</xbrli:instant></xbrli:period>
      </xbrli:context>
      <xbrli:unit id=\"USD\"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
      <cash contextRef=\"ctx_inst\" unitRef=\"USD\">5</cash>
    </xbrli:xbrl>
    """
    facts = extract_finance_facts(
        xml_bytes=xml,
        cik="0001",
        accession="0001-02",
        entity_id="entity:1",
        provider_id="sec:edgar",
        provider_entity_id="0001",
        report_id="10-K",
        source_file="file.xml",
        source_url=None,
        asof=datetime(2025, 1, 1, tzinfo=timezone.utc),
        filed_at=None,
    )
    assert len(facts) == 1
    assert facts[0].period_end.date().isoformat() == "2024-12-31"


def test_root_with_generic_prefix_processed():
    xml = b"""
    <ns0:xbrl xmlns:ns0=\"http://www.xbrl.org/2003/instance\">
      <ns0:context id=\"c1\"><ns0:entity><ns0:identifier scheme=\"CIK\">0001</ns0:identifier></ns0:entity><ns0:period><ns0:instant>2025-12-31</ns0:instant></ns0:period></ns0:context>
      <ns0:unit id=\"USD\"><ns0:measure>iso4217:USD</ns0:measure></ns0:unit>
      <ns0:cash contextRef=\"c1\" unitRef=\"USD\">7</ns0:cash>
    </ns0:xbrl>
    """
    facts = extract_finance_facts(
        xml_bytes=xml,
        cik="0001",
        accession="0001-03",
        entity_id="entity:1",
        provider_id="sec:edgar",
        provider_entity_id="0001",
        report_id="10-K",
        source_file="file.xml",
        source_url=None,
        asof=datetime(2025, 1, 1, tzinfo=timezone.utc),
        filed_at=None,
    )
    assert len(facts) == 1
    assert facts[0].period_end.date().isoformat() == "2025-12-31"
