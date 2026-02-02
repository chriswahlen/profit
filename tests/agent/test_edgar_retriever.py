from datetime import date
from pathlib import Path

from profit.agent import retrievers
from profit.agent.types import RetrievalPlan
from profit.agent.edgar_loader import load_chunks


def test_fetch_edgar_loads_local_docs(tmp_path):
    docs = tmp_path / "edgar"
    docs.mkdir()
    sample = docs / "sample.md"
    sample.write_text("Heading\n\nParagraph one about revenue.\n\nParagraph two.")

    plan = RetrievalPlan(source="edgar", filings=("0000123456",), start=date(2023, 1, 1), end=date(2024, 1, 1))
    result = retrievers.fetch(plan, edgar_docs_path=docs)
    payload = result.payload

    assert payload["provider"] == "edgar"
    assert payload["filings"] == ["0000123456"]
    assert len(payload["chunks"]) >= 2
    assert payload["chunks"][0]["file"] == "sample.md"


def test_load_chunks_adds_metadata(tmp_path):
    docs = tmp_path / "edgar"
    docs.mkdir()
    fname = "0000320193-24-000010_10-K.md"
    (docs / fname).write_text("Risk Factors\n\nThis is risk.\n\nMore text.")
    chunks = load_chunks(docs, keywords=["risk"])
    assert chunks
    meta = chunks[0]
    assert meta.accession == "0000320193-24-000010"
    assert meta.filing_type == "10-K"
    assert meta.cik == "0000320193"
    assert meta.score > 0


def test_load_chunks_ranks_by_keyword_count(tmp_path):
    docs = tmp_path / "edgar"
    docs.mkdir()
    (docs / "a.md").write_text("Risk risk risk.\n\nOther.")
    (docs / "b.md").write_text("Risk.\n\nLess.")
    chunks = load_chunks(docs, keywords=["risk"])
    assert len(chunks) >= 2
    assert chunks[0].file == "a.md"
    assert chunks[0].score > chunks[1].score
