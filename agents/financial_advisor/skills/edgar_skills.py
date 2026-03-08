from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence, Tuple

from agents.financial_advisor.skills.skill_interface import (
    SkillDescriptor,
    SkillExecutionResult,
    SkillInterface,
    SkillUsagePrompt,
)
from data_sources.edgar.common import normalize_cik
from data_sources.edgar.edgar_data_store import EdgarDataStore
from data_sources.entity import EntityStore

SEC_PROVIDER = "provider:edgar"
_PERIOD_TYPES = {"duration", "instant"}
_DEFAULT_LIMIT = 100


@dataclass(frozen=True)
class _EdgarSkillMeta:
    skill_id: str
    name: str
    summary: str
    prompt: str
    example_questions: Sequence[str]


@dataclass(frozen=True)
class _FactsSkillInput:
    cik: str
    concepts: Tuple[str, ...]
    accession: str | None
    period_type: str | None
    start: str | None
    end: str | None
    limit: int


class EdgarSkills(SkillInterface):
    """
    Provides agent-facing EDGAR capabilities: retrieving XBRL fact history.
    """

    SKILL_FACTS = "skill:edgar:facts"

    _SKILL_META = _EdgarSkillMeta(
        skill_id=SKILL_FACTS,
        name="EDGAR XBRL facts",
        summary="Return parsed EDGAR XBRL facts for requested concepts and CIK.",
        prompt="""
Provide a JSON payload that identifies the company (`cik` or canonical `symbol`), the
`concepts` you care about (qualified names like `us-gaap:Assets`), and optional filters.
Optional keys: `accession` (specific filing), `period_type` (`duration` or `instant`),
`start`/`end` (ISO dates to bound the context), and `limit` (max rows). The response
always returns JSON sorted newest context first, with each record containing the concept
qname, label, data type, numeric/text value, measure, accession, and context dates.

Example input:
{
  "symbol": "company:us:apple-inc",
  "concepts": ["us-gaap:Assets", "us-gaap:Liabilities"],
  "start": "2024-01-01",
  "end": "2024-12-31",
  "limit": 50
}

Example output:
{
  "skill_id": "skill:edgar:facts",
  "records": [
    {
      "accession": "0000320193-24-000001",
      "concept": "us-gaap:Assets",
      "label": "Assets",
      "value_numeric": 123000.0,
      "value_raw": "123000",
      "measure": "iso4217:USD",
      "period_type": "instant",
      "period_start": null,
      "period_end": "2024-12-31"
    }
  ]
}
""",
        example_questions=[
            "Get the latest Assets and Liabilities reported by CIK 0000320193.",
            "Show me quarterly NetIncomeLoss (duration) for sec:xnas:aapl between 2023 and 2024.",
        ],
    )

    def __init__(self, store: EdgarDataStore, entity_store: EntityStore, logger: logging.Logger | None = None) -> None:
        self._store = store
        self._entity_store = entity_store
        self._logger = logger or logging.getLogger(__name__)

    # --- SkillInterface -----------------------------------------------------
    def list_skills(self) -> Sequence[SkillDescriptor]:
        meta = self._SKILL_META
        return [SkillDescriptor(skill_id=meta.skill_id, name=meta.name, summary=meta.summary)]

    def describe_skill_usage(self, skill_id: str) -> SkillUsagePrompt:
        if skill_id != self.SKILL_FACTS:
            raise ValueError(f"Unknown EDGAR skill id {skill_id}")
        meta = self._SKILL_META
        return SkillUsagePrompt(
            skill_id=meta.skill_id,
            prompt=meta.prompt,
            example_questions=meta.example_questions,
        )

    def execute_skill(self, skill_id: str, payload: dict[str, Any]) -> SkillExecutionResult:
        if skill_id != self.SKILL_FACTS:
            raise ValueError(f"Unknown EDGAR skill id {skill_id}")

        inputs = self._parse_payload(payload)
        self._logger.info(
            "Running EDGAR fact skill cik=%s accession=%s period=%s start=%s end=%s limit=%d",
            inputs.cik,
            inputs.accession or "any",
            inputs.period_type or "any",
            inputs.start or "any",
            inputs.end or "any",
            inputs.limit,
        )

        rows = self._store.query_xbrl_facts(
            cik=inputs.cik,
            concept_qnames=inputs.concepts,
            accession=inputs.accession,
            period_type=inputs.period_type,
            start_date=inputs.start,
            end_date=inputs.end,
            limit=inputs.limit,
        )

        metadata = {
            "cik": inputs.cik,
            "concepts": list(inputs.concepts),
            "accession": inputs.accession,
            "period_type": inputs.period_type,
            "start": inputs.start,
            "end": inputs.end,
            "row_count": len(rows),
            "limit": inputs.limit,
        }
        return SkillExecutionResult(skill_id=skill_id, records=rows, metadata=metadata)

    # --- helpers ------------------------------------------------------------
    def _parse_payload(self, payload: dict[str, Any]) -> _FactsSkillInput:
        cik = payload.get("cik")
        symbol = payload.get("symbol") or payload.get("entity")
        if isinstance(cik, str) and cik.strip():
            normalized_cik = normalize_cik(cik)
        elif isinstance(symbol, str) and symbol.strip():
            normalized_cik = self._resolve_cik_from_symbol(symbol.strip())
        else:
            raise ValueError("payload must include either `cik` or `symbol`/`entity`")

        concepts_input = payload.get("concepts")
        if isinstance(concepts_input, str):
            concepts_input = [concepts_input]
        if not isinstance(concepts_input, Sequence):
            raise ValueError("concepts must be a list of concept qnames")
        concepts = tuple(
            concept.strip()
            for concept in concepts_input
            if isinstance(concept, str) and concept.strip()
        )
        if not concepts:
            raise ValueError("concepts list must include at least one qname")

        accession = payload.get("accession")
        if isinstance(accession, str):
            accession = accession.strip() or None
        else:
            accession = None

        period_type = payload.get("period_type")
        if period_type is not None:
            if not isinstance(period_type, str):
                raise ValueError("period_type must be a string when provided")
            period_type = period_type.strip().lower()
            if period_type not in _PERIOD_TYPES:
                raise ValueError(f"period_type must be one of {_PERIOD_TYPES}")
        start = payload.get("start")
        start_iso = self._parse_iso_date(start) if start else None
        end = payload.get("end")
        end_iso = self._parse_iso_date(end) if end else None

        limit = payload.get("limit", _DEFAULT_LIMIT)
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")

        return _FactsSkillInput(
            cik=normalized_cik,
            concepts=concepts,
            accession=accession,
            period_type=period_type,
            start=start_iso,
            end=end_iso,
            limit=limit,
        )

    def _resolve_cik_from_symbol(self, symbol: str) -> str:
        provider_ids = self._entity_store.provider_ids_for_entity(symbol, provider=SEC_PROVIDER)
        for provider, provider_entity_id in provider_ids:
            if provider == SEC_PROVIDER and provider_entity_id:
                return normalize_cik(provider_entity_id)
        raise ValueError(f"No EDGAR mapping found for entity {symbol}")

    def _parse_iso_date(self, raw: Any) -> str:
        if not isinstance(raw, str):
            raise ValueError("dates must be ISO-8601 strings (YYYY-MM-DD)")
        try:
            parsed = datetime.strptime(raw.strip(), "%Y-%m-%d")
            return parsed.date().isoformat()
        except ValueError as exc:
            raise ValueError(f"dates must follow YYYY-MM-DD ({raw})") from exc
