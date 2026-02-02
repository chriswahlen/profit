from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, List, Optional, Sequence
import re

ALLOWED_SOURCES = {"prices", "redfin", "edgar", "unknown"}
ALLOWED_AGGS = {"7d_avg", "14d_avg", "30d_avg", "weekly", "monthly_avg"}


@dataclass(frozen=True)
class SourceRequest:
    source: str
    instruments: Sequence[str] = field(default_factory=tuple)
    regions: Sequence[str] = field(default_factory=tuple)
    filings: Sequence[str] = field(default_factory=tuple)
    start: Optional[date] = None
    end: Optional[date] = None
    notes: Optional[str] = None
    max_points: int = 30
    aggregations: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class PlannerDecision:
    sources: List[SourceRequest]
    answer_prompt: str


def _parse_date(val: Any) -> Optional[date]:
    if val in (None, "", "null"):
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return date.fromisoformat(val)
        except ValueError:
            return None
    return None


def _clean_list(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, (list, tuple, set)):
        return [str(v) for v in val if v is not None]
    return [str(val)]


def _validate_source(src: str) -> str:
    return src if src in ALLOWED_SOURCES else "unknown"


def _validate_aggs(aggs: Iterable[str]) -> list[str]:
    return [agg for agg in aggs if agg in ALLOWED_AGGS]


def interpret_planner_output(raw: Any) -> PlannerDecision:
    """
    Parse planner JSON (dict or JSON string) into a PlannerDecision.

    Expected shape:
    {
      "sources": [ { ... } ],
      "answer_prompt": "..."
    }
    """
    if isinstance(raw, str):
        data = json.loads(raw)
    else:
        data = raw
    if not isinstance(data, dict):
        raise ValueError("planner output must be a JSON object")

    sources_raw = data.get("sources") or []
    if not isinstance(sources_raw, list):
        raise ValueError("sources must be a list")
    sources: List[SourceRequest] = []
    for entry in sources_raw:
        if not isinstance(entry, dict):
            continue
        source = _validate_source(str(entry.get("source", "unknown")))
        instruments = tuple(_clean_list(entry.get("instruments")))
        regions = tuple(_clean_list(entry.get("regions")))
        filings = tuple(_clean_list(entry.get("filings")))
        start = _parse_date(entry.get("start"))
        end = _parse_date(entry.get("end"))
        notes = entry.get("notes")
        max_points = int(entry.get("max_points", 30) or 30)
        aggs = tuple(_validate_aggs(_clean_list(entry.get("aggregations"))))
        sources.append(
            SourceRequest(
                source=source,
                instruments=instruments,
                regions=regions,
                filings=filings,
                start=start,
                end=end,
                notes=notes,
                max_points=max_points,
                aggregations=aggs,
            )
        )

    answer_prompt = data.get("answer_prompt") or ""
    return PlannerDecision(sources=sources, answer_prompt=answer_prompt)


JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def extract_planner_json(text: str) -> PlannerDecision:
    """
    Attempt to extract a planner JSON object from free-form LLM text.
    - Finds the first balanced-looking JSON block and parses it.
    - Falls back to ValueError if none found.
    """
    match = JSON_BLOCK_RE.search(text)
    if not match:
        raise ValueError("No JSON object found in LLM response")
    blob = match.group(0)
    return interpret_planner_output(blob)
