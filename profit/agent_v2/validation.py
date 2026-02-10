"""Lightweight validation helpers for Agent V2 LLM outputs."""

from __future__ import annotations

import json
from typing import Any, List

from profit.agent_v2.exceptions import AgentV2ValidationError
from profit.agent_v2.models import (
    Anchor,
    Step1Result,
    Step2Result,
    RetrievalBatch,
    Request,
)


def _load_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:  # pragma: no cover - trivial
        raise AgentV2ValidationError("Payload must be valid JSON") from exc


def parse_step1(raw: str) -> Step1Result:
    """Validate and normalize the planner (step1) response."""

    payload = _load_json(raw)
    if not isinstance(payload, dict):
        raise AgentV2ValidationError("Step1 must be a JSON object")

    needs_data = bool(payload.get("needs_data", False))
    can_answer_now = bool(payload.get("can_answer_now", False))
    stop_reason = payload.get("stop_reason", "")
    final_answer = payload.get("final_answer")

    # If the LLM claims the question is answered, it must provide the answer text.
    if can_answer_now and not needs_data and stop_reason == "answered" and not final_answer:
        raise AgentV2ValidationError("Step1 marked as answered must include final_answer")

    data_needed_fluid = payload.get("data_needed_fluid", [])
    clarifying_questions = payload.get("clarifying_questions")

    anchors: List[Anchor] = payload.get("anchors", [])  # keep raw anchors for downstream JSON use

    return Step1Result(
        raw=payload,
        needs_data=needs_data,
        can_answer_now=can_answer_now,
        stop_reason=stop_reason,
        data_needed_fluid=list(data_needed_fluid or []),
        anchors=anchors,
        final_answer=final_answer,
        clarifying_questions=clarifying_questions,
    )


def parse_step2(raw: str) -> Step2Result:
    """Validate and normalize the retrieval-plan (step2) response."""

    payload = _load_json(raw)
    if not isinstance(payload, dict):
        raise AgentV2ValidationError("Step2 must be a JSON object")

    batches_payload = payload.get("batches", [])
    if not isinstance(batches_payload, list):
        raise AgentV2ValidationError("batches must be a list")

    batches: list[RetrievalBatch] = []
    for batch in batches_payload:
        reqs_payload = batch.get("requests", []) if isinstance(batch, dict) else []
        requests: list[Request] = []
        for req in reqs_payload:
            if not isinstance(req, dict):
                continue
            rtype = req.get("type")
            params = req.get("params", {})
            if rtype == "edgar_xbrl":
                concept_aliases = params.get("concept_aliases", [])
                if not concept_aliases:
                    raise AgentV2ValidationError("edgar_xbrl requests must include concept_aliases")

            requests.append(
                Request(
                    request_id=req.get("request_id", ""),
                    type=rtype,
                    params=params,
                    dataset=req.get("dataset"),
                )
            )

        batches.append(
            RetrievalBatch(
                batch_id=batch.get("batch_id", ""),
                purpose=batch.get("purpose", ""),
                requests=requests,
                depends_on_batches=batch.get("depends_on_batches", []),
            )
        )

    return Step2Result(
        raw=payload,
        batches=batches,
        entity_resolution_report=payload.get("entity_resolution_report", []),
    )


__all__ = ["parse_step1", "parse_step2"]
