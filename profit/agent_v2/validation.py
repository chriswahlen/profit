from __future__ import annotations

import json
from typing import Any, Iterable, List

from profit.agent_v2.exceptions import AgentV2ValidationError
from profit.agent_v2.models import Anchor, Request, RetrievalBatch, Step1Result, Step2Result


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AgentV2ValidationError(message)


def _parse_json(payload: str | dict) -> dict:
    if isinstance(payload, dict):
        return payload
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise AgentV2ValidationError(f"invalid JSON: {exc}") from exc


def parse_step1(raw: str | dict) -> Step1Result:
    data = _parse_json(raw)

    needs_data = bool(data.get("needs_data"))
    can_answer_now = bool(data.get("can_answer_now"))
    stop_reason = data.get("stop_reason")
    final_answer = data.get("final_answer")

    if can_answer_now and stop_reason == "answered":
        _require(final_answer, "final_answer is required when stop_reason=answered and can_answer_now=true")

    anchors_payload = data.get("anchors", []) or []
    anchors: list[Anchor] = []
    for anchor in anchors_payload:
        anchors.append(
            Anchor(
                id=str(anchor.get("id", "")),
                type=str(anchor.get("type", "")),
                priority=str(anchor.get("priority", "")),
                purpose=str(anchor.get("purpose", "")),
                time_range=anchor.get("time_range", {}),
                entity=anchor.get("entity"),
                extras={k: v for k, v in anchor.items() if k not in {"id", "type", "priority", "purpose", "time_range", "entity"}},
            )
        )

    return Step1Result(
        raw=data,
        needs_data=needs_data,
        can_answer_now=can_answer_now,
        stop_reason=stop_reason,
        data_needed_fluid=list(data.get("data_needed_fluid", []) or []),
        anchors=anchors,
        final_answer=final_answer,
        clarifying_questions=data.get("clarifying_questions"),
    )


def _validate_edgar_requests(requests: Iterable[dict]) -> None:
    for req in requests:
        if req.get("type") == "edgar_xbrl":
            aliases = req.get("params", {}).get("concept_aliases") or []
            _require(len(aliases) > 0, "edgar_xbrl requests must include concept_aliases")


def parse_step2(raw: str | dict) -> Step2Result:
    data = _parse_json(raw)
    batches_payload = data.get("batches", []) or []
    _require(len(batches_payload) > 0, "at least one batch is required")
    batches: list[RetrievalBatch] = []
    for batch in batches_payload:
        requests_payload = batch.get("requests", []) or []
        _validate_edgar_requests(requests_payload)
        requests: list[Request] = []
        for req in requests_payload:
            requests.append(
                Request(
                    request_id=str(req.get("request_id", "")),
                    type=str(req.get("type", "")),
                    params=req.get("params", {}) or {},
                    dataset=req.get("dataset"),
                )
            )
        batches.append(
            RetrievalBatch(
                batch_id=str(batch.get("batch_id", "")),
                purpose=str(batch.get("purpose", "")),
                requests=requests,
                depends_on_batches=list(batch.get("depends_on_batches", []) or []),
            )
        )

    return Step2Result(
        raw=data,
        batches=batches,
        entity_resolution_report=list(data.get("entity_resolution_report", []) or []),
    )

