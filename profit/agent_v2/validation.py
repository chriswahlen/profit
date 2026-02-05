from __future__ import annotations

import json
import re
from typing import Any

from pydantic import ValidationError

from profit.agent_v2.exceptions import AgentV2ValidationError
from profit.agent_v2.models import RetrievalPlan, Step1Payload


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_step1(text: str) -> Step1Payload:
    payload = _load_json(text)
    try:
        parsed = Step1Payload.model_validate(payload)
    except ValidationError as exc:
        raise AgentV2ValidationError(f"step1 payload failed validation: {exc}") from exc
    _validate_step1(parsed)
    return parsed


def parse_step2(text: str) -> RetrievalPlan:
    payload = _load_json(text)
    try:
        parsed = RetrievalPlan.model_validate(payload)
    except ValidationError as exc:
        raise AgentV2ValidationError(f"step2 payload failed validation: {exc}") from exc
    _validate_step2(parsed)
    return parsed


def _load_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise AgentV2ValidationError("response is not valid JSON") from exc


def _validate_step1(payload: Step1Payload) -> None:
    _ensure_unique([a.id for a in payload.anchors], "anchors.id")
    for anchor in payload.anchors:
        _validate_date_range(anchor.time_range.start_utc, anchor.time_range.end_utc, f"anchor[{anchor.id}].time_range")

    if payload.stop_reason == "answered" or payload.can_answer_now:
        if not payload.final_answer:
            raise AgentV2ValidationError("final_answer is required when can_answer_now=true or stop_reason=answered")

    if payload.stop_reason == "need_clarification":
        if not payload.clarifying_questions:
            raise AgentV2ValidationError("clarifying_questions is required when stop_reason=need_clarification")


def _validate_step2(plan: RetrievalPlan) -> None:
    _ensure_unique([b.batch_id for b in plan.batches], "batches.batch_id")
    all_request_ids: list[str] = []
    for batch in plan.batches:
        all_request_ids.extend([r.request_id for r in batch.requests])
    _ensure_unique(all_request_ids, "requests.request_id")

    for batch in plan.batches:
        for request in batch.requests:
            if request.type == "market_ohlcv":
                _validate_date_range(request.params.start_utc, request.params.end_utc, f"request[{request.request_id}].params")
            else:
                _validate_sql(request.params.sql, request.params.max_rows, f"request[{request.request_id}].sql")


def _ensure_unique(values: list[str], label: str) -> None:
    seen: set[str] = set()
    dups: set[str] = set()
    for value in values:
        if value in seen:
            dups.add(value)
        seen.add(value)
    if dups:
        raise AgentV2ValidationError(f"{label} must be unique; duplicates: {sorted(dups)}")


def _validate_date_range(start: str, end: str, label: str) -> None:
    if not _DATE_RE.match(start or ""):
        raise AgentV2ValidationError(f"{label}.start_utc must be YYYY-MM-DD")
    if not _DATE_RE.match(end or ""):
        raise AgentV2ValidationError(f"{label}.end_utc must be YYYY-MM-DD")
    if start > end:
        raise AgentV2ValidationError(f"{label} start_utc must be <= end_utc")


_SQL_START_RE = re.compile(r"^\s*(with|select)\b", re.IGNORECASE)
_SQL_DISALLOWED_RE = re.compile(r"\b(attach|detach|pragma|vacuum|insert|update|delete|drop|alter|create|replace)\b", re.IGNORECASE)


def _validate_sql(sql: str, max_rows: int, label: str) -> None:
    if not _SQL_START_RE.match(sql or ""):
        raise AgentV2ValidationError(f"{label} must start with SELECT or WITH")
    stripped = (sql or "").strip()
    if ";" in stripped[:-1]:
        raise AgentV2ValidationError(f"{label} must be a single statement (no internal semicolons)")
    if _SQL_DISALLOWED_RE.search(sql):
        raise AgentV2ValidationError(f"{label} contains disallowed statement/keyword")
    if "limit" not in sql.lower():
        raise AgentV2ValidationError(f"{label} must include LIMIT (max_rows={max_rows})")

