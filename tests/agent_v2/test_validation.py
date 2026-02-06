from __future__ import annotations

import json

import pytest

from profit.agent_v2.validation import parse_step1, parse_step2
from profit.agent_v2.exceptions import AgentV2ValidationError


def _base_step1(**overrides):
    payload = {
        "context": {"user_query": "What is GOOG capex?", "approach": "Pull EDGAR capex facts."},
        "data_needed_fluid": ["Need capex for XNAS:GOOG for 2024 H1."],
        "needs_data": True,
        "can_answer_now": False,
        "stop_reason": "need_more_data",
        "anchors": [
            {
                "id": "edgar_1",
                "type": "edgar_xbrl",
                "priority": "must",
                "purpose": "CapEx in the window",
                "time_range": {"start_utc": "2024-01-01", "end_utc": "2024-06-01"},
                "entity": {"ticker": "GOOG", "exchange_mic": "XNAS"},
                "period_type": "duration",
                "grain": "quarterly",
                "metric": {
                    "kind": "capex",
                    "concept_qnames_allow": ["us-gaap:PaymentsToAcquirePropertyPlantAndEquipment"],
                },
                "dimensions": {"axis_qnames_allow": [], "member_qnames_allow": []},
                "units": {"measures_allow": ["USD"]},
            }
        ],
        "insight_ops": {"search": [], "store_candidates": []},
        "missing_sources": [],
    }
    payload.update(overrides)
    return payload


def test_step1_requires_final_answer_when_answered():
    payload = _base_step1(needs_data=False, can_answer_now=True, stop_reason="answered", anchors=[])
    with pytest.raises(AgentV2ValidationError):
        parse_step1(json.dumps(payload))


def test_step1_accepts_final_answer_when_answered():
    payload = _base_step1(
        needs_data=False,
        can_answer_now=True,
        stop_reason="answered",
        anchors=[],
        final_answer="Answer text.",
    )
    parsed = parse_step1(json.dumps(payload))
    assert parsed.final_answer == "Answer text."


def test_step2_requires_concept_aliases_for_edgar():
    payload = {
        "entity_resolution_report": [],
        "batches": [
            {
                "batch_id": "b1",
                "purpose": "edgar",
                "requests": [
                    {
                        "request_id": "edgar_q1",
                        "type": "edgar_xbrl",
                        "params": {
                            "cik": "0001652044",
                            "start_utc": "2024-01-01",
                            "end_utc": "2024-06-01",
                            "period_type": "duration",
                            "concept_aliases": [],
                            "limit": 100,
                        },
                    }
                ],
            }
        ],
    }
    with pytest.raises(AgentV2ValidationError):
        parse_step2(json.dumps(payload))


def test_step2_accepts_valid_edgar_request():
    payload = {
        "entity_resolution_report": [],
        "batches": [
            {
                "batch_id": "b1",
                "purpose": "edgar",
                "requests": [
                    {
                        "request_id": "edgar_q1",
                        "type": "edgar_xbrl",
                        "params": {
                            "cik": "0001652044",
                            "start_utc": "2024-01-01",
                            "end_utc": "2024-06-01",
                            "period_type": "duration",
                            "concept_aliases": ["CapitalExpenditures"],
                            "limit": 100,
                        },
                    }
                ],
            }
        ],
    }
    parsed = parse_step2(json.dumps(payload))
    assert parsed.batches[0].requests[0].type == "edgar_xbrl"
