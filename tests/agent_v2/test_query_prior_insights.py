from __future__ import annotations

from datetime import datetime, timezone
import json

import pytest

from profit.agent.insights import InsightStore
from profit.agent_v2.insights import InsightsManager
from profit.agent_v2.runners.query_prior_insights import QueryPriorInsightsRunner


def _history_entry(metadata: dict):
    return type("HistoryEntry", (), {"metadata": metadata})()


def test_query_prior_insights_returns_matches(tmp_path):
    store = InsightStore(path=tmp_path / "insights.sqlite")
    manager = InsightsManager(store=store)
    now_iso = datetime.now(timezone.utc).isoformat()
    store.store_insight(
        {
            "title": "Market signal",
            "body": ["Strong growth in Q1"],
            "tags": ["growth"],
            "source_provider": "agent_v2",
            "created_at": now_iso,
        }
    )

    runner = QueryPriorInsightsRunner(insights_manager=manager)
    metadata = {
        "user_context": {"approach": "Assess GOOG growth", "insights": [{"tags": ["growth"]}]},
        "question": "What is the outlook on GOOG?",
    }
    history = [_history_entry(metadata)]

    prompt = runner.get_prompt(previous_history_entries=history)
    assert "What is the outlook on GOOG?" in prompt
    assert '"growth"' in prompt

    result = runner.process_prompt(result="", previous_history_entries=history)
    assert getattr(result, "stage_name", None) == "final_response"
    assert runner.meta["prior_insights"][0]["title"] == "Market signal"
    assert runner.meta["user_context"]["approach"] == "Assess GOOG growth"
    assert runner.meta["data_queries"] == []


def test_query_prior_insights_parses_data_queries(tmp_path):
    store = InsightStore(path=tmp_path / "insights.sqlite")
    manager = InsightsManager(store=store)
    store.store_insight(
        {
            "title": "Market signal",
            "body": ["Strong growth in Q1"],
            "tags": ["growth"],
            "source_provider": "agent_v2",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    runner = QueryPriorInsightsRunner(insights_manager=manager)
    metadata = {
        "user_context": {"approach": "Assess GOOG growth", "insights": [{"tags": ["growth"]}]},
        "question": "What is the outlook on GOOG?",
    }
    history = [_history_entry(metadata)]

    payload = {
        "approach": "Refine conclusion",
        "data_queries": [
            {
                "type": "market",
                "description": "Need GOOG daily series",
                "filters": {"ticker": "XNAS:GOOG", "fields": ["close"]},
            },
            {
                "type": "sec",
                "description": "Need latest ET filings",
                "filters": {"cik": "0001652044"},
            },
        ],
    }
    runner.process_prompt(result=json.dumps(payload), previous_history_entries=history)

    assert runner.meta["user_context"]["approach"] == "Refine conclusion"
    assert len(runner.meta["data_queries"]) == 2
    assert runner.meta["data_queries"][0]["type"] == "market"
    assert runner.meta["data_queries"][1]["filters"]["cik"] == "0001652044"


@pytest.mark.parametrize(
    "data_query",
    [
        {
            "type": "market",
            "description": "Need GOOG daily series",
            "filters": {"ticker": "XNAS:GOOG", "fields": ["close"]},
        },
        {
            "type": "real_estate",
            "description": "Need NYC index",
            "filters": {"geo_id": "US:NYC"},
        },
        {
            "type": "sec",
            "description": "Pull latest 10-Q",
            "filters": {"cik": "0001652044", "period_type": "duration"},
        },
    ],
)
def test_query_prior_insights_data_query_types(tmp_path, data_query):
    store = InsightStore(path=tmp_path / "insights.sqlite")
    manager = InsightsManager(store=store)
    store.store_insight(
        {
            "title": "Market signal",
            "body": ["Strong growth in Q1"],
            "tags": ["growth"],
            "source_provider": "agent_v2",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    runner = QueryPriorInsightsRunner(insights_manager=manager)
    metadata = {
        "user_context": {"approach": "Assess GOOG growth", "insights": [{"tags": ["growth"]}]},
        "question": "What is the outlook on GOOG?",
    }
    history = [_history_entry(metadata)]

    stage = runner.process_prompt(
        result=json.dumps({"approach": "Refine conclusion", "data_queries": [data_query]}),
        previous_history_entries=history,
    )

    assert getattr(stage, "stage_name", None) == "final_response"
    assert runner.meta["data_queries"]
    assert runner.meta["data_queries"][0]["type"] == data_query["type"]
