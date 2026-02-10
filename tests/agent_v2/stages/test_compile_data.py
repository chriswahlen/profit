from __future__ import annotations

from pathlib import Path

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Run
from llm.stub_llm import StubLLM

from agent_v2.insights_store import InsightsStore
from agent_v2.stages.compile_data import CompileDataStage


def test_compile_data_final_stores_insights_and_emits_final_response(tmp_path: Path):
    db = tmp_path / "agent.sqlite"
    insights_store = InsightsStore(db_path=db)
    insights_store.open()
    try:
        backend = StubLLM(
            {
                "STAGE: compile_data": (
                    '{"action":"final","final_answer":"Answer.","insights_to_store":[{"text":"Insight.","description":"Conclusion text.","tags":["t1"],'
                    '"start_date":"2024-01-01","end_date":"2024-12-31"}],"drop_dataset_keys":[],'
                    '"refined_tags":[],"refined_start_date":null,"refined_end_date":null}'
                )
            }
        )
        stage = CompileDataStage(backend=backend, insights_store=insights_store)
        parent = HistoryEntry(
            run_id="run_parent",
            parent_run_ids=[],
            logical_invocation_id="run_parent",
            attempt_number=1,
            stage_name="data_lookup_market",
            status="succeeded",
            timestamp=0.0,
            result="ok",
            metadata={
                "question": "Q",
                "prior_insights": [],
                "market_datasets": {"m1": {"kind": "synthetic_daily_series", "points": []}},
            },
        )
        user_context = {
            "question": "Q",
            "prior_insights": [],
            "market_datasets": {"m1": {"kind": "synthetic_daily_series", "points": []}},
        }
        fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
        assert isinstance(fragment, Run)
        assert fragment.stage_name == "final_response"

        assert user_context["final_answer"] == "Answer."

        hits = insights_store.search(tags=["t1"], start_date=None, end_date=None, limit=10)
        assert len(hits) == 1
        assert hits[0].insight.text == "Insight."
    finally:
        insights_store.close()


def test_compile_data_more_data_emits_query_prior_insights(tmp_path: Path):
    db = tmp_path / "agent.sqlite"
    insights_store = InsightsStore(db_path=db)
    insights_store.open()
    try:
        backend = StubLLM(
            {
                "STAGE: compile_data": (
                    '{"action":"more_data","final_answer":null,"insights_to_store":[],"drop_dataset_keys":[],'
                    '"refined_tags":["macro"],"refined_start_date":"2024-01-01","refined_end_date":"2024-12-31"}'
                )
            }
        )
        stage = CompileDataStage(backend=backend, insights_store=insights_store)
        parent = HistoryEntry(
            run_id="run_parent",
            parent_run_ids=[],
            logical_invocation_id="run_parent",
            attempt_number=1,
            stage_name="data_lookup_market",
            status="succeeded",
            timestamp=0.0,
            result="ok",
            metadata={"question": "Q", "prior_insights": [], "market_datasets": {}},
        )
        user_context = {
            "question": "Q",
            "prior_insights": [],
            "market_datasets": {},
            "tags": ["initial"],
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        }
        fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
        assert isinstance(fragment, Run)
        assert fragment.stage_name == "query_prior_insights"

        assert user_context["tags"] == ["macro"]
        assert user_context["start_date"] == "2024-01-01"
        assert user_context["end_date"] == "2024-12-31"
        assert user_context["question"] == "Q"
    finally:
        insights_store.close()
