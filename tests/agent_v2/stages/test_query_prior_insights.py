from __future__ import annotations

from datetime import date
from pathlib import Path

from agentapi.history_entry import HistoryEntry
from agentapi.plan import Join, Run
from llm.stub_llm import StubLLM

from agent_v2.insights_store import InsightsStore
from agent_v2.models import Insight
from agent_v2.stages.query_prior_insights import QueryPriorInsightsStage


def _parent_entry(metadata: dict) -> HistoryEntry:
    return HistoryEntry(
        run_id="run_parent",
        parent_run_ids=[],
        logical_invocation_id="run_parent",
        attempt_number=1,
        stage_name="initial_prompt",
        status="succeeded",
        timestamp=0.0,
        result="ok",
        metadata=metadata,
    )


def test_query_prior_insights_emits_join_when_requests(tmp_path: Path):
    db = tmp_path / "agent.sqlite"
    store = InsightsStore(db_path=db)
    store.open()
    try:
        store.add(
            [
                Insight(
                    text="Rates were elevated through mid-2024.",
                    description="Rates stayed higher than average in the first half.",
                    tags=("rates", "macro"),
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 6, 30),
                )
            ]
        )
        backend = StubLLM(
            {
                "STAGE: query_prior_insights": (
                    '{"market_requests":[{"key":"m1","request":"S&P 500 2024 daily close","why":"context"}],'
                    '"real_estate_requests":[],"sec_requests":[],"additional_insight_tags":["equities"]}'
                )
            }
        )
        stage = QueryPriorInsightsStage(backend=backend, insights_store=store)
        parent = _parent_entry(
            {
                "question": "How did markets react to rate policy in 2024?",
                "tags": ["rates"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            }
        )
        user_context = {
            "question": "How did markets react to rate policy in 2024?",
            "tags": ["rates"],
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        }
        fragment = stage.run(previous_history_entries=[parent], user_context=user_context)
        assert isinstance(fragment, Join)
        assert isinstance(fragment.then, Run)
        assert fragment.then.stage_name == "compile_data"

        assert user_context["question"].startswith("How did markets react")
        assert user_context["prior_insights"]
        assert user_context["market_requests"][0]["key"] == "m1"
        assert "equities" in user_context["tags"]
    finally:
        store.close()
