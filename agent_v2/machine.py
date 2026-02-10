from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from agentapi.components.sqlite_snapshot_store import SqliteSnapshotStore
from agentapi.state_machine import StateMachine
from llm.chatgpt_llm import ChatGPTLLM
from llm.stub_llm import StubLLM

from agent_v2 import STAGE_REGISTRY_VERSION
from agent_v2.constants import (
    EXECUTION_ID_DEFAULT,
    STAGE_COMPILE_DATA,
    STAGE_DATA_LOOKUP_MARKET,
    STAGE_DATA_LOOKUP_REAL_ESTATE,
    STAGE_DATA_LOOKUP_SEC,
    STAGE_FINAL_RESPONSE,
    STAGE_INITIAL_PROMPT,
    STAGE_QUERY_PRIOR_INSIGHTS,
)
from agent_v2.insights_store import InsightsStore
from agent_v2.stages.compile_data import CompileDataStage
from agent_v2.stages.data_lookup_market import DataLookupMarketStage
from agent_v2.stages.data_lookup_real_estate import DataLookupRealEstateStage
from agent_v2.stages.data_lookup_sec import DataLookupSECStage
from agent_v2.stages.final_response import FinalResponseStage
from agent_v2.stages.initial_prompt import InitialPromptStage
from agent_v2.stages.query_prior_insights import QueryPriorInsightsStage


@dataclass(frozen=True)
class BuiltAgent:
    machine: StateMachine
    snapshot_store: SqliteSnapshotStore
    insights_store: InsightsStore


def build_agent(
    *,
    question: str,
    db_path: Path,
    execution_id: str = EXECUTION_ID_DEFAULT,
    live: bool = False,
) -> BuiltAgent:
    snapshot_store = SqliteSnapshotStore(db_path=db_path)
    snapshot_store.open()

    insights_store = InsightsStore(db_path=db_path)
    insights_store.open()

    if live:
        backend = ChatGPTLLM()
    else:
        backend = StubLLM(
            {
                "STAGE: initial_prompt": '{"tags":["macro","markets"],"start_date":null,"end_date":null}',
                "STAGE: query_prior_insights": '{"market_requests":[{"key":"m1","request":"S&P 500 2024 daily close","why":"baseline market context"}],"real_estate_requests":[],"sec_requests":[],"additional_insight_tags":[]}',
                "STAGE: compile_data": '{"action":"final","final_answer":"[stubbed] Here is a concise finance/econ answer based on the provided data.","insights_to_store":[{"text":"S&P 500 showed typical daily variability in 2024 sample.","tags":["markets","sp500"],"start_date":"2024-01-01","end_date":"2024-12-31"}],"drop_dataset_keys":[],"refined_tags":[],"refined_start_date":null,"refined_end_date":null}',
            },
            default='{"action":"final","final_answer":"[stubbed] no-op","insights_to_store":[],"drop_dataset_keys":[],"refined_tags":[],"refined_start_date":null,"refined_end_date":null}',
        )

    initial = InitialPromptStage(question=question, backend=backend)
    query = QueryPriorInsightsStage(backend=backend, insights_store=insights_store)
    market = DataLookupMarketStage(backend=backend)
    real_estate = DataLookupRealEstateStage(backend=backend)
    sec = DataLookupSECStage(backend=backend)
    compile_stage = CompileDataStage(backend=backend, insights_store=insights_store)
    final = FinalResponseStage()

    machine = StateMachine(
        execution_id=execution_id,
        snapshot_store=snapshot_store,
        stage_registry_version=STAGE_REGISTRY_VERSION,
        max_parallelism=3,
        max_attempts_per_run_id=3,
        max_transitions=200,
        max_runs_per_edge={
            (STAGE_COMPILE_DATA, STAGE_QUERY_PRIOR_INSIGHTS): 5,
            (STAGE_QUERY_PRIOR_INSIGHTS, STAGE_COMPILE_DATA): 10,
        },
    )
    machine.register_stage(STAGE_INITIAL_PROMPT, initial, is_start=True)
    machine.register_stage(STAGE_QUERY_PRIOR_INSIGHTS, query)
    machine.register_stage(STAGE_DATA_LOOKUP_MARKET, market)
    machine.register_stage(STAGE_DATA_LOOKUP_REAL_ESTATE, real_estate)
    machine.register_stage(STAGE_DATA_LOOKUP_SEC, sec)
    machine.register_stage(STAGE_COMPILE_DATA, compile_stage)
    machine.register_stage(STAGE_FINAL_RESPONSE, final, is_terminal=True)
    machine.finalize()

    return BuiltAgent(machine=machine, snapshot_store=snapshot_store, insights_store=insights_store)


def load_question_from_snapshot(*, db_path: Path, execution_id: str = EXECUTION_ID_DEFAULT) -> Optional[str]:
    store = SqliteSnapshotStore(db_path=db_path)
    store.open()
    try:
        snap = store.load_snapshot(execution_id)
        if snap is None:
            return None
        # user_context_json is a JSON object, but we only need `question`.
        import json as _json

        ctx = _json.loads(snap.user_context_json or "{}")
        q = ctx.get("question")
        return str(q) if isinstance(q, str) and q.strip() else None
    finally:
        store.close()
