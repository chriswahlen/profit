from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from agentapi.errors import CachedRetryable
from agentapi.plan import Run
from agentapi.runners import AgentTransformRunner
from agentapi.plan import Plan

logger = logging.getLogger(__name__)

# Stage names live here so the prompt + parser can refer to them consistently.
STAGE_INITIAL_PROMPT = "financial_adviser.initial_prompt"

PROMPT = """
You are an agent that answers user questions by planning and executing a DAG of data queries.
You must not mention database schema/field names in planning; use high-level descriptions.
You must always: (1) list available data sources, (2) produce `planning_instructions`, (3) produce a PlanDAG, (4) define a ConclusionPolicy.
Execution is two-phase per data node: Phase 1 produces query specs; Phase 2 interprets results and may request follow-ups.
Track intents in an IntentLedger; each intent must end as resolved, partially_resolved, or blocked.

AVAILABLE DATA SOURCES (authoritative; do not invent additional sources or data types):
"market": Daily stock, commodity, crypto, and Forex data OHLCV and adjusted_close price.
"edgar": Edgar SEC facts extracted from XBLR; text blobs also available.
"redfin": Real estate market by metro, zipcode, or neighborhood; number of listings, avg. sale price.

TASK:
1) Build planning_instructions: a numbered list of high-level instructions for how you will answer the question. Do not reference DB fields or schema.
2) Build an intent_ledger: decompose the question into 1..N intents that must be resolved to answer.
3) Build a plan_dag: nodes must be per data source/query bundle and carry an intent. Include dependencies, expected outputs, and follow-up policies. Keep the DAG minimal.
4) Define a conclusion_policy: rules for when to answer vs ask for more data and how to store conclusions.

OUTPUT FORMAT (JSON only):

# Outer JSON:
{
  "planning_instructions": {...},
  "intent_ledger": {...},
  "plan_dag": {...},
  "conclusion_policy": {...},
}

# planning_instructions:
// Rules:
// Must be domain-level only
// No field names
// No query details
// Must explain why selected sources are sufficient
{
  "items": [{
      "id": "pi_<n>",
      "instruction": "string",
      "purpose": "string"
    }, { ... }],
  "stopping_criteria": [ "string" ],
  "assumptions": [ "string" ],
  "known_limitations": [ "string" ]
}

# intent_ledger
// Rules:
// Every data node must reference an intent
// least one critical intent must exist
// DAG completion depends on critical intents only
{
  "intents": [{
      "intent_id": "intent_<n>",
      "statement": "string",
      "priority": "critical|supporting",
      "status": "unresolved",
      "required_sources": ["stocks_daily|redfin_sales|sec_facts"],
      "resolution_criteria": ["string"],
      "evidence_refs": [],
      "dependencies": ["intent_<m>"]
    }
  ]
}

# plan_dag:
{
  "dag_id": "string",
  "nodes": [{
      "node_id": "node_<n>",
      "node_type": "data|synthesis|internal",
      "source": "stocks_daily|redfin_sales|sec_facts|internal|synthesis",
      "intent_ids": ["intent_<n>"],
      "intent": "string",
      "depends_on": ["node_<m>"],
      "query_scope": {
        "description": "high-level description of what will be queried",
        "time_policy": "requested_only|requested_plus_context|adaptive",
        "entity_source": "query_spec|upstream_node"
      },
      "expected_outputs": [{
          "artifact_id": "string",
          "description": "string",
          "type": "timeseries|table|event_list|derived_metrics"
        }
      ],
      "interpretation_goal": "string",
      "followup_policy": {
        "if_empty": "request_user_input|widen_timeframe|mark_blocked",
        "if_inconclusive": "spawn_node|refine_node|proceed_with_caveat",
        "stop_condition": "string"
      }
    }
  ],
  "edges": [{
      "from": "node_<n>",
      "to": "node_<m>"
    }]
}

# conclusion_policy
{
  "completion_rule": {
    "critical_intents_must_be": "resolved|resolved_or_partially_resolved"
  },
  "answer_conditions": ["string"],
  "request_more_data_conditions": ["string"],
  "storage_policy": {
    "store_intent_ledger": true,
    "store_conclusions": true,
    "store_assumptions": true
  }
}
"""


class InitialPromptStage(AgentTransformRunner):
    """
    LLM initial planning stage.
    """

    def __init__(
        self,
        *,
        backend,
        model: str | None = None,
        db_query_stage_name: str,
        final_stage_name: str,
    ) -> None:
        super().__init__(name=STAGE_INITIAL_PROMPT, backend=backend, model=model)
        self._db_query_stage_name = db_query_stage_name
        self._final_stage_name = final_stage_name

    def get_prompt(self, *, previous_history_entries: list[Any], user_context: dict[str, Any]) -> str:
        fa = user_context.get("financial_adviser")
        fa_dict = fa if isinstance(fa, dict) else {}
        question = fa_dict.get("question")
        if not isinstance(question, str) or not question.strip():
            raise ValueError("user_context.financial_adviser.question is required")

        # Keep prompt explicit and machine-readable to make it easy to stub in tests.
        return "\n".join(
            [
                PROMPT,
                "",
                f"USER_QUESTION: {question.strip()}",
            ]
        )

    def process_prompt(
        self,
        *,
        result: str,
        previous_history_entries: list[Any],
        user_context: dict[str, Any],
    ) -> Plan:
        # TODO: Implement