from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from profit.agent.llm import BaseLLM, LLMResponse
from profit.agent.types import Question, SnippetSummary
from profit.agent_v2.data_formatter import format_data_block
from profit.agent_v2.exceptions import AgentV2Error, AgentV2ValidationError
from profit.agent_v2.insights import InsightLookup, InsightsManager
from profit.agent_v2.models import EdgarAnchor, RetrievalPlan, Step1Payload
from profit.agent_v2.retrievers import MarketRetrieverV2, SqlRetrieverV2
from profit.agent_v2.validation import parse_step1, parse_step2
from profit.catalog.entity_store import EntityStore
from profit.catalog.identifier_utils import resolve_cik_from_identifier

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class AgentV2RunnerConfig:
    planner_path: Path = Path("profit/agent_v2/prompts/planner.md")
    compiler_path: Path = Path("profit/agent_v2/prompts/compiler.md")
    iteration_limit: int = 5
    entity_db_path: Path = Path("data/profit.sqlite")


class AgentV2Runner:
    def __init__(
        self,
        llm: BaseLLM,
        config: AgentV2RunnerConfig | None = None,
        *,
        market: MarketRetrieverV2 | None = None,
        sql: SqlRetrieverV2 | None = None,
        insights: InsightsManager | None = None,
    ) -> None:
        self.llm = llm
        self.config = config or AgentV2RunnerConfig()
        self._planner_text: str | None = None
        self._compiler_text: str | None = None
        self.market = market or MarketRetrieverV2()
        self.sql = sql or SqlRetrieverV2()
        self.insights = insights or InsightsManager()

    def run(
        self,
        *,
        question: Question,
        snippets: Iterable[SnippetSummary] | None = None,
        extra_instructions: str | None = None,
    ) -> LLMResponse:
        iteration = 0
        data_payloads: list[dict[str, Any]] = []
        current_snippets = list(snippets or [])
        last_step1: Step1Payload | None = None

        while iteration < self.config.iteration_limit:
            planner_prompt = self._build_planner_prompt(
                question=question,
                snippets=current_snippets,
                data_payloads=data_payloads,
                extra_instructions=extra_instructions,
            )
            planner_response = self.llm.generate(question=question, plan=None, data=None, prompt=planner_prompt)
            step1 = parse_step1(planner_response.text)
            last_step1 = step1

            if step1.insights_writeback:
                stored = self.insights.store(step1.insights_writeback)
                logger.info("agent_v2 stored %d insights", len(stored))

            if step1.stop_reason == "need_clarification":
                return LLMResponse(
                    text="\n".join(step1.clarifying_questions),
                    metadata={"stop_reason": step1.stop_reason, "iteration": iteration + 1},
                )
            if step1.stop_reason == "answered" or step1.can_answer_now:
                return LLMResponse(
                    text=step1.final_answer or "",
                    metadata={"stop_reason": step1.stop_reason, "iteration": iteration + 1},
                )

            if not step1.needs_data:
                raise AgentV2Error("agent_v2 indicated no data needed but did not provide final_answer")
            if not step1.anchors:
                raise AgentV2Error("agent_v2 requested more data but provided no anchors")

            cik_map = self._resolve_ciks(step1)

            compiler_prompt = self._build_compiler_prompt(step1=step1, cik_map=cik_map)
            compiler_response = self.llm.generate(question=question, plan=None, data=None, prompt=compiler_prompt)
            plan = parse_step2(compiler_response.text)
            plan = self._attach_entity_resolution(plan, step1=step1, cik_map=cik_map)

            data_payloads = self._execute_plan(plan)

            current_snippets = self._lookup_insights(step1)

            iteration += 1

        raise AgentV2Error(self._iteration_limit_message(last_step1))

    def _build_planner_prompt(
        self,
        *,
        question: Question,
        snippets: list[SnippetSummary],
        data_payloads: list[dict[str, Any]],
        extra_instructions: str | None,
    ) -> str:
        parts: list[str] = [self._load_planner()]
        if extra_instructions:
            parts.append(f"Additional instructions:\n{extra_instructions}")
        if snippets:
            parts.append("INSIGHTS\n" + "\n\n".join(s.format() for s in snippets))
        if data_payloads:
            parts.append(format_data_block(data_payloads))
        q_lines = [f"USER_QUESTION\n{question.text}"]
        if question.hints:
            q_lines.append("HINTS\n" + "\n".join(question.hints))
        parts.append("\n\n".join(q_lines))
        return "\n\n".join(parts)

    def _build_compiler_prompt(self, *, step1: Step1Payload, cik_map: dict[str, str]) -> str:
        parts: list[str] = [self._load_compiler()]
        if cik_map:
            parts.append("ENTITY_RESOLUTION\n" + json.dumps(cik_map, indent=2, sort_keys=True))
        parts.append("STEP1_JSON\n" + json.dumps(step1.model_dump(), indent=2, sort_keys=True))
        return "\n\n".join(parts)

    def _load_planner(self) -> str:
        if self._planner_text is None:
            self._planner_text = self.config.planner_path.read_text(encoding="utf-8")
        return self._planner_text

    def _load_compiler(self) -> str:
        if self._compiler_text is None:
            self._compiler_text = self.config.compiler_path.read_text(encoding="utf-8")
        return self._compiler_text

    def _resolve_ciks(self, step1: Step1Payload) -> dict[str, str]:
        edgar_anchors: list[EdgarAnchor] = [a for a in step1.anchors if isinstance(a, EdgarAnchor)]
        if not edgar_anchors:
            return {}
        if not self.config.entity_db_path.exists():
            raise AgentV2Error(f"entity db not found: {self.config.entity_db_path}")
        store = EntityStore(self.config.entity_db_path, readonly=True)
        try:
            mapping: dict[str, str] = {}
            for anchor in edgar_anchors:
                identifier = f"{anchor.entity.exchange_mic}|{anchor.entity.ticker}"
                cik = resolve_cik_from_identifier(store, identifier)
                if not cik:
                    raise AgentV2Error(f"failed to resolve CIK for {identifier} (anchor_id={anchor.id})")
                mapping[anchor.id] = cik
            return mapping
        finally:
            store.close()

    def _attach_entity_resolution(self, plan: RetrievalPlan, *, step1: Step1Payload, cik_map: dict[str, str]) -> RetrievalPlan:
        if not cik_map:
            return plan
        report = list(plan.entity_resolution_report)
        existing = {r.anchor_id for r in report}
        entity_by_anchor_id: dict[str, dict[str, str]] = {}
        for anchor in step1.anchors:
            if isinstance(anchor, EdgarAnchor):
                entity_by_anchor_id[anchor.id] = anchor.entity.model_dump()

        for anchor_id, cik in cik_map.items():
            if anchor_id in existing:
                continue
            entity = entity_by_anchor_id.get(anchor_id)
            if not entity:
                continue
            report.append(
                {
                    "anchor_id": anchor_id,
                    "entity": entity,
                    "status": "ok",
                    "resolved": {"cik": cik},
                    "note": "resolved by runtime",
                }
            )

        return RetrievalPlan.model_validate({**plan.model_dump(), "entity_resolution_report": report})

    def _execute_plan(self, plan: RetrievalPlan) -> list[dict[str, Any]]:
        # Simple dependency handling: execute in declared order, assuming dependencies refer to earlier batches.
        payloads: list[dict[str, Any]] = []
        for batch in plan.batches:
            for req in batch.requests:
                if req.type == "market_ohlcv":
                    result = self.market.fetch(req)
                    payloads.append(result.payload)
                else:
                    result = self.sql.fetch(req)
                    payloads.append(result.payload)
        return payloads

    def _lookup_insights(self, step1: Step1Payload) -> list[SnippetSummary]:
        lookups = [
            InsightLookup(tags=list(q.tags), freshness_horizon_days=q.freshness_horizon_days)
            for q in step1.insight_ops.search
        ]
        if not lookups:
            return []
        return self.insights.lookup(lookups, limit=5)

    def _iteration_limit_message(self, last_step1: Step1Payload | None) -> str:
        summary = last_step1.context.approach if last_step1 else "unknown"
        return (
            f"agent_v2 hit iteration limit={self.config.iteration_limit} at { _now_iso() }.\n"
            f"last_approach={summary}"
        )
