from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from profit.agent.llm import BaseLLM, LLMResponse
from profit.agent.retrievers import RetrieverRegistry
from profit.agent.snippets import SnippetStore
from profit.agent.types import DataNeed, Question, SnippetSummary
from profit.agent.validation import AgentValidationError, validate_agent_response

logger = logging.getLogger(__name__)


@dataclass
class AgentRunnerConfig:
    planner_path: Path
    iteration_limit: int = 5


class AgentRunner:
    def __init__(
        self,
        llm: BaseLLM,
        config: AgentRunnerConfig | None = None,
        *,
        retriever_registry: RetrieverRegistry | None = None,
        snippet_store: SnippetStore | None = None,
    ) -> None:
        self.llm = llm
        self.config = config or AgentRunnerConfig(planner_path=Path("planner.md"))
        self._planner_text: str | None = None
        self.snippet_store = snippet_store or SnippetStore()
        self.retriever_registry = retriever_registry or RetrieverRegistry(snippet_store=self.snippet_store)

    def run(
        self,
        *,
        question: Question,
        snippets: Iterable[SnippetSummary] | None = None,
        extra_data_block: str | None = None,
        extra_instructions: str | None = None,
    ) -> LLMResponse:
        current_snippets = list(snippets or [])
        context_blocks: list[str] = [extra_data_block] if extra_data_block else []
        iteration = 0
        last_response: LLMResponse | None = None
        last_parsed: dict[str, Any] | None = None
        last_response_text: str | None = None

        while iteration < self.config.iteration_limit:
            data_block = "\n\n".join(block for block in context_blocks if block)
            prompt = self._build_prompt(
                question=question,
                snippets=current_snippets,
                extra_data_block=data_block,
                extra_instructions=extra_instructions,
            )
            logger.debug("prompt payload (#%d): %s", iteration + 1, prompt)
            response = self.llm.generate(question=question, plan=None, data=data_block, prompt=prompt)
            last_response = response

            try:
                parsed = self._parse_json(response.text)
                validate_agent_response(parsed)
            except AgentValidationError as exc:
                snippet = self._make_snippet(response.text)
                logger.warning(
                    "agent validation failed (%s); response snippet=%s",
                    exc,
                    snippet,
                )
                raise

            last_parsed = parsed
            last_response_text = response.text
            self._log_agent_plan(parsed)

            self._log_data_needs(parsed.get("data_needs", []))

            requests = parsed.get("data_request", [])
            if not requests:
                return response

            context_blocks = []
            snippet_hits: list[SnippetSummary] = []
            for entry in requests:
                retriever = self.retriever_registry.get(entry["type"])
                result = retriever.fetch(entry["request"], notes=entry.get("notes"))
                context_blocks.append(json.dumps(result.payload, indent=2))
                snippet_hits.extend(result.snippet_summaries)
                self._log_retriever_data_needs(result.data_needs)

            current_snippets = snippet_hits
            iteration += 1

        raise RuntimeError(self._iteration_limit_message(last_parsed, last_response_text))

    def _build_prompt(
        self,
        *,
        question: Question,
        snippets: Iterable[SnippetSummary] | None,
        extra_data_block: str | None,
        extra_instructions: str | None,
    ) -> str:
        sections: list[str] = [self._planner_text or self._load_planner()]
        if extra_instructions:
            sections.append(f"Instructions:\n{extra_instructions}")

        if snippets:
            sections.append("Research snippets:\n" + "\n\n".join(snippet.format() for snippet in snippets))

        if extra_data_block:
            sections.append(f"DATA:\n{extra_data_block}")

        question_lines = [f"Question:\n{question.text}"]
        if question.hints:
            question_lines.append(f"Hints: {', '.join(question.hints)}")

        sections.append("\n".join(question_lines))
        return "\n\n".join(sections)

    def _load_planner(self) -> str:
        path = self.config.planner_path
        self._planner_text = path.read_text()
        return self._planner_text

    def _parse_json(self, text: str) -> dict:
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise AgentValidationError("agent response is not valid JSON") from exc

    def _log_data_needs(self, needs: Iterable[Mapping[str, Any]] | None) -> None:
        if not needs:
            return
        for need in needs:
            data_need = DataNeed(
                name=need["name"],
                provider=need.get("provider"),
                reason=need.get("reason"),
                criticality=need.get("criticality", "medium"),
            )
            logger.info("agent requested data need: %s", data_need.format())

    def _log_retriever_data_needs(self, needs: Iterable[Any] | None) -> None:
        if not needs:
            return
        for need in needs:
            if isinstance(need, DataNeed):
                formatted = need.format()
            elif isinstance(need, Mapping):
                formatted = DataNeed(
                    name=need.get("name", "unknown"),
                    provider=need.get("provider"),
                    reason=need.get("reason"),
                    criticality=need.get("criticality", "medium"),
                ).format()
            else:
                formatted = str(need)
            logger.info("retriever data need: %s", formatted)

    def _log_agent_plan(self, parsed: dict[str, Any]) -> None:
        agent_response = parsed.get("agent_response", "")
        plans = parsed.get("data_request", [])
        summaries = []
        for entry in plans:
            body = entry.get("request", {})
            summaries.append(f"{entry.get('type')}[{self._describe_request(body)}]")
        plan_summary = ", ".join(summaries) if summaries else "none"
        snippet = self._make_snippet(agent_response)
        logger.info(
            "agent response parsed; agent_response=%s; plan=%s; snippet=%s",
            agent_response.replace("\n", " ")[:200],
            plan_summary,
            snippet,
        )

    @staticmethod
    def _describe_request(body: Mapping[str, Any]) -> str:
        keys = []
        if instruments := body.get("instruments"):
            keys.append(f"instruments={len(instruments)}")
        if regions := body.get("regions"):
            keys.append(f"regions={len(regions)}")
        if companies := body.get("companies"):
            keys.append(f"companies={len(companies)}")
        if action := body.get("action"):
            keys.append(f"action={action}")
        return ";".join(keys) if keys else "details"

    def _iteration_limit_message(self, parsed: dict[str, Any] | None, response_text: str | None) -> str:
        message = "agent iteration limit reached"
        if parsed:
            message += f"; last_agent_response={parsed.get('agent_response')!r}"
        if response_text:
            snippet = self._make_snippet(response_text)
            message += f"; last_response_snippet={snippet}"
        return message

    @staticmethod
    def _make_snippet(text: str, length: int = 200) -> str:
        if len(text) <= length * 2:
            return text
        return f"{text[:length]}...{text[-length:]}"
