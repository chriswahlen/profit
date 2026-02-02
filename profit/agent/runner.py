from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from profit.agent.llm import BaseLLM, LLMResponse
from profit.agent.types import Question, SnippetSummary


@dataclass
class AgentRunnerConfig:
    planner_path: Path
    iteration_limit: int = 5


class AgentRunner:
    def __init__(self, llm: BaseLLM, config: AgentRunnerConfig | None = None) -> None:
        self.llm = llm
        self.config = config or AgentRunnerConfig(planner_path=Path("planner.md"))
        self._planner_text: str | None = None

    def run(
        self,
        *,
        question: Question,
        snippets: Iterable[SnippetSummary] | None = None,
        extra_data_block: str | None = None,
        extra_instructions: str | None = None,
    ) -> LLMResponse:
        prompt = self._build_prompt(
            question=question,
            snippets=snippets,
            extra_data_block=extra_data_block,
            extra_instructions=extra_instructions,
        )
        return self.llm.generate(question=question, plan=None, data=extra_data_block, prompt=prompt)

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
