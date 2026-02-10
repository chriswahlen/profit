from __future__ import annotations

import json

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.exceptions import AgentV2ValidationError

PROMPT = '''
You are a financial planning and market research expert.

As an expert, you will develop key insights in order to answer the user's question. These key
insights will be derived from market data, real estate data, and SEC/EDGAR filings. First, we will
look for any previous key insights we know of that are useful to answer the user's query.

Key insights are keyed by tags and optional date ranges. Return the key insights you would like
to query for as a JSON blob.

Produce ONLY a JSON object matching this shape:
{
  "approach": "<how you plan to answer>",
  "insights": [
    { start_date: "2024-01-01", "end_date": "2024-12-31", tags: ["tag1", "tag2"] }
    { start_date: "2024-01-01", "end_date": "2024-12-31", tags: ["tag1", "tag2"] }
  }
}

User Query:
{{question}}
{{hints}}
'''

class InitialPromptRunner(ContextualAgentRunner):
    """Stage: run planner prompt, emit query_prior_insights."""

    def __init__(self, *, backend, question: str, hints: list[str], extra_instructions: str | None):
        self.question = question
        self.hints = hints
        self.extra_instructions = extra_instructions
        super().__init__(name="initial_prompt", backend=backend)

    def get_prompt(self, *, previous_history_entries):
        template = PROMPT
        hint_block = "\n".join(f"- {h}" for h in self.hints if h)
        extra = f"\nAdditional instructions:\n{self.extra_instructions}" if self.extra_instructions else ""
        prompt = template.replace("{{question}}", self.question)
        prompt = prompt.replace("{{hints}}", hint_block)
        prompt = prompt + extra
        print("[InitialPrompt] PROMPT\n%s\n" % prompt)
        return prompt.strip()

    def process_prompt(self, *, result: str, previous_history_entries):
        print("[InitialPrompt] RESPONSE\n%s\n" % result)
        payload = self._validate_result(result)
        # Persist the validated output in the run metadata so downstream stages can reuse it.
        self.set_meta(user_context=payload, question=self.question)
        return Run(stage_name="query_prior_insights")

    def _validate_result(self, raw: str) -> dict:
        """Validate and normalize the LLM JSON response defined in PROMPT."""

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:  # pragma: no cover - trivial
            raise AgentV2ValidationError("initial_prompt must return JSON matching the documented schema") from exc

        if not isinstance(payload, dict):
            raise AgentV2ValidationError("initial_prompt must return a JSON object")

        approach = payload.get("approach")
        if not isinstance(approach, str) or not approach.strip():
            raise AgentV2ValidationError("approach must be a non-empty string")

        insights = payload.get("insights", [])
        if not isinstance(insights, list):
            raise AgentV2ValidationError("insights must be a list")

        normalized_insights = []
        for idx, insight in enumerate(insights):
            if not isinstance(insight, dict):
                raise AgentV2ValidationError(f"insights[{idx}] must be an object")

            tags = insight.get("tags")
            if not isinstance(tags, list) or not tags or not all(isinstance(t, str) and t.strip() for t in tags):
                raise AgentV2ValidationError(f"insights[{idx}].tags must be a non-empty list of strings")

            start_date = insight.get("start_date")
            end_date = insight.get("end_date")
            for field_name, value in ("start_date", start_date), ("end_date", end_date):
                if value is not None and (not isinstance(value, str) or not value.strip()):
                    raise AgentV2ValidationError(
                        f"insights[{idx}].{field_name} must be a non-empty string when provided"
                    )

            normalized_insights.append(
                {
                    "tags": [t.strip() for t in tags],
                    "start_date": start_date.strip() if isinstance(start_date, str) else None,
                    "end_date": end_date.strip() if isinstance(end_date, str) else None,
                }
            )

        return {"approach": approach.strip(), "insights": normalized_insights}
