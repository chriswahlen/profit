from __future__ import annotations

import json
from pathlib import Path

from agentapi.plan import Run

from profit.agent_v2.runners.common import ContextualAgentRunner
from profit.agent_v2.models import Step1Result

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
        print("PROMPT: %s" % prompt)
        return prompt.strip()

    def process_prompt(self, *, result: str, previous_history_entries):
        print("RESULT:\n--------------------%s\n" % result)
        return Run(stage_name="query_prior_insights")
