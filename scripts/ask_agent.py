#!/usr/bin/env python
"""Simple CLI for driving the planner-based agent."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

from profit.agent import AgentRunner, AgentRunnerConfig, ChatGPTLLM, Question, StubLLM


def _read_text(path: Path | None) -> str | None:
    if not path:
        return None
    return path.read_text(encoding="utf-8").strip()


def _apply_stub_responses(llm: StubLLM, pairs: Sequence[str] | None) -> None:
    if not pairs:
        return
    for pair in pairs:
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        llm.set_response(key.strip(), value.strip())


def main(argv: Sequence[str] | None = None) -> int:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Run the planner-based agent.")
    parser.add_argument("question", help="Natural language question you want answered.")
    parser.add_argument("--hint", action="append", help="Supplementary hint (ticker, region, etc.).", default=[])
    parser.add_argument("--model", default="gpt-5-nano", help="LLM model name (defaults to gpt-5-nano).")
    parser.add_argument("--live", action="store_true", help="Use the live ChatGPTLLM instead of the stub.")
    parser.add_argument("--planner", type=Path, default=Path("planner.md"), help="Path to the planner prompt stub.")
    parser.add_argument("--instructions", type=Path, help="Additional instructions to append to the prompt.")
    parser.add_argument("--data", type=Path, help="Optional DATA block to include (raw text).")
    parser.add_argument(
        "--stub-response", action="append", metavar="KEY=RESPONSE",
        help="Keyword-based stub response (can be repeated).",
    )
    args = parser.parse_args(argv)

    question = Question(
        text=args.question,
        hints=[hint for hint in args.hint if hint],
    )

    llm = ChatGPTLLM(model=args.model) if args.live else StubLLM(model=args.model)
    if isinstance(llm, StubLLM):
        _apply_stub_responses(llm, args.stub_response)

    runner_config = AgentRunnerConfig(planner_path=args.planner)
    runner = AgentRunner(llm, config=runner_config)
    answer = runner.run(
        question=question,
        extra_instructions=_read_text(args.instructions),
        extra_data_block=_read_text(args.data),
    )
    print(answer.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
