#!/usr/bin/env python3
"""Simple CLI for driving the v2 planner/compiler agent."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Iterable, Sequence

from profit.agent import ChatGPTLLM, Question, StubLLM
from profit.agent_v2 import AgentV2Runner, AgentV2RunnerConfig


class FileStubLLM(StubLLM):
    def __init__(self, *, file_map: dict[str, Path] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._remaining = dict(file_map or {})

    def _send(self, prompt: str) -> str:
        lowered = prompt.lower()
        for keyword in list(self._remaining):
            if keyword.lower() in lowered:
                path = self._remaining.pop(keyword)
                return path.read_text(encoding="utf-8")
        return super()._send(prompt)


def _configure_logging(level_name: str | None) -> None:
    if logging.getLogger().hasHandlers():
        return
    env = os.getenv("PROFIT_AGENT_LOG_LEVEL", "").upper()
    level_key = (level_name or env or "INFO").upper()
    level = getattr(logging, level_key, logging.INFO)
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def _read_text(path: Path | None) -> str | None:
    if not path:
        return None
    return path.read_text(encoding="utf-8").strip()


def _sanitize_question(question: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", question.lower()).strip("_")
    return (cleaned[:40] or "stub")


def _write_runtime_stub(question: str, response: str, *, key: str | None = None) -> Path:
    stub_key = key or f"{_sanitize_question(question)}_stub"
    stub_payload = {stub_key: response}
    filename = f"ask_agent_v2_stub_{int(time.time() * 1000)}.json"
    path = Path("/tmp") / filename
    path.write_text(json.dumps(stub_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the v2 planner/compiler agent.")
    parser.add_argument("question", help="Natural language question you want answered.")
    parser.add_argument("--hint", action="append", help="Supplementary hint (ticker, region, etc.).", default=[])
    parser.add_argument("--model", default="gpt-5-nano", help="LLM model name (defaults to gpt-5-nano).")
    parser.add_argument("--live", action="store_true", help="Use the live ChatGPTLLM instead of the stub.")
    parser.add_argument("--planner", type=Path, default=Path("profit/agent_v2/prompts/planner.md"))
    parser.add_argument("--compiler", type=Path, default=Path("profit/agent_v2/prompts/compiler.md"))
    parser.add_argument("--instructions", type=Path, help="Additional instructions to append to Step 1 prompt.")
    parser.add_argument(
        "--stub-response",
        action="append",
        metavar="KEY=RESPONSE",
        help="Keyword-based stub response (can be repeated).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Override the log level (overrides PROFIT_AGENT_LOG_LEVEL).",
    )
    args = parser.parse_args(argv)

    _configure_logging(args.log_level)

    question = Question(text=args.question, hints=[hint for hint in args.hint if hint])

    llm = ChatGPTLLM(model=args.model) if args.live else StubLLM(model=args.model)
    if isinstance(llm, StubLLM) and args.stub_response:
        for pair in args.stub_response:
            if "=" not in pair:
                continue
            key, value = pair.split("=", 1)
            llm.set_response(key.strip(), value.strip())

    runner = AgentV2Runner(
        llm,
        config=AgentV2RunnerConfig(
            planner_path=args.planner,
            compiler_path=args.compiler,
            final_prompt_path=Path("profit/agent_v2/prompts/final_response.md"),
        ),
    )
    answer = runner.run(question=question, extra_instructions=_read_text(args.instructions))
    stub_path = _write_runtime_stub(question.text, answer.text)
    logging.info("wrote runtime stub responses to %s", stub_path)
    print(answer.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
