#!/usr/bin/env python3
"""CLI to run the Agent V2 state machine with either stub or live LLM backends."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

# Ensure repo root is on sys.path for local execution.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from profit.agent import ChatGPTLLM, StubLLM  # type: ignore
from profit.agent_v2 import AgentV2Runner, AgentV2RunnerConfig


def _configure_logging(level_name: str | None) -> None:
    if logging.getLogger().hasHandlers():
        return
    env = os.getenv("PROFIT_AGENT_LOG_LEVEL", "").upper()
    level_key = (level_name or env or "INFO").upper()
    level = getattr(logging, level_key, logging.INFO)
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def _parse_stub_pairs(pairs: list[str] | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for pair in pairs or []:
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        mapping[key.strip()] = value.strip()
    return mapping


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Agent V2 state machine.")
    parser.add_argument("question", help="Natural language question to answer.")
    parser.add_argument("--hint", action="append", default=[], help="Optional hint (ticker, region, etc.).")
    parser.add_argument("--model", default="gpt-5-nano", help="LLM model name (live or stub).")
    parser.add_argument("--live", action="store_true", help="Use the live ChatGPTLLM backend.")
    parser.add_argument("--planner", type=Path, default=Path("profit/agent_v2/prompts/planner.md"))
    parser.add_argument("--compiler", type=Path, default=Path("profit/agent_v2/prompts/compiler.md"))
    parser.add_argument("--final-prompt", type=Path, default=Path("profit/agent_v2/prompts/final_response.md"))
    parser.add_argument("--execution-id", default="agent_v2", help="Execution ID for snapshotting (defaults to agent_v2).")
    parser.add_argument("--snapshot", type=Path, help="Override snapshot sqlite path (defaults to /tmp/agent_v2_state_<execution_id>.sqlite).")
    parser.add_argument("--reset-state", action="store_true", help="Delete snapshot file before running (fresh state).")
    parser.add_argument(
        "--stub-response",
        action="append",
        metavar="KEY=RESPONSE",
        help="Keyword-based stub response (can be repeated).",
    )
    parser.add_argument("--retry", action="store_true", help="Reset retry budget for a specific run (defaults to root run).")
    parser.add_argument("--retry-cacheable", action="store_true", help="Reset retry budget for a run that previously returned CachedRetryable.")
    parser.add_argument("--retry-run-id", help="Run ID to reset; defaults to the root run in the snapshot.")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Override the log level (overrides PROFIT_AGENT_LOG_LEVEL).",
    )
    args = parser.parse_args(argv)

    _configure_logging(args.log_level)

    if args.live:
        backend = ChatGPTLLM(model=args.model)
    else:
        stub_pairs = _parse_stub_pairs(args.stub_response)
        backend = StubLLM(key_responses=stub_pairs, default="[stub] no matching key")

    runner = AgentV2Runner(
        backend,
        config=AgentV2RunnerConfig(
            planner_path=args.planner,
            compiler_path=args.compiler,
            final_prompt_path=args.final_prompt,
            execution_id=args.execution_id,
            snapshot_path=args.snapshot,
        ),
    )
    answer = runner.run(
        question=args.question,
        hints=[h for h in args.hint if h],
        retry_run_id=args.retry_run_id if (args.retry or args.retry_cacheable) else None,
        reset_retry_cached=args.retry_cacheable,
        reset_retry=args.retry,
        reset_state=args.reset_state,
    )
    print(answer.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
