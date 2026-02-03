#!/usr/bin/env python
"""Simple CLI for driving the planner-based agent."""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
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


def _load_stub_responses(path: Path | None) -> dict[str, str]:
    if not path:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise RuntimeError(f"failed to load stub responses from {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"stub responses file {path} must contain a JSON object")
    results: dict[str, str] = {}
    for key, value in data.items():
        if not isinstance(value, str):
            raise RuntimeError(f"stub response for {key!r} must be a string")
        results[key] = value
    return results


def _make_stub_key(question: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", question.lower()).strip("_")
    return (cleaned[:40] or "stub") + "_stub"


def _write_runtime_stub(question: str, response: str, *, key: str | None = None) -> Path:
    stub_key = key or _make_stub_key(question)
    timestamp = int(time.time() * 1000)
    stub_dir = Path("/tmp") / f"ask_agent_stub_{timestamp}"
    responses_dir = stub_dir / "responses"
    responses_dir.mkdir(parents=True, exist_ok=True)

    response_file = responses_dir / f"{stub_key}.txt"
    response_file.write_text(response, encoding="utf-8")

    index = {stub_key: str(response_file.relative_to(stub_dir))}
    index_path = stub_dir / "index.json"
    index_path.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    return index_path


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
    parser.add_argument(
        "--stub-file",
        type=Path,
        help="JSON file mapping keywords to stub responses.",
    )
    args = parser.parse_args(argv)

    question = Question(
        text=args.question,
        hints=[hint for hint in args.hint if hint],
    )

    stub_key = _make_stub_key(question.text)
    file_stubs = _load_stub_responses(args.stub_file)
    if args.stub_file and stub_key not in file_stubs:
        raise RuntimeError(f"stub file {args.stub_file} missing a response for {stub_key!r}")
    llm = ChatGPTLLM(model=args.model) if args.live else StubLLM(model=args.model)
    if isinstance(llm, StubLLM):
        _apply_stub_responses(llm, args.stub_response)
        for key, value in file_stubs.items():
            llm.set_response(key, value)

    runner_config = AgentRunnerConfig(planner_path=args.planner)
    runner = AgentRunner(llm, config=runner_config)
    answer = runner.run(
        question=question,
        extra_instructions=_read_text(args.instructions),
        extra_data_block=_read_text(args.data),
    )
    print(answer.text)
    stub_path = _write_runtime_stub(question.text, answer.text, key=_make_stub_key(question.text))
    logging.info("wrote runtime stub responses to %s", stub_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
