#!/usr/bin/env python
"""Simple CLI for driving the planner-based agent."""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Iterable, Sequence

from profit.agent import AgentRunner, AgentRunnerConfig, ChatGPTLLM, Question, StubLLM


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


def _load_stub_responses(path: Path | None) -> dict[str, Path]:
    if not path:
        return {}
    index_path = path / "index.json" if path.is_dir() else path
    if not index_path.exists():
        raise FileNotFoundError(f"stub index not found: {index_path}")
    try:
        data = json.loads(index_path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise RuntimeError(f"failed to load stub index from {index_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"stub index {index_path} must contain a JSON object")
    base = index_path.parent
    results: dict[str, Path] = {}
    for key, rel_path in data.items():
        if not isinstance(rel_path, str):
            raise RuntimeError(f"stub response reference for {key!r} must be a string path")
        response_path = Path(rel_path)
        if not response_path.is_absolute():
            response_path = base / response_path
        if not response_path.exists():
            raise FileNotFoundError(f"stub response file for {key!r} not found: {response_path}")
        results[key] = response_path
    return results


def _sanitize_question(question: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", question.lower()).strip("_")
    return (cleaned[:40] or "stub")


def _find_stub_key(question: str, available: Iterable[str]) -> str | None:
    if question in available:
        return question
    sanitized = _sanitize_question(question)
    if sanitized in available:
        return sanitized
    candidate = f"{sanitized}_stub"
    if candidate in available:
        return candidate
    return None


def _write_runtime_stub(question: str, response: str, *, key: str | None = None) -> Path:
    stub_key = key or f"{_sanitize_question(question)}_stub"
    stub_payload = {stub_key: response}
    filename = f"ask_agent_stub_{int(time.time() * 1000)}.json"
    path = Path("/tmp") / filename
    path.write_text(json.dumps(stub_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def main(argv: Sequence[str] | None = None) -> int:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Run the planner-based agent.")
    parser.add_argument("question", help="Natural language question you want answered.")
    parser.add_argument("--hint", action="append", help="Supplementary hint (ticker, region, etc.).", default=[])
    parser.add_argument("--model", default="gpt-5-nano", help="LLM model name (defaults to gpt-5-nano).")
    parser.add_argument("--live", action="store_true", help="Use the live ChatGPTLLM instead of the stub.")
    parser.add_argument("--planner", type=Path, default=Path("profit/agent/prompts/planner.md"), help="Path to the planner prompt stub.")
    parser.add_argument("--instructions", type=Path, help="Additional instructions to append to the prompt.")
    parser.add_argument("--data", type=Path, help="Optional DATA block to include (raw text).")
    parser.add_argument(
        "--stub-response", action="append", metavar="KEY=RESPONSE",
        help="Keyword-based stub response (can be repeated).",
    )
    parser.add_argument(
        "--stub-file",
        type=Path,
        help="Path to an index.json (or its parent dir) that maps keywords to response files.",
    )
    args = parser.parse_args(argv)

    question = Question(
        text=args.question,
        hints=[hint for hint in args.hint if hint],
    )

    file_stubs = _load_stub_responses(args.stub_file)
    stub_key: str | None = None
    if args.stub_file:
        stub_key = _find_stub_key(question.text, file_stubs.keys())
        if stub_key is None:
            raise RuntimeError(
                f"stub file {args.stub_file} missing a response for '{question.text}'"
            )
    if args.live:
        llm = ChatGPTLLM(model=args.model)
    else:
        llm = FileStubLLM(model=args.model, file_map=file_stubs) if file_stubs else StubLLM(model=args.model)
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
    stub_path = _write_runtime_stub(question.text, answer.text, key=stub_key)
    logging.info("wrote runtime stub responses to %s", stub_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
