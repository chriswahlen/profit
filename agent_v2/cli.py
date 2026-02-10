from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENTAPI = ROOT / "libs" / "agentapi"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(AGENTAPI) not in sys.path:
    sys.path.insert(0, str(AGENTAPI))

from agentapi.history_entry import HistoryEntry  # noqa: E402

from agent_v2.constants import EXECUTION_ID_DEFAULT  # noqa: E402
from agent_v2.machine import build_agent, load_question_from_snapshot  # noqa: E402


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="agent_v2")
    parser.add_argument(
        "question",
        nargs="*",
        help="User question for the agent (omit with --retry to reuse previous question).",
    )
    parser.add_argument("--live", action="store_true", help="Use the real OpenAI API (requires credentials).")
    parser.add_argument("--retry", action="store_true", help="Retry the last failed step from the previous run.")
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
        help="Minimum logging level emitted during the run.",
    )
    return parser.parse_args(argv)


def _reset_last_failed_run(*, built, execution_id: str) -> None:
    """
    If the previous run ended in a terminal failed state, reset the last failed run_id so
    it can be retried.
    """

    snap = built.snapshot_store.load_snapshot(execution_id)
    if snap is None or snap.terminal_state != "failed":
        return

    history = built.snapshot_store.load_history(execution_id, after_cursor=0)
    last_failed: HistoryEntry | None = None
    for _, entry in history:
        if entry.status == "failed":
            last_failed = entry
    if last_failed is None:
        return

    built.machine.reset_retry_count(last_failed.run_id)


def main(argv: list[str] | None = None) -> int:
    ns = _parse_args(list(argv) if argv is not None else sys.argv[1:])

    db_path = Path("agent_v2.sqlite")
    execution_id = EXECUTION_ID_DEFAULT

    logging.basicConfig(level=getattr(logging, ns.log_level, logging.INFO))

    question = " ".join(ns.question).strip()
    if not question and ns.retry:
        question = load_question_from_snapshot(db_path=db_path, execution_id=execution_id) or ""

    if not question:
        raise SystemExit("Must supply a question (or use --retry with an existing sqlite state).")

    built = build_agent(question=question, db_path=db_path, execution_id=execution_id, live=ns.live)
    try:
        if ns.retry:
            _reset_last_failed_run(built=built, execution_id=execution_id)

        while True:
            ready = built.machine.poll_ready()
            if ready.is_done:
                break
            built.machine.execute_ready_batch()

        final_answer = built.machine.user_context.get("final_answer", "")
        if isinstance(final_answer, str) and final_answer.strip():
            print(final_answer)
        else:
            print("[agent_v2] completed but no final_answer in user_context", file=sys.stderr)
            return 2
        return 0
    finally:
        built.insights_store.close()
        built.snapshot_store.close()


if __name__ == "__main__":
    raise SystemExit(main())
