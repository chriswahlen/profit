#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from config import Config
from agents.financial_advisor.skills.edgar_concept_registry import (
    CONCEPT_SEEDS,
    ConceptRegistryBuilder,
)
from data_sources.edgar.edgar_data_store import EdgarDataStore

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dump EDGAR concept registry")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Path to write JSON registry (defaults to stdout)",
    )
    args = parser.parse_args(args=argv)

    cfg = Config()
    store = EdgarDataStore(cfg)
    try:
        builder = ConceptRegistryBuilder(store=store, seeds=CONCEPT_SEEDS)
        registry = builder.build()
        serialized = registry.serialize()
        output = json.dumps(serialized, indent=2)
        if args.output:
            args.output.write_text(output, encoding="utf-8")
            logger.info("Registry written to %s", args.output)
        else:
            print(output)
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
