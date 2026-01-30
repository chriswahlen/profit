"""
Corporate fundamentals (EDGAR, provider-neutral scaffolding).

This package currently exposes schema helpers and query utilities for SEC filings.
"""

from profit.sources.fundamentals.schemas import ensure_sec_fundamentals_schemas
from profit.sources.fundamentals.query import read_asof
from profit.sources.fundamentals.writer import write_filings, write_facts

__all__ = [
    "ensure_sec_fundamentals_schemas",
    "read_asof",
    "write_filings",
    "write_facts",
]
