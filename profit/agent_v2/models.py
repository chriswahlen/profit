from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional


@dataclass
class Request:
    request_id: str
    type: str
    params: dict[str, Any]
    dataset: Optional[str] = None  # for sql


@dataclass
class RetrievalBatch:
    batch_id: str
    purpose: str
    requests: list[Request]
    depends_on_batches: list[str] = field(default_factory=list)



@dataclass
class AgentV2RunnerConfig:
    planner_path: Path
    compiler_path: Path
    final_prompt_path: Path
    execution_id: str = "agent_v2"
    snapshot_path: Path | None = None
    max_steps: int = 64
    run_id: str | None = None


@dataclass(frozen=True)
class InsightWriteback:
    title: str
    body: list[str]
    tags: list[str]
    related_instruments: list[str] | None = None
    related_regions: list[str] | None = None
    expires_at_utc: str | None = None


# Concrete request param containers used by retrievers
@dataclass(frozen=True)
class MarketOhlcvParams:
    ticker: str
    exchange_mic: str
    start_utc: str
    end_utc: str
    bar_size: str
    fields: list[str]
    adjust_splits: bool | None = None
    adjust_dividends: bool | None = None
    post_aggregations: list[dict] | None = None


@dataclass(frozen=True)
class MarketOhlcvRequest:
    request_id: str
    type: str
    params: MarketOhlcvParams
    timeout_ms: int | None = None


@dataclass(frozen=True)
class EdgarParams:
    cik: str
    start_utc: str
    end_utc: str
    period_type: str
    concept_aliases: list[str]
    limit: int


@dataclass(frozen=True)
class EdgarRequest:
    request_id: str
    type: str
    params: EdgarParams
    timeout_ms: int | None = None


@dataclass(frozen=True)
class RealEstateParams:
    geo_id: str
    start_utc: str
    end_utc: str
    measures: list[str]
    aggregation: list[str]


@dataclass(frozen=True)
class RealEstateRequest:
    request_id: str
    type: str
    params: RealEstateParams
    timeout_ms: int | None = None


@dataclass(frozen=True)
class SqlParams:
    sql: str
    dialect: str
    max_rows: int
    timeout_ms: int
    concept_aliases: list[str] | None = None


@dataclass(frozen=True)
class SqlRequest:
    request_id: str
    type: str
    params: SqlParams
    dataset: str
