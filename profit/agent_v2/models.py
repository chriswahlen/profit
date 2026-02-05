from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, ConfigDict


class V2BaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Context(V2BaseModel):
    user_query: str = Field(min_length=1, max_length=2000)
    approach: str = Field(min_length=1, max_length=2000)


class DateRange(V2BaseModel):
    start_utc: str
    end_utc: str


class EntityMic(V2BaseModel):
    ticker: str = Field(min_length=1, max_length=24)
    exchange_mic: str = Field(pattern=r"^[A-Z0-9]{4}$")


class InsightQuery(V2BaseModel):
    tags: list[str] = Field(min_length=1)
    freshness_horizon_days: int = Field(ge=1, le=3650)
    intent: str = Field(min_length=1, max_length=240)


class InsightStoreCandidate(V2BaseModel):
    proposed_tags: list[str] = Field(min_length=1)
    intent: str = Field(min_length=1, max_length=240)


class InsightOps(V2BaseModel):
    search: list[InsightQuery]
    store_candidates: list[InsightStoreCandidate]


class MissingSource(V2BaseModel):
    name: str = Field(min_length=1, max_length=64)
    why: str = Field(min_length=1, max_length=240)
    priority: Literal["future"]


class SnippetWriteback(V2BaseModel):
    title: str = Field(min_length=1, max_length=120)
    body: list[str] = Field(min_length=1)
    tags: list[str] = Field(min_length=1)
    related_instruments: list[str] = Field(default_factory=list)
    related_regions: list[str] = Field(default_factory=list)
    expires_at_utc: str | None = None


class AnchorBase(V2BaseModel):
    id: str = Field(min_length=1)
    type: str
    priority: Literal["must", "should", "nice_to_have"]
    purpose: str = Field(min_length=1, max_length=240)
    time_range: DateRange


class MarketTransform(V2BaseModel):
    name: Literal["x_day_avg", "x_day_min", "x_day_max", "resample"]
    field: Literal["open", "high", "low", "close", "volume", "adj_close"] | None = None
    window_days: int | None = Field(default=None, ge=2, le=400)
    resample_to: Literal["1w", "1mo"] | None = None
    method: Literal["mean", "min", "max", "sum", "last"] | None = None


class MarketAdjustments(V2BaseModel):
    splits: bool
    dividends: bool


class MarketAnchor(AnchorBase):
    type: Literal["market_ohlcv"]
    entity: EntityMic
    bar_size: Literal["1d"]
    fields: list[Literal["open", "high", "low", "close", "volume", "adj_close"]] = Field(min_length=1)
    adjustments: MarketAdjustments | None = None
    transforms: list[MarketTransform] = Field(default_factory=list)


class EdgarMetric(V2BaseModel):
    kind: str = Field(min_length=1, max_length=64)
    concept_qnames_allow: list[str] = Field(min_length=1)


class EdgarDimensions(V2BaseModel):
    axis_qnames_allow: list[str] = Field(default_factory=list)
    member_qnames_allow: list[str] = Field(default_factory=list)


class EdgarUnits(V2BaseModel):
    measures_allow: list[str] = Field(default_factory=list)


class EdgarAnchor(AnchorBase):
    type: Literal["edgar_xbrl"]
    entity: EntityMic
    allow_ambiguous_cik: bool = False
    period_type: Literal["instant", "duration"]
    grain: Literal["quarterly", "annual", "ttm"]
    metric: EdgarMetric
    dimensions: EdgarDimensions | None = None
    units: EdgarUnits | None = None


class RealEstateScope(V2BaseModel):
    geo_id: str = Field(min_length=1, max_length=128)


class RealEstateAnchor(AnchorBase):
    type: Literal["real_estate_intent"]
    entity_scope: RealEstateScope
    grain: Literal["monthly", "quarterly", "annual"]
    measures: list[str] = Field(min_length=1)
    filters: dict = Field(default_factory=dict)


Anchor = Annotated[
    MarketAnchor | EdgarAnchor | RealEstateAnchor,
    Field(discriminator="type"),
]


class Step1Payload(V2BaseModel):
    context: Context
    data_needed_fluid: list[str]
    needs_data: bool
    can_answer_now: bool
    stop_reason: Literal["answered", "need_more_data", "insufficient_datasets", "need_clarification"]
    clarifying_questions: list[str] = Field(default_factory=list)
    final_answer: str | None = None
    anchors: list[Anchor] = Field(default_factory=list)
    insight_ops: InsightOps
    insights_writeback: list[SnippetWriteback] = Field(default_factory=list)
    missing_sources: list[MissingSource] = Field(default_factory=list)


# --- Step 2 ---------------------------------------------------------------

class EntityResolution(V2BaseModel):
    anchor_id: str = Field(min_length=1)
    entity: EntityMic
    status: Literal["ok", "not_found", "ambiguous", "error"]
    resolved: dict | None = None
    note: str | None = Field(default=None, max_length=240)


class MarketOhlcvParams(V2BaseModel):
    ticker: str = Field(min_length=1, max_length=24)
    exchange_mic: str = Field(pattern=r"^[A-Z0-9]{4}$")
    start_utc: str
    end_utc: str
    bar_size: Literal["1d"]
    fields: list[Literal["open", "high", "low", "close", "volume", "adj_close"]] = Field(min_length=1)
    adjust_splits: bool | None = None
    adjust_dividends: bool | None = None
    post_aggregations: list[dict] = Field(default_factory=list)


class MarketOhlcvRequest(V2BaseModel):
    request_id: str = Field(min_length=1)
    type: Literal["market_ohlcv"]
    params: MarketOhlcvParams
    timeout_ms: int | None = Field(default=None, ge=1000, le=300000)


class SqlParams(V2BaseModel):
    dialect: Literal["sqlite", "postgres"]
    read_only: Literal[True]
    sql: str = Field(min_length=1)
    timeout_ms: int = Field(ge=1000, le=300000)
    max_rows: int = Field(ge=1, le=200000)


class SqlRequest(V2BaseModel):
    request_id: str = Field(min_length=1)
    type: Literal["sql"]
    dataset: Literal["edgar", "real_estate"]
    params: SqlParams


Request = Annotated[MarketOhlcvRequest | SqlRequest, Field(discriminator="type")]


class Batch(V2BaseModel):
    batch_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1, max_length=240)
    depends_on_batches: list[str] = Field(default_factory=list)
    requests: list[Request] = Field(min_length=1)


class RetrievalPlan(V2BaseModel):
    entity_resolution_report: list[EntityResolution] = Field(default_factory=list)
    batches: list[Batch] = Field(min_length=1)

