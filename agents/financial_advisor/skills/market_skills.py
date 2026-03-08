from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from statistics import mean, median
from typing import Any, Sequence, Tuple

from agents.financial_advisor.skills.skill_interface import (
    SkillDescriptor,
    SkillExecutionResult,
    SkillInterface,
    SkillUsagePrompt,
)
from data_sources.market.market_data_store import MarketDataStore


@dataclass(frozen=True)
class _MarketSkillMeta:
    skill_id: str
    name: str
    summary: str
    prompt: str
    example_questions: Sequence[str]


@dataclass(frozen=True)
class _QuotesSkillInput:
    symbol: str
    user_columns: Tuple[str, ...]
    mapped_columns: Tuple[str, ...]
    start_ts: str | None
    end_ts: str | None
    start_date: date | None
    end_date: date | None
    aggregation_method: str
    aggregation_period: str | None


class MarketSkills(SkillInterface):
    """
    Provides descriptor/usage info and execution support for market OHLCV queries.
    """

    SKILL_QUOTES = "skill:market:quotes"

    _SKILL_META = _MarketSkillMeta(
        skill_id=SKILL_QUOTES,
        name="Market quotes",
        summary="Return OHLCV data for a canonical symbol.",
        prompt="""
Provide a JSON payload describing the query. Required keys: `symbol` (canonical ID like `sec:xnas:aapl`) and
`columns`, a list of metrics from the store (`open`, `high`, `low`, `close`, `adj_close`, `dividend`, `volume`).
Optional keys: `start` and `end` (ISO dates) plus `aggregation` with `method` (none|avg|median) and `period`
(week|month|year).

Example input:
{
  "symbol": "sec:xnas:aapl",
  "columns": ["open", "close", "volume"],
  "start": "2026-01-01",
  "end": "2026-03-31",
  "aggregation": {
    "method": "avg",
    "period": "month"
  }
}
""",
        example_questions=[
            "Retrieve the latest open, close, and volume for sec:xnas:aapl.",
            "Give me weekly average close and volume for sec:xnas:aapl from 2025-01-01 to 2025-03-31.",
        ],
    )

    _COLUMN_ALIASES: dict[str, str] = {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "dividend": "dividend",
        "adjusted_close": "adj_close",
        "adj_close": "adj_close",
    }
    _DEFAULT_COLUMNS = ("close",)
    _AGGREGATION_METHODS = {"none", "avg", "median"}
    _AGGREGATION_PERIODS = {"week", "month", "year"}

    def __init__(self, store: MarketDataStore, logger: logging.Logger | None = None) -> None:
        self._store = store
        self._logger = logger or logging.getLogger(__name__)

    # --- SkillInterface -----------------------------------------------------
    def list_skills(self) -> Sequence[SkillDescriptor]:
        meta = self._SKILL_META
        return [SkillDescriptor(skill_id=meta.skill_id, name=meta.name, summary=meta.summary)]

    def describe_skill_usage(self, skill_id: str) -> SkillUsagePrompt:
        if skill_id != self.SKILL_QUOTES:
            raise ValueError(f"Unknown market skill id {skill_id}")
        meta = self._SKILL_META
        return SkillUsagePrompt(
            skill_id=meta.skill_id,
            prompt=meta.prompt,
            example_questions=meta.example_questions,
        )

    def execute_skill(self, skill_id: str, payload: dict[str, Any]) -> SkillExecutionResult:
        if skill_id != self.SKILL_QUOTES:
            raise ValueError(f"Unknown market skill id {skill_id}")

        inputs = self._parse_payload(payload)
        self._logger.info(
            "Executing market quotes skill symbol=%s start=%s end=%s agg=%s/%s",
            inputs.symbol,
            inputs.start_ts or "any",
            inputs.end_ts or "any",
            inputs.aggregation_method,
            inputs.aggregation_period or "none",
        )
        rows = self._store.query_candles_best(
            canonical_id=inputs.symbol,
            start_ts=inputs.start_ts,
            end_ts=inputs.end_ts,
        )

        if not rows:
            metadata = {
                "symbol": inputs.symbol,
                "row_count": 0,
                "aggregation": inputs.aggregation_method,
                "period": inputs.aggregation_period,
                "start": inputs.start_ts,
                "end": inputs.end_ts,
            }
            return SkillExecutionResult(skill_id=skill_id, records=[], metadata=metadata)

        metadata = {
            "symbol": inputs.symbol,
            "row_count": len(rows),
            "aggregation": inputs.aggregation_method,
            "period": inputs.aggregation_period,
            "start": inputs.start_ts,
            "end": inputs.end_ts,
        }

        if inputs.aggregation_method != "none":
            aggregated = self._aggregate_rows(rows=rows, inputs=inputs)
            metadata["aggregate_rows"] = len(aggregated)
            return SkillExecutionResult(skill_id=skill_id, records=aggregated, metadata=metadata)

        standard_records = self._project_rows(rows=rows, inputs=inputs)
        return SkillExecutionResult(skill_id=skill_id, records=standard_records, metadata=metadata)

    # --- helpers ------------------------------------------------------------
    def _parse_payload(self, payload: dict[str, Any]) -> _QuotesSkillInput:
        symbol = payload.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a non-empty string")
        symbol = symbol.strip()

        columns_input = payload.get("columns")
        if columns_input is None:
            columns_input = list(self._DEFAULT_COLUMNS)
        if not isinstance(columns_input, Sequence) or isinstance(columns_input, (str, bytes)):
            raise ValueError("columns must be a list of column names")

        seen: set[str] = set()
        user_columns: list[str] = []
        mapped_columns: list[str] = []
        for raw_column in columns_input:
            if not isinstance(raw_column, str):
                raise ValueError("each column name must be a string")
            normalized = raw_column.strip().lower()
            if not normalized:
                continue
            if normalized in seen:
                continue
            alias = self._COLUMN_ALIASES.get(normalized)
            if alias is None:
                raise ValueError(f"Unknown column name '{raw_column}'")
            seen.add(normalized)
            user_columns.append(normalized)
            mapped_columns.append(alias)

        if not user_columns:
            raise ValueError("columns must include at least one metric")

        start_str = payload.get("start")
        start_date = self._parse_iso_date(start_str) if start_str else None
        end_str = payload.get("end")
        end_date = self._parse_iso_date(end_str) if end_str else None
        if start_date and end_date and start_date > end_date:
            raise ValueError("start date must be earlier than or equal to end date")

        aggregation_payload = payload.get("aggregation") or {}
        if not isinstance(aggregation_payload, dict):
            raise ValueError("aggregation must be an object when provided")

        method = aggregation_payload.get("method", "none")
        if method is None:
            method = "none"
        if not isinstance(method, str):
            raise ValueError("aggregation.method must be a string")
        method = method.strip().lower()
        if method not in self._AGGREGATION_METHODS:
            raise ValueError(f"Unsupported aggregation method '{method}'")

        period: str | None = None
        if method != "none":
            period_raw = aggregation_payload.get("period")
            if not isinstance(period_raw, str) or not period_raw.strip():
                raise ValueError("aggregation.period is required when aggregation.method is not 'none'")
            period = period_raw.strip().lower()
            if period not in self._AGGREGATION_PERIODS:
                raise ValueError(f"Unsupported aggregation period '{period_raw}'")

        return _QuotesSkillInput(
            symbol=symbol,
            user_columns=tuple(user_columns),
            mapped_columns=tuple(mapped_columns),
            start_ts=start_str,
            end_ts=end_str,
            start_date=start_date,
            end_date=end_date,
            aggregation_method=method,
            aggregation_period=period,
        )

    def _parse_iso_date(self, raw: Any) -> date:
        if not isinstance(raw, str):
            raise ValueError("date values must be strings")
        try:
            return datetime.strptime(raw.strip(), "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"dates must follow ISO format YYYY-MM-DD ({raw})") from exc

    def _project_rows(self, rows: Sequence[dict[str, Any]], inputs: _QuotesSkillInput) -> list[dict[str, Any]]:
        projected = []
        for row in rows:
            record = {"start_ts": row["start_ts"], "provider": row["provider"]}
            for user_col, db_col in zip(inputs.user_columns, inputs.mapped_columns):
                record[user_col] = row.get(db_col)
            projected.append(record)
        return projected

    def _aggregate_rows(self, rows: Sequence[dict[str, Any]], inputs: _QuotesSkillInput) -> list[dict[str, Any]]:
        assert inputs.aggregation_period is not None  # guarded by parser
        buckets: dict[Tuple[str, date, date], list[dict[str, Any]]] = {}
        for row in rows:
            ts = row.get("start_ts")
            if not ts:
                continue
            row_date = self._parse_iso_date(ts)
            label, period_start, period_end = self._period_bounds(row_date, inputs.aggregation_period)
            key = (label, period_start, period_end)
            buckets.setdefault(key, []).append(row)

        aggregated: list[dict[str, Any]] = []
        for (label, period_start, period_end), bucket_rows in buckets.items():
            bucket_record = {
                "period_label": label,
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "count": len(bucket_rows),
            }
            for user_col, db_col in zip(inputs.user_columns, inputs.mapped_columns):
                values = [row[db_col] for row in bucket_rows if row.get(db_col) is not None]
                if not values:
                    bucket_record[user_col] = None
                elif inputs.aggregation_method == "avg":
                    bucket_record[user_col] = float(mean(values))
                else:
                    bucket_record[user_col] = float(median(values))
            aggregated.append(bucket_record)
        aggregated.sort(key=lambda record: record["period_end"], reverse=True)
        return aggregated

    def _period_bounds(self, sample_date: date, period: str) -> Tuple[str, date, date]:
        if period == "week":
            start = sample_date - timedelta(days=sample_date.weekday())
            end = start + timedelta(days=6)
            label = f"{start.isocalendar()[0]}-W{start.isocalendar()[1]:02d}"
            return label, start, end
        if period == "month":
            start = sample_date.replace(day=1)
            last_day = calendar.monthrange(sample_date.year, sample_date.month)[1]
            end = sample_date.replace(day=last_day)
            label = f"{sample_date.year}-{sample_date.month:02d}"
            return label, start, end
        if period == "year":
            start = date(sample_date.year, 1, 1)
            end = date(sample_date.year, 12, 31)
            label = str(sample_date.year)
            return label, start, end
        raise ValueError(f"Unsupported aggregation period '{period}'")
