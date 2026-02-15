

from __future__ import annotations

from typing import Iterable, List
import csv
import gzip
import logging
from pathlib import Path
from datetime import datetime

from config import Config
from data_sources.data_source import DataSource, DataSourceUpdateResults
from data_sources.entity import EntityStore, Entity, EntityType
from data_sources.redfin.redfin_data_store import RedfinDataStore, MarketMetric
from data_sources.region import Region


class RedfinDataSource(DataSource):
    """Redfin data source stub that ingests into `RedfinDataStore`.
    """

    def __init__(self, config: Config, entity_store: EntityStore):
        super().__init__(name="redfin", summary="Residential listings from Redfin", config=config, entity_store=entity_store)
        self.store = RedfinDataStore(config)
        self.logger = logging.getLogger(__name__)

    def describe_detailed(self, *, indent: str = "  ") -> str:
        lines = [f"{indent}Storage: sqlite at {self.store.db_path}"]
        lines.append(self.store.describe_detailed(indent=indent))
        return "\n".join(lines)

    def ensure_up_to_date(self, entity_ids: Iterable[str]) -> DataSourceUpdateResults:
        data_dir = Path("incoming/datasets/redfin")
        files = sorted(data_dir.glob("*.tsv000.gz"))
        if not files:
            return DataSourceUpdateResults(detail="No redfin files found")

        total_updated = total_failed = 0
        provider = "redfin"

        for gz_path in files:
            run_id = self.store.start_ingestion_run(provider=provider, source_url=str(gz_path))
            updated, failed = self._ingest_file(gz_path, provider)
            status = "success" if failed == 0 else "partial"
            self.store.finish_ingestion_run(
                run_id=run_id,
                status=status,
                row_count=updated,
                notes=f"updated={updated}, failed={failed}",
            )
            total_updated += updated
            total_failed += failed

        return DataSourceUpdateResults(updated=total_updated, failed=total_failed)

    # --- internal helpers -----------------------------------------------------
    def _ingest_file(self, gz_path: Path, provider: str) -> tuple[int, int]:
        updated = failed = 0
        metrics_batch: List = []
        batch_flush_size = 10_000
        read_rows = 0

        self.logger.info("Opening Redfin data file %s", gz_path)
        truncated = False
        with gzip.open(gz_path, "rt", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t", quotechar='"')
            try:
                for idx, row in enumerate(reader, start=1):
                    try:
                        region_id, data_revision = self._ensure_region(row, provider)
                        self._ensure_property_type(row, provider)
                        metric = self._row_to_metric(row, region_id, data_revision, provider)
                        metrics_batch.append(metric)
                    except Exception:
                        failed += 1
                        self.logger.exception("Failed to ingest row from %s", gz_path.name)
                    read_rows = idx
                    if idx % 5000 == 0:
                        self.logger.info("Read %d rows from %s", idx, gz_path.name)
                    if len(metrics_batch) >= batch_flush_size:
                        res = self.store.upsert_market_metrics(metrics_batch)
                        updated += res.updated
                        failed += res.failed
                        self.logger.info(
                            "Flushed %d metrics (total read %d) from %s",
                            res.updated,
                            read_rows,
                            gz_path.name,
                        )
                        metrics_batch.clear()
            except EOFError:
                truncated = True
                self.logger.warning("File truncated while reading %s after %d rows", gz_path.name, read_rows)

        if metrics_batch:
            res = self.store.upsert_market_metrics(metrics_batch)
            updated += res.updated
            failed += res.failed
        self.logger.info(
            "Imported %d rows from %s%s",
            updated,
            gz_path.name,
            " (truncated read)" if truncated else "",
        )

        return updated, failed

    def _ensure_region(self, row: dict, provider: str) -> tuple[str, int]:
        region_type = row.get("REGION_TYPE", "").lower()
        region_name = row.get("REGION", "").strip()
        city = row.get("CITY", "").strip()
        state_code = row.get("STATE_CODE", "").strip()
        provider_region_id = row.get("TABLE_ID") or row.get("REGION_TYPE_ID") or ""
        parent_provider_code = row.get("PARENT_METRO_REGION_METRO_CODE") or None

        region_obj = Region.from_fields(
            region_type=region_type,
            region_name=region_name,
            country_iso2="us",
            state_code=state_code,
            city=city,
        )
        region_id = region_obj.canonical_id  # use canonical code as primary key for now
        parent_region_id = None
        if parent_provider_code:
            parent_region_id = self.store.resolve_region_by_provider(provider, parent_provider_code)

        # derive data revision from LAST_UPDATED timestamp
        data_revision = self._parse_revision(row.get("LAST_UPDATED"))

        self.store.upsert_region(
            region_id=region_id,
            region_type=region_type,
            name=region_name,
            canonical_code=region_id,
            country_iso2="US",
            parent_region_id=parent_region_id,
            metadata=None,
        )

        self.store.upsert_region_provider_map(
            provider=provider,
            provider_region_id=str(provider_region_id),
            region_id=region_id,
            provider_name=region_name,
            active_from=datetime.utcnow().date().isoformat(),
            active_to=None,
            data_revision=data_revision,
        )

        # Maintain canonical entity registry.
        entity_id = region_id
        self.entity_store.upsert_entity(
            Entity(entity_id=entity_id, entity_type=EntityType.REGION, name=region_name)
        )
        self.entity_store.map_provider_entity(
            provider=provider,
            provider_entity_id=str(provider_region_id),
            entity_id=entity_id,
            active_from=datetime.utcnow().date().isoformat(),
        )

        return region_id, data_revision

    def _row_to_metric(self, row: dict, region_id: str, data_revision: int, provider: str):
        def to_int(val):
            if val in ("", None, "NA"):
                return None
            try:
                return int(float(val))
            except ValueError:
                return None

        def to_float(val):
            if val in ("", None, "NA"):
                return None
            try:
                return float(val)
            except ValueError:
                return None

        return MarketMetric(
            region_id=region_id,
            property_type_id=str(row.get("PROPERTY_TYPE_ID") or ""),
            period_start_date=row.get("PERIOD_BEGIN"),
            period_granularity=str(row.get("PERIOD_DURATION")),
            data_revision=data_revision,
            source_provider=provider,
            median_sale_price=to_float(row.get("MEDIAN_SALE_PRICE")),
            median_list_price=to_float(row.get("MEDIAN_LIST_PRICE")),
            homes_sold=to_int(row.get("HOMES_SOLD")),
            new_listings=to_int(row.get("NEW_LISTINGS")),
            inventory=to_int(row.get("INVENTORY")),
            median_dom=to_float(row.get("MEDIAN_DOM")),
            sale_to_list_ratio=to_float(row.get("AVG_SALE_TO_LIST")),
            price_drops_pct=to_float(row.get("PRICE_DROPS")),
            pending_sales=to_int(row.get("PENDING_SALES")),
            months_supply=to_float(row.get("MONTHS_OF_SUPPLY")),
            avg_ppsf=to_float(row.get("MEDIAN_PPSF")),
        )

    @staticmethod
    def _parse_revision(last_updated: str | None) -> int:
        if not last_updated:
            return 0
        try:
            # Example: 2026-01-12 14:43:38.223 Z
            ts = datetime.fromisoformat(last_updated.replace(" Z", "+00:00"))
            return int(ts.timestamp())
        except Exception:
            return 0

    def _ensure_property_type(self, row: dict, provider: str) -> None:
        prop_id = str(row.get("PROPERTY_TYPE_ID") or "")
        prop_name = row.get("PROPERTY_TYPE") or ""
        if not prop_id:
            return
        self.store.upsert_property_type(
            provider=provider,
            property_type_id=prop_id,
            property_type_name=prop_name or prop_id,
        )
