from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, List, Optional
import zipfile

from config import Config
from data_sources.entity import Entity, EntityStore, EntityType
from data_sources.market.market_data_store import Candle, MarketDataStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StooqFile:
    zip_path: Path
    member: str  # path inside zip


class StooqImporter:
    """Batch importer for Stooq daily OHLCV text archives."""

    def __init__(
        self,
        *,
        config: Config,
        zip_paths: Iterable[Path] | None = None,
        provider: str = "stooq",
        store: MarketDataStore | None = None,
        entity_store: EntityStore | None = None,
    ):
        self.config = config
        self.provider = provider
        self.zip_paths = list(zip_paths) if zip_paths else self._default_archives()
        self.store = store or MarketDataStore(config)
        self.entities = entity_store or EntityStore(config)

    def _default_archives(self) -> list[Path]:
        base = Path("incoming/datasets/stooq")
        return sorted(base.glob("*.zip"))

    def import_all(self) -> None:
        run_id = self.store.start_ingestion_run(provider=self.provider, source="stooq:zip-archives")
        total = 0
        failed_files = 0
        for idx, stooq_file in enumerate(self._iter_files(), start=1):
            logger.info("Reading %s::%s (file %d)", stooq_file.zip_path.name, stooq_file.member, idx)
            candles, rows = self._candles_from_file(stooq_file)
            if not candles:
                logger.info("No rows found in %s::%s", stooq_file.zip_path.name, stooq_file.member)
                continue
            res = self.store.upsert_candles_raw(candles)
            count = res.updated
            total += count
            logger.info("Imported %d candles (%d rows parsed) from %s::%s", count, rows, stooq_file.zip_path.name, stooq_file.member)
            if res.failed:
                failed_files += 1
            if idx % 200 == 0:
                logger.info("Progress: processed %d files, total candles %d", idx, total)
        status = "success" if failed_files == 0 else "partial"
        self.store.finish_ingestion_run(run_id=run_id, status=status, row_count=total, notes=f"failed_files={failed_files}")
        logger.info("Completed Stooq import: %d candles (failed_files=%d)", total, failed_files)

    def _iter_files(self) -> Iterator[StooqFile]:
        for zip_path in self.zip_paths:
            with zipfile.ZipFile(zip_path) as zf:
                for member in zf.namelist():
                    if not member.lower().endswith(".txt"):
                        continue
                    yield StooqFile(zip_path=zip_path, member=member)

    def _candles_from_file(self, stooq_file: StooqFile) -> tuple[list[Candle], int]:
        candles: list[Candle] = []
        rows = 0
        with zipfile.ZipFile(stooq_file.zip_path) as zf:
            with zf.open(stooq_file.member) as fh:
                reader = csv.reader((line.decode("utf-8") for line in fh))
                header = next(reader, None)
                if not header or "<TICKER>" not in header:
                    return candles, rows
                for row in reader:
                    rows += 1
                    candle = self._row_to_candle(row, stooq_file.member)
                    if candle:
                        candles.append(candle)
                    if rows % 100000 == 0:
                        logger.info("...%s rows parsed from %s", rows, stooq_file.member)
        return candles, rows

    def iter_all_candles_with_progress(self, logger: Optional[logging.Logger] = None) -> Iterator[Candle]:
        log = logger or logging.getLogger(__name__)
        total_files = 0
        total_rows = 0
        total_candles = 0
        for stooq_file in self._iter_files():
            total_files += 1
            log.info("Reading %s::%s (file %d)", stooq_file.zip_path.name, stooq_file.member, total_files)
            candles, rows = self._candles_from_file(stooq_file)
            total_rows += rows
            total_candles += len(candles)
            if rows == 0:
                log.info("No rows in %s::%s", stooq_file.zip_path.name, stooq_file.member)
            else:
                log.info(
                    "Parsed %d rows -> %d candles from %s::%s (totals: files=%d, rows=%d, candles=%d)",
                    rows,
                    len(candles),
                    stooq_file.zip_path.name,
                    stooq_file.member,
                    total_files,
                    total_rows,
                    total_candles,
                )
            if total_files % 200 == 0:
                log.info("Progress: processed %d files (rows=%d, candles=%d)", total_files, total_rows, total_candles)
            for candle in candles:
                yield candle

    def _row_to_candle(self, row: list[str], member_path: str) -> Optional[Candle]:
        try:
            ticker = row[0].strip().lower()  # e.g., aapl.us
            date_str = row[2].strip()
            time_str = row[3].strip() or "000000"
            start_ts = self._combine_ts(date_str, time_str)
            open_p, high_p, low_p, close_p = (self._to_float(v) for v in row[4:8])
            vol = self._to_float(row[8])
        except Exception:
            logger.debug("Skipping malformed row in %s: %s", member_path, row)
            return None

        canonical_id = self._canonical_id(ticker, member_path)
        # Ensure entity mapping.
        self.entities.upsert_entity(Entity(entity_id=canonical_id, entity_type=EntityType.SECURITY, name=ticker.upper()))
        self.entities.map_provider_entity(provider=self.provider, provider_entity_id=ticker.upper(), entity_id=canonical_id, active_from="1970-01-01")

        return Candle(
            canonical_id=canonical_id,
            start_ts=start_ts,
            open=open_p,
            high=high_p,
            low=low_p,
            close=close_p,
            volume=vol,
            provider=self.provider,
        )

    @staticmethod
    def _canonical_id(ticker: str, member_path: str) -> str:
        """
        Derive a provider-agnostic canonical id.
        - Use MIC when we can infer the venue from the path.
        - Strip country suffix from ticker (aapl.us -> AAPL).
        - Fallback to stooq:<SYMBOL> if we cannot map the venue.
        """
        parts = member_path.lower().split("/")
        sym = ticker.split(".")[0].upper()
        lower = member_path.lower()

        mic_map = {
            "nasdaq": "xnas",
            "nasdaq etfs": "xnas",
            "nyse": "xnys",
            "amex": "xase",
            "bats": "xbats",
        }
        mic = None
        for key, val in mic_map.items():
            if key in lower:
                mic = val
                break

        # Special buckets
        if "indices" in lower:
            return f"index:stooq:{sym.lower()}"
        if "bonds" in lower:
            return f"bond:stooq:{sym.lower()}"
        if "crypto" in lower:
            return f"crypto:stooq:{sym.lower()}"
        if "fx/" in lower or "forex" in lower:
            return f"fx:{sym.lower()}"

        if mic:
            return f"sec:{mic}:{sym.lower()}"
        raise ValueError(f"Unable to derive canonical id for {ticker} from path {member_path}")

    @staticmethod
    def _combine_ts(date_str: str, time_str: str) -> str:
        ts = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
        return ts.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _to_float(value: str) -> Optional[float]:
        if value in ("", None):
            return None
        try:
            return float(value)
        except ValueError:
            return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = Config()
    importer = StooqImporter(config=cfg)
    importer.import_all()
