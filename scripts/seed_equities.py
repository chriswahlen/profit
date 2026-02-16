#!/usr/bin/env python3
"""Seed canonical equities from FinanceDatabase CSV exports.

This script ingests FinanceDatabase equity listings into the canonical entity
store. It maps provider symbols to canonical IDs using a static
exchange→MIC lookup, records optional metadata such as sector, industry,
currency, country, and ISIN, and links securities to companies.

Usage:
  python scripts/seed_equities.py --csv path/to/equities.csv --asset-class equities
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Iterable, Optional

try:
    import pycountry
except ImportError:
    pycountry = None
from config import Config
from data_sources.entity import Entity, EntityStore, EntityType
from data_sources.entities import Company, Sector, Industry
from scripts.seed_exchanges import EXCHANGES

# FinanceDatabase exchange codes to MIC mappings (subset focused on US + majors).
EXCHANGE_TO_MIC = {
    # United States
    "NMS": "XNAS",  # NASDAQ
    "NGM": "XNAS",  # NASDAQ Global Market
    "NCM": "XNAS",  # NASDAQ Capital Market
    "NAS": "XNAS",
    "NYQ": "XNYS",  # NYSE
    "NYS": "XNYS",
    "ASE": "XASE",  # NYSE American (AMEX)
    "XASE": "XASE",
    "PCX": "ARCX",  # NYSE Arca
    "ARCX": "ARCX",
    "BATS": "BATS",  # Cboe BZX
    "IEX": "IEXG",
    # OTC / Pink (map to OTC Link)
    "PNK": "OTCM",
    "OTC": "OTCM",
    # Canada
    "TOR": "XTSE",
    "TSE": "XTSE",
    "VAN": "XTSX",
    "TSX": "XTSE",
    # UK
    "LSE": "XLON",
    # Europe (Germany/Austria/Switzerland)
    "FRA": "XFRA",
    "STU": "XSTU",
    "BER": "XBER",
    "MUN": "XMUN",
    "DUS": "XDUS",
    "HAM": "XHAM",
    "GER": "XETR",  # use XETR for aggregated Germany code
    "VIE": "XWBO",
    "EBS": "XSWX",
    "SWX": "XSWX",
    "VTX": "XVTX",
    # France
    "PAR": "XPAR",
    # Italy
    "MIL": "XMIL",
    # Spain
    "MCE": "XMAD",
    # Nordics
    "CSE": "XCSE",
    "STO": "XSTO",
    "HEL": "XHEL",
    "ICE": "XICE",
    "OSL": "XOSL",
    # Asia-Pacific
    "JPX": "XTKS",  # Tokyo (generic JPX code -> Tokyo)
    "TKS": "XTKS",
    "TKO": "XTKS",
    "TSE": "XTKS",  # sometimes reused; prefer Tokyo
    "OSE": "XOSE",
    "NGO": "XNGO",
    "NSE": "XNSE",
    "BSE": "XBOM",
    "HKG": "XHKG",
    "SHZ": "XSHE",
    "SHE": "XSHE",
    "SHH": "XSHG",
    "SHG": "XSHG",
    "KSC": "XKOS",
    "KOS": "XKOS",
    "KOE": "XKRX",
    "ASX": "XASX",
    "NZX": "XNZE",
    "SGX": "XSES",
    "TWO": "ROCO",  # Taipei OTC
    "TAI": "XTAI",
    "PSE": "XPHS",
    "SET": "XBKK",
    # Others
    "SAO": "BVMF",
    "MEX": "XMEX",
    "JNB": "XJSE",
    "IST": "XIST",
    # Additional exchanges from remaining codes
    "HAN": "XHAN",  # Hannover
    "KLS": "XKLS",  # Bursa Malaysia
    "IOB": "IOBA",  # LSE International Order Book
    "JKT": "XIDX",  # Indonesia
    "CPH": "XCSE",  # alias Copenhagen
    "TLO": "XTAL",  # Tallinn
    "SGO": "XSGO",  # Santiago
    "CNQ": "XNCA",  # CSE (Canadian Securities Exchange)
    "SES": "XSES",  # alias Singapore
    "TLV": "XTAE",  # Tel Aviv
    "BUE": "XBCBA",  # Buenos Aires
    "AQS": "AQSE",   # Aquis Stock Exchange
    "NZE": "XNZE",   # NZX alias
    "MCX": "MOEX",   # Moscow Exchange
    "CAI": "XCAI",   # Cairo
    "AMS": "XAMS",   # Euronext Amsterdam
    "BRU": "XBRU",   # Euronext Brussels
    "ATH": "XATH",   # Athens
    "SAU": "XSAU",   # Saudi Exchange (Tadawul)
    "LIS": "XLIS",   # Euronext Lisbon
    "CCS": "XCAS",   # Casablanca
    "DOH": "XQAT",   # Doha / Qatar
    "BUD": "XBUD",   # Budapest
    "PRA": "XPRA",   # Prague
    "ISE": "XDUB",   # Irish Stock Exchange (Dublin)
    "FKA": "XFRA",   # Frankfurt alias
    "NEO": "NEOE",   # NEO Exchange (Canada)
    "LIT": "XLIT",   # Cboe Europe LIT
    "RIS": "XRIS",   # Riga (Nasdaq Riga)
    "TAL": "XTAL",   # Tallinn
    "SAP": "XSAU",   # Saudi (parallel market)
    "ENX": "XAMS",   # Euronext umbrella -> Amsterdam
    "BTS": "XBTS",   # Bratislava
    "NSI": "XNSA",   # Nigerian Exchange
    "SAT": "XSAU",   # Saudi
    "NAE": "XNAM",   # Namibia
    "OBB": "XTAL",   # Assume OBB -> Vienna/Talinn style; treat as Vienna region
    # Allow MIC keys directly (for suffix inference returning MIC)
    "XKRX": "XKRX",
    "XKOS": "XKOS",
    "XHKG": "XHKG",
    "XTAI": "XTAI",
    "ROCO": "ROCO",
    "XTSE": "XTSE",
    "XTSX": "XTSX",
    "NEOE": "NEOE",
    "XLON": "XLON",
    "XPAR": "XPAR",
    "XFRA": "XFRA",
    "XBER": "XBER",
    "XDUS": "XDUS",
    "XMUN": "XMUN",
    "XHAM": "XHAM",
    "XSES": "XSES",
    "XMIL": "XMIL",
    "XAMS": "XAMS",
    "XBRU": "XBRU",
    "XWBO": "XWBO",
    "XLIS": "XLIS",
    "XDUB": "XDUB",
    "XHEL": "XHEL",
    "XCSE": "XCSE",
    "XTKS": "XTKS",
    "XKLS": "XKLS",
    "XSHG": "XSHG",
    "XASX": "XASX",
    "XNZE": "XNZE",
    "BVMF": "BVMF",
}

UNKNOWN_EXCHANGE_CODE = "UNKNOWN"
DEFAULT_COMPANY_COUNTRY = "US"

_COUNTRY_CACHE: dict[str, Optional[str]] = {}

COUNTRY_ALIASES: dict[str, str] = {
    "united states": "us",
    "united states of america": "us",
    "usa": "us",
    "us": "us",
    "canada": "ca",
    "china": "cn",
    "hong kong": "hk",
    "south korea": "kr",
    "korea, south": "kr",
    "korea": "kr",
    "japan": "jp",
    "united kingdom": "gb",
    "uk": "gb",
    "france": "fr",
    "germany": "de",
    "australia": "au",
    "india": "in",
    "thailand": "th",
    "taiwan": "tw",
    "sweden": "se",
    "brazil": "br",
    "singapore": "sg",
    "italy": "it",
    "israel": "il",
    "norway": "no",
    "spain": "es",
    "netherlands": "nl",
    "brazil": "br",
    "switzerland": "ch",
    "denmark": "dk",
    "ireland": "ie",
    "poland": "pl",
    "mexico": "mx",
    "austria": "at",
    "russia": "ru",
    "bahrain": "bh",
}

def normalize_country_name(country: str | None) -> Optional[str]:
    """Return ISO2 code for a country name (cache results)."""

    if not country:
        return None
    key = country.strip()
    if not key:
        return None
    lower_key = key.lower()
    if lower_key in _COUNTRY_CACHE:
        return _COUNTRY_CACHE[lower_key]
    iso = None
    if pycountry:
        try:
            match = pycountry.countries.lookup(key)
            iso = match.alpha_2.lower()
        except LookupError:
            iso = None
    if not iso:
        iso = COUNTRY_ALIASES.get(lower_key)
    _COUNTRY_CACHE[lower_key] = iso
    return iso

EXCHANGE_COUNTRY: dict[str, str] = {}
for entry in EXCHANGES:
    if entry.country:
        iso = normalize_country_name(entry.country)
        if iso:
            EXCHANGE_COUNTRY[entry.mic.upper()] = iso

PROVIDER = "provider:financedatabase"
ISIN_PROVIDER = "provider:isin"
ISIN_PROVIDER_DESC = "ANNA/ISIN registry"
RELATION_COMPANY_ISSUER = "issued_security"
RELATION_SECTOR = "belongs_to_sector"
RELATION_INDUSTRY = "belongs_to_industry"


def canonical_id(symbol: str, exchange: str) -> str | None:
    exch_clean = (exchange or "").upper()
    mic = EXCHANGE_TO_MIC.get(exch_clean)
    if not mic and exch_clean == UNKNOWN_EXCHANGE_CODE:
        mic = UNKNOWN_EXCHANGE_CODE
    if not mic:
        return None
    if not symbol:
        return None
    # Strip common provider suffixes (.SA, .KS, etc.) from the symbol when building canonical ID.
    base_symbol = symbol
    if "." in symbol:
        base_symbol = symbol.split(".")[0]
    return f"sec:{mic.lower()}:{base_symbol.lower()}"


def infer_exchange_from_suffix(symbol: str) -> str | None:
    """Infer exchange code when exchange column is empty using common suffixes."""
    upper = symbol.upper()
    # Asia / Oceania
    if upper.endswith(".KS"):
        return "XKRX"  # KRX main board
    if upper.endswith(".KQ"):
        return "XKOS"  # KOSDAQ
    if upper.endswith(".HK"):
        return "XHKG"
    if upper.endswith(".TW"):
        return "XTAI"
    if upper.endswith(".TWO"):
        return "ROCO"
    if upper.endswith(".JP"):
        return "XTKS"
    if upper.endswith(".TO"):
        return "XTSE"
    if upper.endswith(".V"):
        return "XTSX"
    if upper.endswith(".NE"):
        return "NEOE"
    if upper.endswith(".AX"):
        return "XASX"
    if upper.endswith(".NZ"):
        return "XNZE"
    if upper.endswith(".NX"):
        return "ENX"
    if upper.endswith(".T"):
        return "XTKS"
    # Americas
    if upper.endswith(".SA"):
        return "BVMF"
    # UK / Europe
    if upper.endswith(".L"):
        return "XLON"
    if upper.endswith(".IL"):
        return "XLON"
    if upper.endswith(".PA"):
        return "XPAR"
    if upper.endswith(".F"):
        return "XFRA"
    if upper.endswith(".BE"):
        return "XBER"
    if upper.endswith(".DU"):
        return "XDUS"
    if upper.endswith(".MU"):
        return "XMUN"
    if upper.endswith(".HA"):
        return "XHAM"
    if upper.endswith(".DE"):
        return "XFRA"
    if upper.endswith(".SG"):
        return "XSES"
    if upper.endswith(".MI"):
        return "XMIL"
    if upper.endswith(".AS"):
        return "XAMS"
    if upper.endswith(".BR"):
        return "XBRU"
    if upper.endswith(".VI"):
        return "XWBO"
    if upper.endswith(".LS"):
        return "XLIS"
    if upper.endswith(".IR"):
        return "XDUB"
    if upper.endswith(".HE"):
        return "XHEL"
    if upper.endswith(".CO"):
        return "XCSE"
    if upper.endswith(".HM"):
        return "XHAM"
    if upper.endswith(".SS"):
        return "XSHG"
    if upper.endswith(".SZ"):
        return "XSHE"
    if upper.endswith(".KL"):
        return "XKLS"
    return None


def row_metadata(_: dict) -> str:
    return ""


def seed_rows(rows: Iterable[dict], store: EntityStore) -> tuple[int, int]:
    store.upsert_provider(PROVIDER, description="FinanceDatabase symbols")
    store.ensure_relation_type("listed_on", description="Security listed on exchange")
    store.ensure_relation_type(RELATION_COMPANY_ISSUER, description="Company issues the security")
    store.ensure_relation_type(RELATION_SECTOR, description="Security belongs to sector")
    store.ensure_relation_type(RELATION_INDUSTRY, description="Security belongs to industry")
    inserted = skipped = 0
    for row in rows:
        symbol = (row.get("symbol") or "").strip().upper()
        provided_name = (row.get("name") or "").strip()
        name = provided_name or symbol
        exchange = (row.get("exchange") or "").strip().upper()

        # If exchange missing, try to infer from symbol suffix.
        if not exchange:
            inferred = infer_exchange_from_suffix(symbol)
            exchange = inferred or UNKNOWN_EXCHANGE_CODE

        if not symbol:
            skipped += 1
            continue

        if not name:
            name = symbol  # fallback: use symbol as name when missing

        cid = canonical_id(symbol, exchange)
        if not cid:
            skipped += 1
            continue
        company_id = None

        entity = Entity(entity_id=cid, entity_type=EntityType.SECURITY, name=name, metadata=row_metadata(row))
        store.upsert_entity(entity)
        meta = None
        store.map_provider_entity(
            provider=PROVIDER,
            provider_entity_id=symbol,
            entity_id=cid,
            active_from=None,
            active_to=None,
            metadata=meta,
        )
        # Link security to its exchange if we have a mapped MIC (and not UNKNOWN).
        if mic := EXCHANGE_TO_MIC.get(exchange) or (exchange if exchange == UNKNOWN_EXCHANGE_CODE else None):
            logging.debug("Linking security %s to exchange %s", cid, mic)
            store.map_entity_relation(
                src_entity_id=cid,
                dst_entity_id=f"mic:{mic.lower()}",
                relation="listed_on",
            )

        sector_val = (row.get("sector") or "").strip()
        if sector_val:
            try:
                sector = Sector.from_name(sector_val)
                sector_entity = Entity(
                    entity_id=sector.canonical_id,
                    entity_type=EntityType.SECTOR,
                    name=sector.name,
                )
                store.upsert_entity(sector_entity)
                store.map_entity_relation(
                    src_entity_id=cid,
                    dst_entity_id=sector_entity.entity_id,
                    relation=RELATION_SECTOR,
                )
            except ValueError:
                logging.debug("Skipping sector entity for %s", sector_val)

        industry_val = (row.get("industry") or "").strip()
        if industry_val:
            try:
                industry = Industry.from_name(industry_val)
                industry_entity = Entity(
                    entity_id=industry.canonical_id,
                    entity_type=EntityType.INDUSTRY,
                    name=industry.name,
                )
                store.upsert_entity(industry_entity)
                store.map_entity_relation(
                    src_entity_id=cid,
                    dst_entity_id=industry_entity.entity_id,
                    relation=RELATION_INDUSTRY,
                )
            except ValueError:
                logging.debug("Skipping industry entity for %s", industry_val)

        company_iso = (
            normalize_country_name(row.get("country"))
            or EXCHANGE_COUNTRY.get(exchange)
            or DEFAULT_COMPANY_COUNTRY.lower()
        )
        company_id = None
        if provided_name:
            try:
                company = Company.from_name(provided_name, country_iso2=company_iso)
                company_entity = Entity(
                    entity_id=company.canonical_id,
                    entity_type=EntityType.COMPANY,
                    name=name,
                )
                store.upsert_entity(company_entity)
                company_id = company.canonical_id
                logging.debug("Linking company %s to security %s", company.canonical_id, cid)
                store.map_entity_relation(
                    src_entity_id=company.canonical_id,
                    dst_entity_id=cid,
                    relation=RELATION_COMPANY_ISSUER,
                )
            except ValueError:
                logging.debug("Skipping company creation for %s (country iso %s)", symbol, company_iso)

        isin = (row.get("isin") or "").strip()
        if isin.lower() == "not available":
            isin = ""
        if isin and company_id:
            store.upsert_provider(ISIN_PROVIDER, description=ISIN_PROVIDER_DESC)
            store.map_provider_entity(
                provider=ISIN_PROVIDER,
                provider_entity_id=isin,
                entity_id=company_id,
            )
        inserted += 1
    return inserted, skipped


def load_csv(path: Path, limit: int | None = None) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = []
        for idx, row in enumerate(reader):
            rows.append(row)
            if limit and idx + 1 >= limit:
                break
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed equities from FinanceDatabase CSV")
    parser.add_argument("--csv", required=True, help="Path to FinanceDatabase CSV (e.g., equities.csv)")
    parser.add_argument(
        "--asset-class",
        default="equities",
        choices=["equities"],
        help="Asset class to load (currently supports equities)",
    )
    parser.add_argument("--limit", type=int, help="Optional row limit for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    cfg = Config()
    store = EntityStore(cfg)
    rows = load_csv(csv_path, limit=args.limit)
    inserted, skipped = seed_rows(rows, store)
    logging.info("FinanceDatabase equities seeded %d rows (skipped %d)", inserted, skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
