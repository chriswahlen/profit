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
import re
from pathlib import Path
from typing import Iterable, Optional

try:
    import pycountry
except ImportError:
    pycountry = None
from config import Config
from data_sources.entity import Entity, EntityStore, EntityType
from data_sources.entities import Company, FundEntity, Sector, Industry
from scripts.name_detector import (
    CompanyNameDetector,
    FundNameDetector,
    NameEquivalence,
    ProductLabelDetector,
)
from scripts.seed_exchanges import EXCHANGES

CUSIP_PROVIDER = "provider:cusip"
CUSIP_PROVIDER_DESC = "CUSIP registry"
FIGI_PROVIDER = "provider:figi"
FIGI_PROVIDER_DESC = "OpenFIGI"
_COMPANY_METADATA_SOURCE = "financedatabase"

_DEFAULT_EQUITIES_CSV = Path("incoming/datasets/fdb/equities.csv")

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


def _company_metadata_payload(**fields: str) -> str:
    payload: dict[str, str] = {"source": _COMPANY_METADATA_SOURCE}
    for key, value in fields.items():
        if value:
            payload[key] = value
    return json.dumps(payload, ensure_ascii=True)

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


def _sanitize_ticker(symbol: str | None) -> str | None:
    if not symbol:
        return None
    base_symbol = symbol.split(".", 1)[0].strip().upper()
    sanitized = re.sub(r"[^A-Z0-9]+", "", base_symbol)
    return sanitized.lower() or None


def _normalize_isin(value: str | None) -> str | None:
    if not value:
        return None
    candidate = re.sub(r"[^A-Z0-9]+", "", value.strip().upper())
    if not candidate:
        return None
    if len(candidate) != 12:
        return None
    if not re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", candidate):
        return None
    return candidate.lower()


def canonical_id(symbol: str, exchange: str, isin: str | None) -> str | None:
    if isin_normalized := _normalize_isin(isin):
        return f"sec:isin:{isin_normalized}"
    if ticker := _sanitize_ticker(symbol):
        return f"sec:ticker:{ticker}"


def _security_for_company(store: EntityStore, company_id: str) -> str | None:
    cur = store.connection.execute(
        """
        SELECT dst_entity_id
        FROM entity_entity_map
        WHERE src_entity_id = ? AND relation = ?
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        (company_id, RELATION_COMPANY_ISSUER),
    )
    row = cur.fetchone()
    return row[0] if row else None
    return None


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


def row_metadata(row: dict) -> str:
    payload: dict[str, str] = {}
    for key in (
        "summary",
        "isin",
        "cusip",
        "figi",
        "composite_figi",
        "shareclass_figi",
    ):
        if value := (row.get(key) or "").strip():
            payload[key] = value
    return json.dumps(payload, ensure_ascii=True) if payload else ""


def seed_rows(rows: Iterable[dict], store: EntityStore) -> tuple[int, int]:
    store.upsert_provider(PROVIDER, description="FinanceDatabase symbols")
    store.ensure_relation_type("listed_on", description="Security listed on exchange")
    store.ensure_relation_type(RELATION_COMPANY_ISSUER, description="Company issues the security")
    store.ensure_relation_type(RELATION_SECTOR, description="Security belongs to sector")
    store.ensure_relation_type(RELATION_INDUSTRY, description="Security belongs to industry")
    inserted = skipped = 0
    created_companies: set[str] = set()
    created_fund_entities: set[str] = set()
    known_names: dict[str, tuple[str, EntityType, str]] = {}
    created_entities: set[str] = set()
    listed_relations: set[tuple[str, str]] = set()
    mapped_symbols: set[str] = set()
    mapped_provider_ids: set[tuple[str, str]] = set()
    symbol_to_entity: dict[str, str] = {}

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

        isin = (row.get("isin") or "").strip()
        if isin.lower() == "not available":
            isin = ""
        cusip = (row.get("cusip") or "").strip()
        figi = (row.get("figi") or "").strip()
        composite_figi = (row.get("composite_figi") or "").strip()
        shareclass_figi = (row.get("shareclass_figi") or "").strip()

        identifier_entries: list[tuple[str, str]] = []
        identifier_mappings: list[tuple[str, str, str]] = []
        seen_identifiers: set[tuple[str, str]] = set()

        def _add_identifier(provider: str, description: str, value: str | None) -> None:
            if not value:
                return
            key = (provider, value.strip())
            if not key[1] or key in seen_identifiers:
                return
            seen_identifiers.add(key)
            identifier_entries.append(key)
            identifier_mappings.append((provider, description, key[1]))

        _add_identifier(ISIN_PROVIDER, ISIN_PROVIDER_DESC, isin)
        _add_identifier(CUSIP_PROVIDER, CUSIP_PROVIDER_DESC, cusip)
        for figi_value in {figi, composite_figi, shareclass_figi}:
            _add_identifier(FIGI_PROVIDER, FIGI_PROVIDER_DESC, figi_value)

        mic = EXCHANGE_TO_MIC.get(exchange) or (exchange if exchange == UNKNOWN_EXCHANGE_CODE else None)
        if not mic:
            skipped += 1
            continue

        company_iso = (
            normalize_country_name(row.get("country"))
            or EXCHANGE_COUNTRY.get(exchange)
            or DEFAULT_COMPANY_COUNTRY.lower()
        )
        company_id: str | None = None
        fund_entity_id: str | None = None
        if identifier_entries:
            if existing := _existing_company_from_identifiers(store, identifier_entries):
                company_id = existing
                if provided_name:
                    _remember_entity_name(provided_name, company_id, EntityType.COMPANY, known_names)
        if provided_name:
            if not company_id:
                match = _lookup_entity_by_name(provided_name, known_names)
                if match:
                    matched_id, matched_type, stored_name = match
                    if matched_type == EntityType.COMPANY:
                        company_id = matched_id
                    elif matched_type == EntityType.FUND_ENTITY:
                        fund_entity_id = matched_id
                    _remember_entity_name(provided_name, matched_id, matched_type, known_names)
                elif CompanyNameDetector.is_company_name(provided_name):
                    company_id = _create_company_entity(
                        store=store,
                        name=provided_name,
                        country_iso=company_iso,
                        exchange=exchange,
                        ticker=symbol,
                        created_companies=created_companies,
                    )
                    if company_id:
                        _remember_entity_name(provided_name, company_id, EntityType.COMPANY, known_names)
                elif FundNameDetector.is_fund_name(provided_name):
                    fund_entity_id = _ensure_fund_entity(
                        provided_name, store, created_fund_entities
                    )
                    if fund_entity_id:
                        _remember_entity_name(provided_name, fund_entity_id, EntityType.FUND_ENTITY, known_names)
            else:
                if CompanyNameDetector.is_company_name(provided_name):
                    _remember_entity_name(provided_name, company_id, EntityType.COMPANY, known_names)
                elif FundNameDetector.is_fund_name(provided_name):
                    fund_entity_id = _ensure_fund_entity(
                        provided_name, store, created_fund_entities
                    )
                    if fund_entity_id:
                        _remember_entity_name(provided_name, fund_entity_id, EntityType.FUND_ENTITY, known_names)
        elif FundNameDetector.is_fund_name(name):
            # fallback: name missing, but symbol-level fallback qualifies as fund
            if fund_entity_id := _ensure_fund_entity(
                name, store, created_fund_entities
            ):
                _remember_entity_name(name, fund_entity_id, EntityType.FUND_ENTITY, known_names)

        cid = None
        if company_id:
            if existing := _security_for_company(store, company_id):
                cid = existing
        symbol_key = symbol
        if not cid:
            cid = symbol_to_entity.get(symbol_key)
        if not cid:
            resolved = store.resolve_entity(PROVIDER, symbol)
            if resolved and not resolved.startswith("mic:"):
                cid = resolved
                symbol_to_entity[symbol_key] = cid
        if not cid:
            cid = canonical_id(symbol, exchange, isin)
        if not cid:
            skipped += 1
            continue
        entity = Entity(entity_id=cid, entity_type=EntityType.SECURITY, name=name, metadata=row_metadata(row))
        if cid not in created_entities:
            store.upsert_entity(entity)
            created_entities.add(cid)
            inserted += 1
        else:
            store.upsert_entity(entity, overwrite=True)

        if symbol not in mapped_symbols:
            store.map_provider_entity(
                provider=PROVIDER,
                provider_entity_id=symbol,
                entity_id=cid,
                active_from=None,
                active_to=None,
                metadata=None,
            )
            mapped_symbols.add(symbol)
            symbol_to_entity[symbol_key] = cid

        exchange_id = f"mic:{mic.lower()}"
        relation_key = (cid, exchange_id)
        if relation_key not in listed_relations:
            logging.debug("Linking security %s to exchange %s", cid, mic)
            store.map_entity_relation(
                src_entity_id=cid,
                dst_entity_id=exchange_id,
                relation="listed_on",
                metadata=json.dumps({"symbol": symbol}, ensure_ascii=True),
            )
            listed_relations.add(relation_key)

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

        if company_id:
            logging.debug("Linking company %s to security %s", company_id, cid)
            store.map_entity_relation(
                src_entity_id=company_id,
                dst_entity_id=cid,
                relation=RELATION_COMPANY_ISSUER,
            )
            identifier_mappings: list[tuple[str, str, str]] = []
            if isin:
                identifier_mappings.append((ISIN_PROVIDER, ISIN_PROVIDER_DESC, isin))
            if cusip:
                identifier_mappings.append((CUSIP_PROVIDER, CUSIP_PROVIDER_DESC, cusip))
            figi_values = {value for value in (figi, composite_figi, shareclass_figi) if value}
            for figi_value in figi_values:
                identifier_mappings.append((FIGI_PROVIDER, FIGI_PROVIDER_DESC, figi_value))
            for provider, description, value in identifier_mappings:
                _map_provider_identifier(
                    store=store,
                    provider=provider,
                    description=description,
                    provider_entity_id=value,
                    entity_id=cid,
                    mapped=mapped_provider_ids,
                )
        if fund_entity_id:
            logging.debug("Linking fund entity %s to security %s", fund_entity_id, cid)
            store.map_entity_relation(
                src_entity_id=fund_entity_id,
                dst_entity_id=cid,
                relation=RELATION_COMPANY_ISSUER,
            )
    return inserted, skipped


def _lookup_entity_by_name(
    name: str, known_names: dict[str, tuple[str, EntityType, str]]
) -> tuple[str, EntityType, str] | None:
    normalized = NameEquivalence.normalize(name)
    if not normalized:
        return None
    return known_names.get(normalized)


def _remember_entity_name(
    name: str,
    entity_id: str,
    entity_type: EntityType,
    known_names: dict[str, tuple[str, EntityType, str]],
) -> None:
    normalized = NameEquivalence.normalize(name)
    if not normalized:
        return
    known_names.setdefault(normalized, (entity_id, entity_type, name))


def _ensure_fund_entity(
    name: str, store: EntityStore, created: set[str]
) -> str | None:
    normalized = NameEquivalence.normalize(name)
    if not normalized:
        return None
    try:
        fund_entity = FundEntity.from_name(normalized)
    except ValueError:
        return None
    entity_id = fund_entity.canonical_id
    if entity_id in created or store.entity_exists(entity_id):
        created.add(entity_id)
        return entity_id
    store.upsert_entity(
        Entity(entity_id=entity_id, entity_type=EntityType.FUND_ENTITY, name=name)
    )
    created.add(entity_id)
    return entity_id


def _create_company_entity(
    *,
    store: EntityStore,
    name: str,
    country_iso: str,
    exchange: str,
    ticker: str,
    created_companies: set[str],
) -> str | None:
    try:
        company = Company.from_name(name, country_iso2=country_iso)
    except ValueError:
        logging.debug("Skipping company creation for %s (country iso %s)", ticker, country_iso)
        return None
    company_id = company.canonical_id
    if company_id in created_companies or store.entity_exists(company_id):
        created_companies.add(company_id)
        logging.info("Company %s already exists, skipping metadata insert", company_id)
        return company_id
    metadata = _company_metadata_payload(
        country_iso=country_iso,
        exchange=exchange,
        ticker=ticker,
    )
    store.upsert_entity(
        Entity(entity_id=company_id, entity_type=EntityType.COMPANY, name=name, metadata=metadata)
    )
    created_companies.add(company_id)
    return company_id


def _map_provider_identifier(
    *,
    store: EntityStore,
    provider: str,
    description: str,
    provider_entity_id: str,
    entity_id: str,
    mapped: set[tuple[str, str]],
) -> None:
    key = (provider, provider_entity_id)
    if key in mapped:
        return
    store.upsert_provider(provider, description=description)
    store.map_provider_entity(
        provider=provider,
        provider_entity_id=provider_entity_id,
        entity_id=entity_id,
        active_from=None,
        active_to=None,
        metadata=None,
    )
    mapped.add(key)


def _existing_company_from_identifiers(
    store: EntityStore, identifiers: list[tuple[str, str]]
) -> str | None:
    for provider, provider_entity_id in identifiers:
        if not provider_entity_id:
            continue
        resolved = store.resolve_entity(provider, provider_entity_id)
        if not resolved or resolved.startswith("mic:"):
            continue
        if company := _company_for_security(store, resolved):
            return company
    return None


def _company_for_security(store: EntityStore, security_id: str) -> str | None:
    row = store.connection.execute(
        """
        SELECT src_entity_id
        FROM entity_entity_map
        WHERE dst_entity_id = ? AND relation = ?
        LIMIT 1;
        """,
        (security_id, RELATION_COMPANY_ISSUER),
    ).fetchone()
    return row[0] if row else None


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
    parser.add_argument("--limit", type=int, help="Optional row limit for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    csv_path = _DEFAULT_EQUITIES_CSV
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    cfg = Config()
    store = EntityStore(cfg)
    try:
        rows = load_csv(csv_path, limit=args.limit)
        inserted, skipped = seed_rows(rows, store)
        logging.info("FinanceDatabase equities seeded %d rows (skipped %d)", inserted, skipped)
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
