#!/usr/bin/env python3
"""Seed exchange (market venue) entities from a hard-coded list.

Canonical IDs: mic:<mic_lower>
EntityType: MARKET_VENUE

Also maps FinanceDatabase exchange codes to canonical MICs via provider_entity_map
using provider:financedatabase.
"""

from __future__ import annotations

import argparse
import logging
import json
from dataclasses import dataclass
from typing import Iterable

from config import Config
from data_sources.entity import Entity, EntityStore, EntityType


@dataclass(frozen=True)
class ExchangeEntry:
    mic: str
    name: str
    country: str | None = None
    currency: str | None = None
    fd_codes: tuple[str, ...] = ()


# Reuse the mapping we use in FinanceDatabase seeding; add display names.
EXCHANGES: list[ExchangeEntry] = [
    ExchangeEntry("XNAS", "NASDAQ", "US", "USD", ("NMS", "NGM", "NCM", "NAS", "XNAS")),
    ExchangeEntry("XNYS", "NYSE", "US", "USD", ("NYQ", "NYS", "XNYS")),
    ExchangeEntry("XASE", "NYSE American", "US", "USD", ("ASE", "XASE")),
    ExchangeEntry("ARCX", "NYSE Arca", "US", "USD", ("PCX", "ARCX")),
    ExchangeEntry("BATS", "Cboe BZX", "US", "USD", ("BATS",)),
    ExchangeEntry("IEXG", "Investors Exchange", "US", "USD", ("IEX",)),
    ExchangeEntry("OTCM", "OTC Markets / Pink", "US", "USD", ("PNK", "OTC")),
    ExchangeEntry("XTSE", "Toronto Stock Exchange", "CA", "CAD", ("TOR", "TSE", "TSX")),
    ExchangeEntry("XTSX", "TSX Venture Exchange", "CA", "CAD", ("VAN",)),
    ExchangeEntry("XLON", "London Stock Exchange", "GB", "GBP", ("LSE",)),
    ExchangeEntry("XFRA", "Deutsche Börse Frankfurt", "DE", "EUR", ("FRA",)),
    ExchangeEntry("XSTU", "Stuttgart Stock Exchange", "DE", "EUR", ("STU",)),
    ExchangeEntry("XBER", "Berlin Stock Exchange", "DE", "EUR", ("BER",)),
    ExchangeEntry("XMUN", "Munich Stock Exchange", "DE", "EUR", ("MUN",)),
    ExchangeEntry("XDUS", "Düsseldorf Stock Exchange", "DE", "EUR", ("DUS",)),
    ExchangeEntry("XHAM", "Hamburg Stock Exchange", "DE", "EUR", ("HAM",)),
    ExchangeEntry("XETR", "Xetra", "DE", "EUR", ("GER",)),
    ExchangeEntry("XWBO", "Wiener Börse", "AT", "EUR", ("VIE",)),
    ExchangeEntry("XSWX", "SIX Swiss Exchange", "CH", "CHF", ("EBS", "SWX")),
    ExchangeEntry("XVTX", "SIX Swiss Exchange (Virt-X)", "CH", "CHF", ("VTX",)),
    ExchangeEntry("XPAR", "Euronext Paris", "FR", "EUR", ("PAR",)),
    ExchangeEntry("XMIL", "Borsa Italiana", "IT", "EUR", ("MIL",)),
    ExchangeEntry("XMAD", "Bolsa de Madrid", "ES", "EUR", ("MCE",)),
    ExchangeEntry("XCSE", "Copenhagen Stock Exchange", "DK", "DKK", ("CSE",)),
    ExchangeEntry("XSTO", "Nasdaq Stockholm", "SE", "SEK", ("STO",)),
    ExchangeEntry("XHEL", "Nasdaq Helsinki", "FI", "EUR", ("HEL",)),
    ExchangeEntry("XICE", "Nasdaq Iceland", "IS", "ISK", ("ICE",)),
    ExchangeEntry("XOSL", "Oslo Børs", "NO", "NOK", ("OSL",)),
    ExchangeEntry("XTKS", "Tokyo Stock Exchange", "JP", "JPY", ("JPX", "TKS", "TKO", "TSE")),
    ExchangeEntry("XOSE", "Osaka Exchange", "JP", "JPY", ("OSE",)),
    ExchangeEntry("XNGO", "Nagoya Stock Exchange", "JP", "JPY", ("NGO",)),
    ExchangeEntry("XBOM", "BSE India", "IN", "INR", ("BSE",)),
    ExchangeEntry("XNSE", "National Stock Exchange of India", "IN", "INR", ("NSE",)),
    ExchangeEntry("XHKG", "Hong Kong Stock Exchange", "HK", "HKD", ("HKG",)),
    ExchangeEntry("XSHE", "Shenzhen Stock Exchange", "CN", "CNY", ("SHZ", "SHE")),
    ExchangeEntry("XSHG", "Shanghai Stock Exchange", "CN", "CNY", ("SHH", "SHG")),
    ExchangeEntry("XKOS", "KOSDAQ", "KR", "KRW", ("KSC", "KOS")),
    ExchangeEntry("XKRX", "Korea Exchange", "KR", "KRW", ("KOE",)),
    ExchangeEntry("XASX", "ASX", "AU", "AUD", ("ASX",)),
    ExchangeEntry("XNZE", "NZX", "NZ", "NZD", ("NZX",)),
    ExchangeEntry("XSES", "Singapore Exchange", "SG", "SGD", ("SGX", "SES")),
    ExchangeEntry("XBKK", "Stock Exchange of Thailand", "TH", "THB", ("SET",)),
    ExchangeEntry("XJSE", "Johannesburg Stock Exchange", "ZA", "ZAR", ("JNB",)),
    ExchangeEntry("XMEX", "Bolsa Mexicana de Valores", "MX", "MXN", ("MEX",)),
    ExchangeEntry("BVMF", "B3 - Brasil Bolsa Balcão", "BR", "BRL", ("SAO",)),
    ExchangeEntry("XIST", "Borsa Istanbul", "TR", "TRY", ("IST",)),
    ExchangeEntry("XPHS", "Philippine Stock Exchange", "PH", "PHP", ("PSE",)),
    ExchangeEntry("XTAI", "Taiwan Stock Exchange", "TW", "TWD", ("TAI",)),
    ExchangeEntry("ROCO", "Taipei Exchange (OTC)", "TW", "TWD", ("TWO",)),
    ExchangeEntry("XBCBA", "Buenos Aires Stock Exchange", "AR", "ARS", ("BUE",)),
    ExchangeEntry("AQSE", "Aquis Stock Exchange", "GB", "GBP", ("AQS",)),
    ExchangeEntry("MOEX", "Moscow Exchange", "RU", "RUB", ("MCX",)),
    ExchangeEntry("XCAI", "Cairo Exchange", "EG", "EGP", ("CAI",)),
    ExchangeEntry("XAMS", "Euronext Amsterdam", "NL", "EUR", ("AMS",)),
    ExchangeEntry("XBRU", "Euronext Brussels", "BE", "EUR", ("BRU",)),
    ExchangeEntry("XATH", "Athens Stock Exchange", "GR", "EUR", ("ATH",)),
    ExchangeEntry("XSAU", "Saudi Exchange (Tadawul)", "SA", "SAR", ("SAU",)),
    ExchangeEntry("XLIS", "Euronext Lisbon", "PT", "EUR", ("LIS",)),
    ExchangeEntry("XCAS", "Casablanca Stock Exchange", "MA", "MAD", ("CCS",)),
    ExchangeEntry("XQAT", "Qatar Stock Exchange", "QA", "QAR", ("DOH",)),
    ExchangeEntry("XBUD", "Budapest Stock Exchange", "HU", "HUF", ("BUD",)),
    ExchangeEntry("XPRA", "Prague Stock Exchange", "CZ", "CZK", ("PRA",)),
    ExchangeEntry("XDUB", "Euronext Dublin (Irish Stock Exchange)", "IE", "EUR", ("ISE",)),
    ExchangeEntry("NEOE", "NEO Exchange", "CA", "CAD", ("NEO",)),
    ExchangeEntry("XLIT", "Cboe Europe LIT", "GB", "GBP", ("LIT",)),
    ExchangeEntry("XRIS", "Nasdaq Riga", "LV", "EUR", ("RIS",)),
    ExchangeEntry("XTAL", "Nasdaq Tallinn", "EE", "EUR", ("TAL", "TLO")),
    ExchangeEntry("XBTS", "Bratislava Stock Exchange", "SK", "EUR", ("BTS",)),
    ExchangeEntry("XNSA", "Nigerian Exchange", "NG", "NGN", ("NSI",)),
    ExchangeEntry("XNAM", "Namibian Stock Exchange", "NA", "NAD", ("NAE",)),
]


def seed_exchanges(entries: Iterable[ExchangeEntry], store: EntityStore) -> tuple[int, int]:
    inserted = skipped = 0
    # map FinanceDatabase exchange codes to canonical MICs
    store.upsert_provider("provider:financedatabase", description="FinanceDatabase symbols")
    store.ensure_relation_type("traded_in", description="Exchange trades in currency")

    for ex in entries:
        eid = f"mic:{ex.mic.lower()}"
        entity = Entity(
            entity_id=eid,
            entity_type=EntityType.MARKET_VENUE,
            name=ex.name,
            metadata=json.dumps(
                {k: v for k, v in {"country": ex.country, "currency": ex.currency}.items() if v},
                ensure_ascii=True,
            ),
        )
        store.upsert_entity(entity)
        inserted += 1

        for fd_code in ex.fd_codes:
            store.map_provider_entity(
                provider="provider:financedatabase",
                provider_entity_id=fd_code,
                entity_id=eid,
                active_from=None,
                active_to=None,
                metadata=None,
            )
        # Link exchange -> currency entity if currency provided
        if ex.currency:
            ccy_id = f"ccy:{ex.currency.lower()}"
            if not store.entity_exists(ccy_id):
                store.upsert_entity(
                    Entity(entity_id=ccy_id, entity_type=EntityType.CURRENCY, name=ex.currency.upper())
                )
            store.map_entity_relation(
                src_entity_id=eid,
                dst_entity_id=ccy_id,
                relation="traded_in",
            )
    return inserted, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed exchange (market venue) entities")
    parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    cfg = Config()
    store = EntityStore(cfg)
    try:
        inserted, skipped = seed_exchanges(EXCHANGES, store)
        logging.info("Seeded %d exchanges (skipped %d)", inserted, skipped)
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
