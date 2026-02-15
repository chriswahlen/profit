#!/usr/bin/env python3
"""
Seed canonical regions (countries and states/provinces) into the regions store and entity store.

Requires `pycountry` for ISO country/subdivision data:
    pip install pycountry
"""

from __future__ import annotations

import logging
from typing import Iterable

import pycountry

from config import Config
from data_sources.entity import EntityStore, Entity, EntityType
from data_sources.region import Region


def seed_regions(
    *,
    config: Config,
    countries: Iterable[str] | None = None,
    provider: str = "seed",
) -> None:
    entity_store = EntityStore(config)
    inserted = 0

    for country in _iter_countries(countries):
        country_obj = pycountry.countries.get(alpha_2=country)
        country_name = country_obj.name if country_obj else country
        national = Region.national(country_iso2=country, name=country_name)
        _upsert_region(
            entity_store,
            national,
            provider,
            provider_region_id=country,
            metadata={
                "iso_alpha2": country,
                "iso_numeric": getattr(country_obj, "numeric", None),
                "source": "pycountry/ISO3166-1",
            },
        )
        inserted += 1

        # Subdivisions (states/provinces/oblasts/etc.)
        for subdiv in pycountry.subdivisions.get(country_code=country):
            code = subdiv.code.split("-")[-1]
            reg_type = _map_subdivision_type(country, subdiv.type)
            region = Region.from_fields(
                region_type=reg_type,
                region_name=subdiv.name,
                country_iso2=country,
                state_code=code,
            )
            _upsert_region(
                entity_store,
                region,
                provider,
                provider_region_id=subdiv.code,
                parent_region_id=national.canonical_id,
                metadata={
                    "iso_code_full": subdiv.code,
                    "iso_code": code,
                    "iso_type": subdiv.type,
                    "source": "pycountry/ISO3166-2",
                },
            )
            inserted += 1

    logging.info("Seeded %d regions (entities only)", inserted)


def _iter_countries(allow_list: Iterable[str] | None) -> Iterable[str]:
    if allow_list:
        return [c.upper() for c in allow_list]
    return [c.alpha_2.upper() for c in pycountry.countries]


def _map_subdivision_type(country: str, subdiv_type: str) -> str:
    t = (subdiv_type or "").lower()
    if country.upper() == "US" and t in {"state", "district", "territory"}:
        return "state"
    if country.upper() == "CA" and t in {"province", "territory"}:
        return "province"
    # Default: treat any subdivision as province-level
    return "province"


def _upsert_region(
    entity_store: EntityStore,
    region: Region,
    provider: str,
    provider_region_id: str,
    parent_region_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    entity_store.upsert_entity(
        Entity(
            entity_id=region.canonical_id,
            entity_type=EntityType.REGION,
            name=region.name,
            metadata="" if metadata is None else str(metadata),
        )
    )
    entity_store.map_provider_entity(
        provider=provider,
        provider_entity_id=provider_region_id,
        entity_id=region.canonical_id,
        active_from="1970-01-01",
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = Config()
    seed_regions(config=cfg)


if __name__ == "__main__":
    main()
