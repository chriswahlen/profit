from __future__ import annotations

import unittest

from data_sources.region import Region


class RegionIdTests(unittest.TestCase):
    def test_national(self):
        self.assertEqual(Region.national().canonical_id, "region:national:us")
        self.assertEqual(Region.from_fields(region_type="national", region_name="United States").canonical_id, "region:national:us")

    def test_metro(self):
        rid = Region.metro(name="Dallas-Fort Worth, TX", state_code="TX").canonical_id
        self.assertEqual(rid, "region:metro:us:tx:dallas_fort_worth")

    def test_county_strips_state_suffix(self):
        rid = Region.county(name="Maury County, TN", state_code="TN").canonical_id
        self.assertEqual(rid, "region:county:us:tn:maury_county")

    def test_neighborhood_includes_city(self):
        rid = Region.neighborhood(name="Ballard", city="Seattle", state_code="WA").canonical_id
        self.assertEqual(rid, "region:neighborhood:us:wa:seattle:ballard")

    def test_canonical_dispatch(self):
        rid = Region.from_fields(region_type="county", region_name="Fulton County, GA", state_code="GA").canonical_id
        self.assertEqual(rid, "region:county:us:ga:fulton_county")

    def test_missing_state_raises(self):
        with self.assertRaises(ValueError):
            Region.metro(name="DFW", state_code=None)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            Region.county(name="King County", state_code=None)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            Region.neighborhood(name="Ballard", city="Seattle", state_code=None)  # type: ignore[arg-type]

    def test_missing_city_raises(self):
        with self.assertRaises(ValueError):
            Region.neighborhood(name="Ballard", city=None, state_code="WA")  # type: ignore[arg-type]

    def test_empty_strings_raise(self):
        with self.assertRaises(ValueError):
            Region.metro(name="", state_code="TX")
        with self.assertRaises(ValueError):
            Region.county(name="   ", state_code="TX")
        with self.assertRaises(ValueError):
            Region.neighborhood(name="Ballard", city=" ", state_code="WA")
        with self.assertRaises(ValueError):
            Region.neighborhood(name=" ", city="Seattle", state_code="WA")

    def test_unsupported_type_raises(self):
        with self.assertRaises(ValueError):
            Region.from_fields(region_type="province", region_name="Ontario")

    def test_strips_region_prefix(self):
        rid = Region.from_fields(region_type="region:county", region_name="Maury County, TN", state_code="TN").canonical_id
        self.assertEqual(rid, "region:county:us:tn:maury_county")

    def test_state_and_province(self):
        wa_named = Region.state(code="WA", name="Washington")
        self.assertEqual(wa_named.canonical_id, "region:admin1:us:washington")
        self.assertEqual(wa_named.name, "Washington")

        wa_code = Region.state(code="WA")
        self.assertEqual(wa_code.canonical_id, "region:admin1:us:wa")
        self.assertEqual(wa_code.name, "WA")

        ab = Region.province(code="AB", country_iso2="ca", name="Alberta")
        self.assertEqual(ab.canonical_id, "region:admin1:ca:alberta")
        self.assertEqual(ab.name, "Alberta")

    def test_parent_for_county_and_neighborhood(self):
        county = Region.county(name="Maury County, TN", state_code="TN")
        parent = county.parent()
        self.assertEqual(parent.canonical_id, "region:admin1:us:tn")
        self.assertIn("region:state:us:tn", parent.alias_ids())

        nbh = Region.neighborhood(name="Ballard", city="Seattle", state_code="WA")
        parent2 = nbh.parent()
        self.assertEqual(parent2.canonical_id, "region:admin1:us:wa")
        self.assertIn("region:state:us:wa", parent2.alias_ids())

    def test_parent_for_state(self):
        state = Region.state(code="WA")
        self.assertEqual(state.parent().canonical_id, "region:national:us")


if __name__ == "__main__":
    unittest.main()
