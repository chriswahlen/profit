from __future__ import annotations

import unittest

from data_sources.entities import Currency, Company


class EntityHelpersTests(unittest.TestCase):
    def test_currency_canonical(self):
        self.assertEqual(Currency.from_code("USD").canonical_id, "ccy:usd")
        with self.assertRaises(ValueError):
            Currency.from_code("")

    def test_company_canonical(self):
        cid = Company.from_name("Neo Aeronautics", country_iso2="US").canonical_id
        self.assertEqual(cid, "us:com:neo-aeronautics")

    def test_company_requires_fields(self):
        with self.assertRaises(ValueError):
            Company.from_name("", country_iso2="US")
        with self.assertRaises(ValueError):
            Company.from_name("Acme", country_iso2="")


if __name__ == "__main__":
    unittest.main()
