from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from config import Config
from data_sources.entity import EntityStore
from scripts.seed_cryptos import rows_from_csv, seed_rows, PROVIDER


class CryptoSeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        os.environ["PROFIT_DATA_PATH"] = str(Path(self.tmpdir.name) / "data")
        self.cfg = Config()
        self.store = EntityStore(self.cfg)

    def tearDown(self) -> None:
        os.environ.pop("PROFIT_DATA_PATH", None)
        self.tmpdir.cleanup()

    def test_seed_rows_creates_canonical_crypto_entities(self) -> None:
        csv_path = Path(self.tmpdir.name) / "cryptos.csv"
        csv_path.write_text(
            "symbol,name,cryptocurrency,currency,summary,exchange\n"
            "AAVE-USD,Aave USD,AAVE,USD,Aave (AAVE) is a decentralized finance protocol.,CCC\n"
            "BTC-USD,Bitcoin USD,BTC,USD,Bitcoin (BTC) is a decentralized asset.,CCC\n"
            "AAVE-EUR,Aave EUR,AAVE,EUR,Aave (AAVE) is a decentralized finance protocol. (EUR),CCC\n"
        )

        rows = list(rows_from_csv(csv_path))
        processed, skipped, unique = seed_rows(rows, self.store, progress_interval=1)

        self.assertEqual(processed, 3)
        self.assertEqual(skipped, 0)
        self.assertEqual(unique, 2)

        cur = self.store.connection.execute(
            "SELECT entity_id, name, metadata FROM entities WHERE entity_id LIKE 'crypto:%' ORDER BY entity_id;"
        )
        entries = cur.fetchall()
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0][0], "crypto:aave")
        self.assertEqual(entries[1][0], "crypto:btc")
        self.assertEqual(entries[0][1], "Aave")
        self.assertEqual(entries[1][1], "Bitcoin")

        metadata = json.loads(entries[0][2])
        self.assertEqual(metadata, {"summary": "Aave (AAVE) is a decentralized finance protocol. (EUR)"})

        metadata_btc = json.loads(entries[1][2])
        self.assertEqual(metadata_btc, {"summary": "Bitcoin (BTC) is a decentralized asset."})

        provider_rows = self.store.connection.execute(
            "SELECT COUNT(*) FROM provider_entity_map WHERE provider=?;",
            (PROVIDER,),
        ).fetchone()[0]
        self.assertEqual(provider_rows, 0)

    def test_skips_rows_without_symbol_and_crypto(self) -> None:
        rows = [
            {"symbol": "", "cryptocurrency": "", "name": "", "currency": "", "summary": "", "exchange": ""},
            {"symbol": "", "cryptocurrency": "", "name": "", "currency": "USD", "summary": "", "exchange": "CCC"},
            {"symbol": "ETH-USD", "cryptocurrency": "", "name": "Ethereum USD", "currency": "", "summary": "Ethereum (ETH) is a smart contract platform.", "exchange": ""},
        ]
        processed, skipped, unique = seed_rows(rows, self.store)
        self.assertEqual(processed, 1)
        self.assertEqual(skipped, 2)
        self.assertEqual(unique, 1)
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM entities WHERE entity_id LIKE 'crypto:%';").fetchone()[0],
            1,
        )


if __name__ == "__main__":
    unittest.main()
