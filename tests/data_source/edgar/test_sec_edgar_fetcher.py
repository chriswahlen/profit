from __future__ import annotations

import json
import unittest

from data_sources.edgar.http import FetchResponse
from data_sources.edgar.sec_edgar import EdgarSubmissionsFetcher


class EdgarSubmissionsFetcherTests(unittest.TestCase):
    def test_submissions_fetch_parses_recent_filings(self) -> None:
        requested_urls: list[str] = []
        sent_headers: list[dict[str, str]] = []

        def fake_fetch(url: str, *, timeout: float, headers: dict[str, str]):
            requested_urls.append(url)
            sent_headers.append(headers)
            payload = {
                "cik": "0000320193",
                "name": "Apple Inc.",
                "filings": {
                    "recent": {
                        "accessionNumber": ["0000320193-24-000001"],
                        "form": ["10-K"],
                        "filingDate": ["2024-11-03"],
                        "reportDate": ["2024-09-28"],
                        "primaryDocument": ["a10-k2024.htm"],
                    }
                },
            }
            return FetchResponse(status=200, body=json.dumps(payload).encode(), headers={})

        fetcher = EdgarSubmissionsFetcher(fetch_fn=fake_fetch, user_agent="test-agent/1.0")
        result = fetcher.fetch("320193")

        self.assertEqual(requested_urls, ["https://data.sec.gov/submissions/CIK0000320193.json"])
        self.assertEqual(sent_headers[0]["User-Agent"], "test-agent/1.0")
        self.assertEqual(result.cik, "0000320193")
        self.assertEqual(result.entity_name, "Apple Inc.")
        self.assertEqual(len(result.recent_filings), 1)
        filing = result.recent_filings[0]
        self.assertEqual(filing.accession_number, "0000320193-24-000001")
        self.assertEqual(filing.form, "10-K")
        self.assertEqual(filing.filing_date.isoformat(), "2024-11-03")
        self.assertEqual(filing.report_date.isoformat(), "2024-09-28")
        self.assertEqual(filing.primary_document, "a10-k2024.htm")

    def test_submissions_fetch_follows_paged_files(self) -> None:
        requested_urls: list[str] = []

        main_payload = {
            "cik": "0000000001",
            "name": "Test Co",
            "filings": {
                "recent": {
                    "accessionNumber": ["0000000001-24-000003", "0000000001-24-000002"],
                    "form": ["10-K", "10-Q"],
                    "filingDate": ["2024-12-31", "2024-09-30"],
                    "reportDate": ["2024-09-28", "2024-06-30"],
                    "primaryDocument": ["k.htm", "q.htm"],
                },
                "files": [{"name": "CIK0000000001-001.json"}],
            },
        }

        page_payload = {
            "filings": {
                "recent": {
                    "accessionNumber": ["0000000001-23-000001"],
                    "form": ["10-K"],
                    "filingDate": ["2023-12-31"],
                    "reportDate": ["2023-09-28"],
                    "primaryDocument": ["k2023.htm"],
                }
            }
        }

        def fake_fetch(url: str, *, timeout: float, headers: dict[str, str]):
            requested_urls.append(url)
            if url.endswith("CIK0000000001.json"):
                body = json.dumps(main_payload).encode()
            elif url.endswith("CIK0000000001-001.json"):
                body = json.dumps(page_payload).encode()
            else:
                raise AssertionError(f"unexpected url {url}")
            return FetchResponse(status=200, body=body, headers={})

        fetcher = EdgarSubmissionsFetcher(fetch_fn=fake_fetch, user_agent="test-agent/1.0")
        result = fetcher.fetch("1")

        self.assertEqual(
            requested_urls,
            [
                "https://data.sec.gov/submissions/CIK0000000001.json",
                "https://data.sec.gov/submissions/CIK0000000001-001.json",
            ],
        )
        self.assertEqual(len(result.recent_filings), 3)
        accessions = [f.accession_number for f in result.recent_filings]
        self.assertEqual(
            accessions,
            [
                "0000000001-24-000003",
                "0000000001-24-000002",
                "0000000001-23-000001",
            ],
        )


if __name__ == "__main__":
    unittest.main()

