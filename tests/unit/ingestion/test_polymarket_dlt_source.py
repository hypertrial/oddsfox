from oddsfox.ingestion.polymarket.dlt_source import (
    normalize_market_payloads_for_dlt,
    polymarket_markets_source,
)
from oddsfox.storage.duckdb.dlt_batch import DLT_STRICT_SCHEMA_CONTRACT


def test_polymarket_markets_source_yields_prefetched_rows():
    rows = [
        {
            "id": "m1",
            "question": "Who will win?",
            "category": "Sports",
            "description": "Winner market",
            "outcomes": '["Yes", "No"]',
            "volume": 100.0,
            "active": True,
            "closed": False,
            "created_at": "2025-01-01 00:00:00",
            "scraped_at": "2025-01-02 00:00:00",
            "end_date": "2026-07-19 00:00:00",
            "slug": "2026-fifa-world-cup-winner",
            "event_slug": "2026-fifa-world-cup-winner",
            "event_id": "99",
        }
    ]
    resource = polymarket_markets_source(rows=rows).resources["markets"]

    assert list(resource) == rows


def test_markets_resource_has_frozen_columns_and_types_contract():
    resource = polymarket_markets_source().resources["markets"]

    assert resource.schema_contract == DLT_STRICT_SCHEMA_CONTRACT
    assert resource.columns["id"]["data_type"] == "text"
    assert resource.columns["volume"]["data_type"] == "double"
    assert resource.columns["created_at"]["data_type"] == "timestamp"


def test_normalize_market_payloads_for_dlt_matches_raw_market_contract():
    rows = normalize_market_payloads_for_dlt(
        [
            {
                "id": "m1",
                "question": "Who will win the 2026 FIFA World Cup?",
                "category": "Sports",
                "description": "Winner market",
                "outcomes": ["Yes", "No"],
                "volumeNum": "12345.67",
                "active": True,
                "closed": False,
                "createdAt": "2025-01-01T00:00:00Z",
                "endDate": "2026-07-19T00:00:00Z",
                "clobTokenIds": ["tok_yes", "tok_no"],
                "slug": "2026-fifa-world-cup-winner",
                "events": [{"id": 99, "slug": "2026-fifa-world-cup-winner"}],
            }
        ]
    )

    assert rows == [
        {
            "id": "m1",
            "question": "Who will win the 2026 FIFA World Cup?",
            "category": "Sports",
            "description": "Winner market",
            "outcomes": '["Yes", "No"]',
            "volume": 12345.67,
            "active": True,
            "closed": False,
            "created_at": "2025-01-01 00:00:00",
            "scraped_at": rows[0]["scraped_at"],
            "end_date": "2026-07-19 00:00:00",
            "slug": "2026-fifa-world-cup-winner",
            "event_slug": "2026-fifa-world-cup-winner",
            "event_id": "99",
        }
    ]


def test_normalize_market_payloads_for_dlt_dedupes_by_id_last_wins():
    base = {
        "category": "Sports",
        "description": "Winner market",
        "outcomes": ["Yes", "No"],
        "volumeNum": "100.0",
        "active": True,
        "closed": False,
        "createdAt": "2025-01-01T00:00:00Z",
        "endDate": "2026-07-19T00:00:00Z",
        "clobTokenIds": ["tok_yes", "tok_no"],
        "slug": "2026-fifa-world-cup-winner",
        "events": [{"id": 99, "slug": "2026-fifa-world-cup-winner"}],
    }
    rows = normalize_market_payloads_for_dlt(
        [
            {"id": "dup", "question": "old question", **base},
            {"id": "dup", "question": "new question", **base},
        ]
    )

    assert len(rows) == 1
    assert rows[0]["id"] == "dup"
    assert rows[0]["question"] == "new question"
