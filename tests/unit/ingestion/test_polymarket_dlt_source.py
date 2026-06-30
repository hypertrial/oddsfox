from oddsfox.ingestion.polymarket.dlt_source import normalize_market_payloads_for_dlt


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
