"""Unit tests for WC 2026 Gamma tag discovery."""

from __future__ import annotations

from unittest.mock import MagicMock

import requests

from oddsfox.ingestion.polymarket.errors import GammaRequestError
from oddsfox.ingestion.polymarket.wc2026_tags import (
    discover_wc2026_tag_slugs,
    fetch_gamma_sports,
    fetch_gamma_tag_by_slug,
    fetch_gamma_tags,
    tag_matches_keywords,
)


def test_tag_matches_keywords_world_cup():
    assert tag_matches_keywords(
        {"label": "FIFA World Cup", "slug": "fifa-world-cup"},
        ("fifa", "world cup"),
    )
    assert not tag_matches_keywords(
        {"label": "Premier League", "slug": "epl"},
        ("fifa", "world cup"),
    )


def test_fetch_gamma_tag_by_slug_returns_payload():
    client = MagicMock()
    client.get.return_value = {
        "id": "102232",
        "slug": "fifa-world-cup",
        "label": "FIFA World Cup",
    }
    tag = fetch_gamma_tag_by_slug(client, "fifa-world-cup")
    assert tag is not None
    assert tag["id"] == "102232"


def test_tag_matches_keywords_wc2026_specific_terms():
    assert tag_matches_keywords(
        {"label": "World Cup Qualifiers", "slug": "world-cup-qualifiers"},
        ("world-cup-qualifiers",),
    )
    assert tag_matches_keywords(
        {"label": "WC 2026", "slug": "wc-2026"},
        ("wc-2026",),
    )
    assert not tag_matches_keywords(
        {"label": "Premier League", "slug": "epl"},
        ("world-cup-qualifiers", "wc-2026"),
    )


def test_discover_wc2026_tag_slugs_unions_seed_and_list():
    client = MagicMock()

    def _get(endpoint, **kwargs):
        if endpoint == "/tags/slug/fifa-world-cup":
            return {"id": "102232", "slug": "fifa-world-cup", "label": "FIFA World Cup"}
        if endpoint == "/tags":
            return [
                {"id": "519", "slug": "world-cup", "label": "world cup"},
                {"id": "999", "slug": "epl", "label": "Premier League"},
            ]
        if endpoint == "/sports":
            return [{"id": 1, "sport": "wc", "tags": "519,999"}]
        return []

    client.get.side_effect = _get
    result = discover_wc2026_tag_slugs(
        client,
        seed_slugs=["fifa-world-cup"],
        keywords=("world cup", "fifa"),
    )
    assert "fifa-world-cup" in result.tag_slugs
    assert "world-cup" in result.tag_slugs
    assert "epl" not in result.tag_slugs
    assert result.sources["world-cup"] == ("sports", "tags_list")


def test_tag_matches_keywords_rejects_empty_blob():
    assert not tag_matches_keywords({}, ("fifa",))


def test_fetch_gamma_tag_by_slug_missing_returns_none():
    client = MagicMock()
    client.get.return_value = {}
    assert fetch_gamma_tag_by_slug(client, "fifa-world-cup") is None


def test_fetch_gamma_tag_by_slug_404_returns_none():
    client = MagicMock()
    response = MagicMock(status_code=404)
    client.get.side_effect = GammaRequestError(response=response)
    assert fetch_gamma_tag_by_slug(client, "missing-tag") is None


def test_fetch_gamma_tags_and_sports():
    client = MagicMock()

    def _get(endpoint, **kwargs):
        if endpoint == "/tags":
            return [{"id": "1", "slug": "fifa-world-cup", "label": "FIFA"}]
        if endpoint == "/sports":
            return [{"tags": "1,2"}, {"tags": ["3"]}]
        return []

    client.get.side_effect = _get
    tags = fetch_gamma_tags(client, limit=50)
    sports = fetch_gamma_sports(client)
    assert tags[0]["slug"] == "fifa-world-cup"
    assert len(sports) == 2


def test_discover_tolerates_api_failures():
    client = MagicMock()
    client.get.side_effect = requests.RequestException("network down")
    result = discover_wc2026_tag_slugs(client, seed_slugs=["fifa-world-cup"])
    assert result.tag_slugs == ("fifa-world-cup",)


def test_discover_skips_invalid_seed_slug():
    client = MagicMock()
    client.get.return_value = []
    result = discover_wc2026_tag_slugs(client, seed_slugs=["bad slug!"])
    assert result.tag_slugs == ()
