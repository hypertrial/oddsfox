"""Cover every column branch in prepare_batch_for_db."""

from datetime import datetime, timezone

import polars as pl
import pytest

from oddsfox.ingestion.polymarket.markets.persistence import (
    prepare_batch_for_db,
)


def _dt():
    return datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"id": ["a"]},
        {"id": ["a"], "question": ["q"]},
        {"id": ["a"], "question": ["q"], "category": ["c"]},
        {"id": ["a"], "question": ["q"], "category": ["c"], "description": ["d"]},
        {
            "id": ["a"],
            "question": ["q"],
            "category": ["c"],
            "description": ["d"],
            "outcomes_str": ["[]"],
        },
        {
            "id": ["a"],
            "question": ["q"],
            "category": ["c"],
            "description": ["d"],
            "outcomes_str": ["[]"],
            "volume": [3.0],
        },
        {
            "id": ["a"],
            "question": ["q"],
            "category": ["c"],
            "description": ["d"],
            "outcomes_str": ["[]"],
            "volumeNum": [4.0],
        },
        {
            "id": ["a"],
            "question": ["q"],
            "category": ["c"],
            "description": ["d"],
            "outcomes_str": ["[]"],
            "volumeNum": [1.0],
            "active": [True],
            "closed": [False],
            "created_at": [_dt()],
            "end_date": [_dt()],
            "slug": [None],
            "event_slug": [None],
            "clobTokenIds_str": ['["x"]'],
        },
    ],
)
def test_prepare_batch_column_fallbacks(kwargs):
    df = pl.DataFrame(kwargs)
    m, t = prepare_batch_for_db(df)
    assert isinstance(m, list)
    assert isinstance(t, list)


def test_prepare_batch_volume_branch_prefers_volume_num():
    df = pl.DataFrame(
        {
            "id": ["1"],
            "question": ["q"],
            "category": ["c"],
            "description": ["d"],
            "outcomes_str": ["[]"],
            "volumeNum": [10.0],
            "volume": [99.0],
            "active": [True],
            "closed": [False],
            "created_at": [_dt()],
            "end_date": [_dt()],
            "slug": ["s"],
            "event_slug": ["e"],
            "clobTokenIds_str": ['["t"]'],
        }
    )
    m, t = prepare_batch_for_db(df)
    assert m[0][5] == 10.0


def test_prepare_batch_missing_id_column():
    df = pl.DataFrame({"question": ["q"], "category": ["c"]})
    m, t = prepare_batch_for_db(df)
    assert m[0][0] == ""


def test_prepare_batch_skips_empty_token_strings():
    df = pl.DataFrame(
        {
            "id": ["1"],
            "question": ["q"],
            "category": ["c"],
            "description": ["d"],
            "outcomes_str": ["[]"],
            "volumeNum": [1.0],
            "active": [True],
            "closed": [False],
            "created_at": [_dt()],
            "end_date": [_dt()],
            "slug": [None],
            "event_slug": [None],
            "clobTokenIds_str": [""],
        }
    )
    m, t = prepare_batch_for_db(df)
    assert m
    assert t == []
