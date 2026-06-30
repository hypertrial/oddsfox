"""Branch coverage for nested JSON helpers used by process_markets_dataframe."""

import polars as pl

from oddsfox.ingestion.polymarket.markets.transform import (
    _jsonify_nested_value,
    _normalize_nested_value,
)


def test_normalize_nested_value_series_tuple_and_scalar():
    assert _normalize_nested_value(pl.Series([1, 2])) == [1, 2]
    assert _normalize_nested_value((1, 2)) == [1, 2]
    assert _normalize_nested_value("plain") == "plain"


def test_jsonify_nested_value_list_and_scalar():
    assert _jsonify_nested_value([1, 2]) == "[1, 2]"
    assert _jsonify_nested_value(99) == "99"
