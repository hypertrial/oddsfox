"""dbt-modeled DuckDB schema names and Dagster asset-key helpers."""

from __future__ import annotations

from typing import Final, Mapping, Sequence

from dagster import AssetKey

DBT_SOURCE_POLYMARKET: Final = "polymarket"

POLYMARKET_STAGING_SCHEMA: Final = "polymarket_staging"
POLYMARKET_INTERMEDIATE_SCHEMA: Final = "polymarket_intermediate"
POLYMARKET_MARTS_SCHEMA: Final = "polymarket_marts"
POLYMARKET_OBSERVABILITY_SCHEMA: Final = "polymarket_observability"
DBT_FALLBACK_SCHEMA: Final = "dbt"

DBT_MODELED_SCHEMAS: Final[tuple[str, ...]] = (
    POLYMARKET_STAGING_SCHEMA,
    POLYMARKET_INTERMEDIATE_SCHEMA,
    POLYMARKET_MARTS_SCHEMA,
    POLYMARKET_OBSERVABILITY_SCHEMA,
)


def qualified_relation(schema: str, model_name: str) -> str:
    return f"{schema}.{model_name}"


def resolve_source_slug(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> str:
    tags = set(props.get("tags") or ())
    path_fqn = list(fqn or props.get("fqn") or ())
    if "polymarket" in tags or (len(path_fqn) >= 2 and path_fqn[1] == "polymarket"):
        return DBT_SOURCE_POLYMARKET
    name = str(props.get("name") or "")
    if name.startswith(("stg_polymarket_", "int_polymarket_")) or name in {
        "market_coverage",
        "token_coverage",
        "wc2026_token_minutely_odds",
        "wc2026_token_daily_odds",
        "wc2026_markets",
        "wc2026_whale_minutely_odds",
        "sync_run_observability",
    }:
        return DBT_SOURCE_POLYMARKET
    return DBT_FALLBACK_SCHEMA


def shorten_model_name(model_name: str, source_slug: str) -> str:
    if source_slug == DBT_SOURCE_POLYMARKET:
        for layer, prefix in (
            ("stg", "stg_polymarket_"),
            ("int", "int_polymarket_"),
        ):
            if model_name.startswith(prefix):
                return f"{layer}_{model_name[len(prefix) :]}"
        if model_name.startswith("polymarket_"):
            return model_name[len("polymarket_") :]
    return model_name


def dbt_model_asset_key(
    props: Mapping[str, object],
    *,
    fqn: Sequence[str] | None = None,
) -> AssetKey:
    source = resolve_source_slug(props, fqn=fqn)
    name = str(props.get("name") or "")
    return AssetKey(f"{source}_{shorten_model_name(name, source)}")


def dbt_model_asset_key_for_name(model_name: str, source_slug: str) -> AssetKey:
    return AssetKey(f"{source_slug}_{shorten_model_name(model_name, source_slug)}")


__all__ = [
    "DBT_FALLBACK_SCHEMA",
    "DBT_MODELED_SCHEMAS",
    "DBT_SOURCE_POLYMARKET",
    "POLYMARKET_INTERMEDIATE_SCHEMA",
    "POLYMARKET_MARTS_SCHEMA",
    "POLYMARKET_OBSERVABILITY_SCHEMA",
    "POLYMARKET_STAGING_SCHEMA",
    "dbt_model_asset_key",
    "dbt_model_asset_key_for_name",
    "qualified_relation",
    "resolve_source_slug",
    "shorten_model_name",
]
