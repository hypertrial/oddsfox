from __future__ import annotations

from typing import Any, Mapping

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator

from oddsfox.orchestration.dbt_project import DBT_DAGSTER_GROUP_NAME
from oddsfox.storage.duckdb.schemas.dbt_schemas import dbt_model_asset_key


class PolymarketDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        return dbt_model_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return DBT_DAGSTER_GROUP_NAME

    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        props = self.get_resource_props(manifest, unique_id)
        if props.get("resource_type") == "model":
            return spec.merge_attributes(
                deps=[AssetKey("polymarket_token_odds_history")]
            )
        return spec


__all__ = ["PolymarketDagsterDbtTranslator"]
