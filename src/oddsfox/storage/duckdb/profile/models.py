"""Dataclasses and configuration for DuckDB warehouse profiling."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Final, Optional, Set

import duckdb

from oddsfox.storage.duckdb.schemas.dbt_schemas import DBT_MODELED_SCHEMAS


@dataclass
class ColumnSpec:
    name: str
    ordinal_position: int
    data_type: str
    is_nullable: str
    column_default: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ColumnStats:
    non_null_count: Optional[int] = None
    null_count: Optional[int] = None
    null_percent: Optional[float] = None
    approx_distinct: Optional[int] = None
    min_value: Any = None
    max_value: Any = None
    avg_value: Any = None
    stddev_value: Any = None
    true_count: Optional[int] = None
    false_count: Optional[int] = None
    min_len: Optional[int] = None
    max_len: Optional[int] = None
    avg_len: Optional[float] = None
    sample_value: Any = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ColumnProfile:
    spec: ColumnSpec
    stats: Optional[ColumnStats] = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"spec": self.spec.to_dict()}
        if self.stats is not None:
            out["stats"] = self.stats.to_dict()
        return out


@dataclass
class RelationInfo:
    table_schema: str
    table_name: str
    table_type: str  # BASE TABLE, VIEW, etc.

    @property
    def qualified_name(self) -> str:
        from .discovery import qualified_name

        return qualified_name(self.table_schema, self.table_name)


@dataclass
class RelationProfile:
    table_schema: str
    table_name: str
    table_type: str
    row_count: Optional[int] = None
    column_count: int = 0
    is_empty: Optional[bool] = None
    columns: list[ColumnProfile] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "table_schema": self.table_schema,
            "table_name": self.table_name,
            "table_type": self.table_type,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "is_empty": self.is_empty,
            "columns": [c.to_dict() for c in self.columns],
        }
        if self.error is not None:
            d["error"] = self.error
        return d


@dataclass
class RefreshStepResult:
    step: str
    ok: bool
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ReportMetadata:
    generated_at_utc: str
    duckdb_path: str
    profile_config: dict[str, Any] = field(default_factory=dict)
    refresh_steps: list[RefreshStepResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at_utc": self.generated_at_utc,
            "duckdb_path": self.duckdb_path,
            "profile_config": self.profile_config,
            "refresh_steps": [r.to_dict() for r in self.refresh_steps],
        }


@dataclass
class WarehouseProfileReport:
    metadata: ReportMetadata
    relations: list[RelationProfile] = field(default_factory=list)
    report_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "relations": [r.to_dict() for r in self.relations],
            "report_errors": list(self.report_errors),
        }

    def as_json(self, *, indent: int = 2, default: Any = str) -> str:
        return json.dumps(
            self.to_dict(),
            indent=indent,
            default=default,
            sort_keys=False,
        )


class StatsLevel(str, Enum):
    quick = "quick"
    standard = "standard"
    full = "full"


class OutputFormat(str, Enum):
    markdown = "markdown"
    json = "json"
    both = "both"


DEFAULT_SCHEMAS: Final[tuple[str, ...]] = (
    "polymarket_raw",
    "polymarket_ops",
    *DBT_MODELED_SCHEMAS,
)

_SYSTEM_SCHEMAS: Final[Set[str]] = {"information_schema", "pg_catalog"}

_PROFILE_QUERY_ERRORS = (duckdb.Error, OSError, RuntimeError)


@dataclass
class ProfileConfig:
    """Configuration for a profiling run (non-CLI, library use)."""

    duckdb_path: Path
    schemas: Optional[Set[str]] = None  # if None, use DEFAULT_SCHEMAS
    exclude_schemas: Optional[Set[str]] = None
    include_views: bool = True
    stats_level: StatsLevel = StatsLevel.standard
    sample_rows: Optional[int] = None
    max_columns: Optional[int] = None
    max_relations: Optional[int] = None

    def schema_allowlist(self) -> set[str]:
        if self.schemas is not None:
            return set(self.schemas)
        return set(DEFAULT_SCHEMAS)

    def to_dict(self) -> dict[str, Any]:
        return {
            "duckdb_path": str(self.duckdb_path),
            "schemas": sorted(self.schema_allowlist())
            if self.schemas is None
            else sorted(self.schemas or []),
            "exclude_schemas": sorted(self.exclude_schemas or []),
            "include_views": self.include_views,
            "stats_level": self.stats_level.value,
            "sample_rows": self.sample_rows,
            "max_columns": self.max_columns,
            "max_relations": self.max_relations,
        }
