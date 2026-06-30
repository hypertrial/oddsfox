"""Full-warehouse profile assembly and Markdown rendering."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Optional

import duckdb

from .discovery import discover_relations
from .models import (
    _PROFILE_QUERY_ERRORS,
    ColumnStats,
    ProfileConfig,
    RefreshStepResult,
    RelationProfile,
    ReportMetadata,
    WarehouseProfileReport,
)
from .stats import profile_relation


def build_warehouse_profile_report(
    conn: duckdb.DuckDBPyConnection,
    cfg: ProfileConfig,
    *,
    refresh_steps: Optional[list[RefreshStepResult]] = None,
) -> WarehouseProfileReport:
    """Profile all relations matching ``cfg``."""
    wh = set(cfg.schema_allowlist())
    ex = set(cfg.exclude_schemas or ())
    rels = discover_relations(
        conn,
        schema_whitelist=wh,
        exclude_schemas=ex,
        include_views=cfg.include_views,
    )
    if cfg.max_relations is not None and cfg.max_relations > 0:
        rels = rels[: int(cfg.max_relations)]
    out_rels: list[RelationProfile] = []
    errors: list[str] = []
    for r in rels:
        try:
            out_rels.append(
                profile_relation(
                    conn,
                    r,
                    stats_level=cfg.stats_level,
                    sample_rows=cfg.sample_rows,
                    max_columns=cfg.max_columns,
                )
            )
        except _PROFILE_QUERY_ERRORS as e:  # pragma: no cover
            errors.append(f"{r.table_schema}.{r.table_name}: {e}")
    meta = ReportMetadata(
        generated_at_utc=datetime.now(timezone.utc).isoformat(),
        duckdb_path=str(cfg.duckdb_path.resolve()),
        profile_config=cfg.to_dict(),
        refresh_steps=list(refresh_steps) if refresh_steps is not None else [],
    )
    return WarehouseProfileReport(
        metadata=meta, relations=out_rels, report_errors=errors
    )


def render_markdown_report(report: WarehouseProfileReport) -> str:
    """Render a skimmable Markdown summary."""
    lines: list[str] = [
        "# Warehouse profile",
        "",
        f"- **Generated (UTC)**: {report.metadata.generated_at_utc}",
        f"- **DuckDB**: `{report.metadata.duckdb_path}`",
    ]
    if report.metadata.refresh_steps:
        lines.append("\n## Refresh\n")
        for s in report.metadata.refresh_steps:
            st = "ok" if s.ok else "failed"
            lines.append(f"- **{s.step}** ({st}): {s.message or ''}")
    if report.metadata.profile_config:
        lines.append("\n## Config\n")
        for k, v in sorted(report.metadata.profile_config.items()):
            lines.append(f"- **{k}**: `{v}`")
    lines.append("\n## Relations\n")
    if report.report_errors:
        for e in report.report_errors:
            lines.append(f"- (report error) {e}\n")
    by_schema: dict[str, list[RelationProfile]] = {}
    for r in report.relations:
        by_schema.setdefault(r.table_schema, []).append(r)
    for sch in sorted(by_schema.keys()):
        lines.append(f"\n### schema `{sch}`\n")
        for rel in by_schema[sch]:
            ttag = f"{rel.table_type}"
            rpart = f"{rel.row_count}" if rel.row_count is not None else "?"
            if rel.error:
                lines.append(
                    f"- **`{rel.table_name}`** ({ttag}): *error* — {rel.error}\n"
                )
                continue
            lines.append(
                f"- **`{rel.table_name}`** ({ttag}): {rpart} rows, {rel.column_count} columns\n"
            )
            for cp in rel.columns:
                s = cp.spec
                col_line = f"  - `{s.name}` (`{s.data_type}`"
                if s.is_nullable == "NO":
                    col_line += ", NOT NULL"
                col_line += ")"
                if (  # pragma: no branch
                    cp.stats and stats_present(cp.stats) and (not cp.stats.error)
                ):
                    parts: list[str] = []
                    st = cp.stats
                    if st.null_percent is not None:  # pragma: no branch
                        parts.append(f"null%={st.null_percent}")
                    if st.approx_distinct is not None:  # pragma: no branch
                        parts.append(f"~distinct={st.approx_distinct}")
                    if (
                        st.min_value is not None and st.max_value is not None
                    ):  # pragma: no branch
                        parts.append(f"min={st.min_value!r} max={st.max_value!r}")
                    if (  # pragma: no branch
                        st.true_count is not None and st.false_count is not None
                    ):
                        parts.append(f"true={st.true_count} false={st.false_count}")
                    if st.min_len is not None:  # pragma: no branch
                        parts.append(
                            f"len[min,avg,max]=[{st.min_len},{st.avg_len},{st.max_len}]"
                        )
                    if parts:  # pragma: no branch
                        col_line += " — " + ", ".join(parts)
                if cp.stats and cp.stats.error:
                    col_line += f" *stats: {cp.stats.error}*"
                lines.append(col_line)
            lines.append("")
    if not report.relations and not report.report_errors:
        lines.append("_No relations matched the filters._\n")
    return "\n".join(lines).rstrip() + "\n"


def stats_present(st: ColumnStats) -> bool:
    """True if any non-error stat is set."""
    d = asdict(st)
    for k, v in d.items():
        if k == "error":
            continue
        if v is not None:
            return True
    return False
