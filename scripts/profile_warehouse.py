#!/usr/bin/env python3
"""
Profile the OddsFox DuckDB warehouse: relation list, row counts, column types, and stats.

By default, runs without ingestion (no ``--refresh``) and opens DuckDB **read-only**.
DuckDB allows only one read-write process per database file: a second process cannot
open the same file (even read-only) while Dagster or another job holds a write
connection. Use ``--snapshot-copy`` to profile a filesystem copy of the file while
the writer keeps running (best-effort consistency if the DB is actively writing).
Use ``--no-read-only`` if you need a read-write handle. Use ``--refresh`` to run
optional pipeline steps first (refresh still needs access to the live DB).

Usage:
  python3 scripts/profile_warehouse.py
  python3 scripts/profile_warehouse.py --snapshot-copy
  python3 scripts/profile_warehouse.py --format both --stats quick
  python3 scripts/profile_warehouse.py --refresh dbt
  python3 scripts/profile_warehouse.py --refresh all --continue-on-refresh-error
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT: Final[Path] = ensure_src_on_path()


def _parse_schema_csv(raw: str | None) -> set[str] | None:
    if raw is None or not str(raw).strip():
        return None
    return {x.strip() for x in raw.split(",") if x.strip()}


def _parse_exclude_csv(raw: str | None) -> set[str]:
    if raw is None or not str(raw).strip():
        return set()
    return {x.strip() for x in raw.split(",") if x.strip()}


def _expand_refresh_all() -> list[str]:
    return ["polymarket", "dbt"]


def _normalize_refresh_steps(steps: list[str]) -> list[str]:
    if "all" in steps:
        return _expand_refresh_all()
    # preserve order, unique
    seen: set[str] = set()
    out: list[str] = []
    for s in steps:
        s = s.strip().lower()
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _run_dbt_build(repo: Path) -> tuple[bool, str]:
    cmd = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "build",
        "--project-dir",
        str(repo / "dbt"),
        "--profiles-dir",
        str(repo / "dbt" / "profiles"),
    ]
    proc = subprocess.run(
        cmd,
        cwd=repo,
        capture_output=True,
        text=True,
    )
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        tail = out[-4000:] if len(out) > 4000 else out
        return False, f"dbt build exit {proc.returncode}: {tail}"
    return True, "ok"


def run_refresh(
    repo: Path,
    duckdb_path: Path,
    steps: list[str],
) -> list:
    from oddsfox.storage.duckdb.profile import RefreshStepResult

    results: list[RefreshStepResult] = []
    for step in steps:
        if step == "dbt":
            ok, msg = _run_dbt_build(repo)
            results.append(RefreshStepResult("dbt", ok, msg, {}))
            continue
        if step == "polymarket":
            try:
                from oddsfox.ingestion.polymarket.markets.sync import (
                    sync_markets,
                )
                from oddsfox.ingestion.polymarket.odds.sync import sync_odds

                sm = sync_markets()
                so = sync_odds()
                results.append(
                    RefreshStepResult(
                        "polymarket",
                        True,
                        "sync_markets+sync_odds completed",
                        {
                            "sync_markets": sm
                            if isinstance(sm, dict)
                            else str(sm)[:200],
                            "sync_odds": so if isinstance(so, dict) else str(so)[:200],
                        },
                    )
                )
            except Exception as e:
                results.append(RefreshStepResult("polymarket", False, str(e), {}))
            continue
        results.append(
            RefreshStepResult(
                step,
                False,
                f"unknown refresh step: {step}",
                {},
            )
        )
    return results


def _parse_stats(s: str) -> Any:
    from oddsfox.storage.duckdb.profile import StatsLevel

    return StatsLevel(s.strip().lower())


def _parse_format(s: str) -> Any:
    from oddsfox.storage.duckdb.profile import OutputFormat

    return OutputFormat(s.strip().lower())


def _snapshot_duckdb_files(src: Path, dest_dir: Path) -> Path:
    """Copy ``src`` and same-directory siblings (e.g. ``.wal``) for offline profiling."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(f for f in src.parent.glob(src.name + "*") if f.is_file())
    if not files:
        raise FileNotFoundError(
            f"No DuckDB files matched {src.name!r}* under {src.parent}"
        )
    for f in files:
        shutil.copy2(f, dest_dir / f.name)
    main = dest_dir / src.name
    if not main.is_file():
        raise FileNotFoundError(f"Expected {main} after snapshot copy")
    return main


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help="DuckDB file (default: DUCKDB_PATH from settings / .env)",
    )
    p.add_argument(
        "--schemas",
        type=str,
        default=None,
        help="Comma-separated schema allowlist (default: packaged DEFAULT_SCHEMAS)",
    )
    p.add_argument(
        "--exclude-schemas",
        type=str,
        default="",
        help="Comma-separated schemas to exclude from the allowlist",
    )
    p.add_argument(
        "--include-views",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include VIEW relations (default: true)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "artifacts" / "warehouse_profile",
        help="Output directory for reports",
    )
    p.add_argument(
        "--format",
        type=_parse_format,
        default=None,
        help="Output format: markdown, json, or both (default: both)",
    )
    p.add_argument(
        "--stats",
        type=_parse_stats,
        default=None,
        help="Stats depth: quick, standard, or full (default: standard)",
    )
    p.add_argument(
        "--sample-rows",
        type=int,
        default=None,
        help="If set, compute stats on at most this many rows per table",
    )
    p.add_argument(
        "--max-columns",
        type=int,
        default=None,
        help="If set, profile only the first N columns per table",
    )
    p.add_argument(
        "--max-relations",
        type=int,
        default=None,
        help="If set, stop after this many relations (discovery order)",
    )
    p.add_argument(
        "--refresh",
        action="append",
        default=[],
        metavar="STEP",
        help="Run refresh: polymarket, dbt, or all (can repeat)",
    )
    p.add_argument(
        "--continue-on-refresh-error",
        action="store_true",
        help="If set, continue to profile even if a refresh step failed",
    )
    p.add_argument(
        "--read-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Open DuckDB read-only (default: true). Use --no-read-only for read-write.",
    )
    p.add_argument(
        "--snapshot-copy",
        action="store_true",
        help=(
            "Copy the DuckDB file(s) under --duckdb-path into a temp folder under "
            "--output-dir, then profile the copy. Use when a writer already has the "
            "live file open. Snapshot may be slightly inconsistent during active writes."
        ),
    )
    args = p.parse_args()

    from oddsfox.config import settings
    from oddsfox.storage.duckdb import open_duckdb_connection
    from oddsfox.storage.duckdb.profile import (
        OutputFormat,
        ProfileConfig,
        StatsLevel,
        build_warehouse_profile_report,
        render_markdown_report,
    )

    out_fmt = args.format or OutputFormat.both
    stats_level = args.stats or StatsLevel.standard
    duck = args.duckdb_path or settings.DUCKDB_PATH
    duck = Path(duck).resolve()
    if not duck.parent.exists():
        duck.parent.mkdir(parents=True, exist_ok=True)

    refresh_norm = _normalize_refresh_steps([x for x in args.refresh if x])
    refresh_results: list = []
    if refresh_norm:
        refresh_results = run_refresh(REPO_ROOT, duck, refresh_norm)
        for r in refresh_results:
            if not r.ok:
                sys.stderr.write(f"refresh step {r.step} failed: {r.message}\n")
                if not args.continue_on_refresh_error:
                    return 1

    sch = _parse_schema_csv(args.schemas)
    ex = _parse_exclude_csv(args.exclude_schemas)
    cfg = ProfileConfig(
        duckdb_path=duck,
        schemas=sch,
        exclude_schemas=ex,
        include_views=args.include_views,
        stats_level=stats_level,
        sample_rows=args.sample_rows,
        max_columns=args.max_columns,
        max_relations=args.max_relations,
    )
    profile_path = duck
    snap_dir: Path | None = None
    if args.snapshot_copy:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        snap_dir = Path(
            tempfile.mkdtemp(
                prefix="warehouse_profile_snap_",
                dir=str(args.output_dir),
            )
        )
        try:
            profile_path = _snapshot_duckdb_files(duck, snap_dir)
        except BaseException:
            shutil.rmtree(snap_dir, ignore_errors=True)
            raise

    conn = open_duckdb_connection(profile_path, read_only=args.read_only)
    try:
        report = build_warehouse_profile_report(
            conn, cfg, refresh_steps=refresh_results
        )
    finally:
        conn.close()
        if snap_dir is not None:
            shutil.rmtree(snap_dir, ignore_errors=True)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = args.output_dir / f"warehouse_profile_{ts}"
    if out_fmt in (OutputFormat.json, OutputFormat.both):
        p_json = base.with_suffix(".json")
        p_json.write_text(report.as_json(), encoding="utf-8")
        print(f"Wrote {p_json}")
    if out_fmt in (OutputFormat.markdown, OutputFormat.both):
        p_md = base.with_suffix(".md")
        p_md.write_text(render_markdown_report(report), encoding="utf-8")
        print(f"Wrote {p_md}")
    if any(not r.ok for r in refresh_results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
