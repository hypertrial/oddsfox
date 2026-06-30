from __future__ import annotations

import logging

from dagster_dbt import DbtProject
from dagster_dbt.dbt_project import DagsterDbtProjectPreparer

from oddsfox.config.settings import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    resolve_dbt_executable,
)

logger = logging.getLogger(__name__)


class OddsfoxDbtProjectPreparer(DagsterDbtProjectPreparer):
    """Use the repo-resolved dbt executable during Dagster dev manifest prep."""

    def _dbt_cli(self, project: DbtProject):
        from dagster_dbt.core.resource import DbtCliResource

        return DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
            profile="oddsfox",
            target="dev",
            dbt_executable=resolve_dbt_executable(),
        )

    def _prepare_packages(self, project: DbtProject) -> None:
        self._dbt_cli(project).cli(
            ["deps", "--quiet"], target_path=project.target_path
        ).wait()

    def _prepare_manifest(self, project: DbtProject) -> None:
        self._dbt_cli(project).cli(
            [
                "parse",
                "--quiet",
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
                "--profile",
                "oddsfox",
                "--target",
                "dev",
            ],
            target_path=project.target_path,
        ).wait()
        self._invalidate_seeds_in_partial_parse(project)


_ODDSFOX_DBT_PREPARER = OddsfoxDbtProjectPreparer()


def prepare_dbt_project(
    project: DbtProject,
    *,
    preparer: DagsterDbtProjectPreparer | None = None,
) -> None:
    active_preparer = preparer or _ODDSFOX_DBT_PREPARER
    if active_preparer.using_dagster_dev():
        try:
            active_preparer.prepare_if_dev(project)
        except Exception:
            if not project.manifest_path.exists():
                raise
            logger.warning(
                "Using existing dbt manifest at %s after prepare_if_dev() failed",
                project.manifest_path,
                exc_info=True,
            )
    elif not project.manifest_path.exists():
        active_preparer.prepare(project)


DBT_PROJECT = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    profile="oddsfox",
    target="dev",
    prepare_project_cli_args=[
        "parse",
        "--quiet",
        "--profiles-dir",
        str(DBT_PROFILES_DIR),
    ],
)
prepare_dbt_project(DBT_PROJECT)

DBT_DAGSTER_GROUP_NAME = "analytics"


__all__ = [
    "DBT_DAGSTER_GROUP_NAME",
    "DBT_PROJECT",
    "OddsfoxDbtProjectPreparer",
    "prepare_dbt_project",
]
