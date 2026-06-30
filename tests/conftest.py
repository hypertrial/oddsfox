import os
import sys
from pathlib import Path

import pytest


def _isolate_xdist_xdg_cache() -> None:
    # ArviZ writes ~/.cache/arviz/daily_warning at import time; under pytest-xdist
    # multiple workers can race on the atomic rename and fail collection.
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        os.environ["XDG_CACHE_HOME"] = os.path.join(
            os.environ.get("TMPDIR", "/tmp"),
            f"pytest-xdg-{worker}",
        )


_isolate_xdist_xdg_cache()

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

_INTEGRATION_ROOTS = (
    ROOT / "tests" / "integration",
    ROOT / "tests" / "dbt",
    ROOT / "tests" / "dagster",
)


def pytest_collection_modifyitems(config, items):
    """Tag heavier suites with ``pytest.mark.integration`` for optional filtering."""
    for item in items:
        try:
            rp = Path(item.path).resolve()
        except (OSError, TypeError, ValueError, AttributeError):
            continue
        for base in _INTEGRATION_ROOTS:
            try:
                rp.relative_to(base)
            except ValueError:
                continue
            item.add_marker(pytest.mark.integration)
            break
