"""Run shipped browser JavaScript against a minimal DOM using Node's built-in tests."""

import subprocess
from pathlib import Path


def test_browser_editor_workflow():
    subprocess.run(
        ["node", "--test", "tests/frontend.cjs"],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        timeout=30,
    )
