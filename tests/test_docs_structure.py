from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _nav_targets(items):
    for item in items:
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            for value in item.values():
                if isinstance(value, str):
                    yield value
                else:
                    yield from _nav_targets(value)


def test_mkdocs_nav_targets_exist():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())
    docs_dir = REPO_ROOT / "docs"

    for target in _nav_targets(config["nav"]):
        assert (docs_dir / target).is_file(), target


def test_readme_links_to_operator_docs():
    readme = (REPO_ROOT / "README.md").read_text()

    assert "(docs/index.md)" in readme
    assert "(docs/quickstart.md)" in readme
