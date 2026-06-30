from pathlib import Path


def test_dbt_project_is_polymarket_only():
    dbt_root = Path(__file__).resolve().parents[2] / "dbt"

    assert (dbt_root / "dbt_project.yml").exists()
    assert (dbt_root / "profiles" / "profiles.yml").exists()
    assert (dbt_root / "models" / "sources" / "polymarket_sources.yml").exists()
    assert (dbt_root / "models" / "polymarket" / "staging" / "staging.yml").exists()
    assert (dbt_root / "models" / "polymarket" / "marts" / "polymarket.yml").exists()
    assert (
        dbt_root / "models" / "polymarket" / "observability" / "observability.yml"
    ).exists()

    model_dirs = {p.name for p in (dbt_root / "models").iterdir() if p.is_dir()}
    assert model_dirs == {"polymarket", "sources"}


def test_dbt_project_version():
    text = (Path(__file__).resolve().parents[2] / "dbt" / "dbt_project.yml").read_text()

    assert "version: 0.1.0" in text
    assert "profile: oddsfox" in text
