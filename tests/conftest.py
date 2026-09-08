import pytest

from oddsfox.store import Store


@pytest.fixture
def store(tmp_path):
    dataset = Store(tmp_path / "dataset")
    yield dataset
    dataset.close()
