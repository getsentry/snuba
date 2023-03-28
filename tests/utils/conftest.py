import pytest


@pytest.fixture(autouse=True)
def run_migrations() -> None:
    pass
