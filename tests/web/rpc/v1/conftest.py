from typing import Generator

import pytest

from snuba.datasets.factory import reset_dataset_factory
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import Runner


@pytest.fixture
def eap_clickhouse_db(
    request: pytest.FixtureRequest, create_databases: None
) -> Generator[None, None, None]:
    """
    A custom ClickHouse fixture that only runs EAP (Events Analytics Platform) migrations.
    This is much faster than running all migrations for tests that only need EAP tables.

    Use this with @pytest.mark.eap_clickhouse_db marker.
    """
    if not request.node.get_closest_marker("eap_clickhouse_db"):
        pytest.fail("Need to use eap_clickhouse_db marker if eap_clickhouse_db fixture is used")

    try:
        reset_dataset_factory()
        # Run only SYSTEM migrations (required for migrations table) and EAP migrations
        runner = Runner()
        runner.run_all(group=MigrationGroup.EVENTS_ANALYTICS_PLATFORM, force=True)
        yield
    finally:
        # Import here to avoid circular imports
        from tests.conftest import _clear_db

        _clear_db()


# Hook to modify test collection
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Custom setup to handle eap_clickhouse_db marker."""
    if item.get_closest_marker("eap_clickhouse_db"):
        # Remove block_clickhouse_db if it was added by parent conftest
        fixturenames = getattr(item, "fixturenames", None)
        if fixturenames is not None:
            if "block_clickhouse_db" in fixturenames:
                fixturenames.remove("block_clickhouse_db")
            # Add our custom fixture if not already present
            if "eap_clickhouse_db" not in fixturenames:
                fixturenames.append("eap_clickhouse_db")
