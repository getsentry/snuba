from typing import Generator

import pytest

from snuba.datasets.factory import reset_dataset_factory
from snuba.migrations.groups import MigrationGroup
from snuba.migrations.runner import Runner


@pytest.fixture
def eap(request: pytest.FixtureRequest, create_databases: None) -> Generator[None, None, None]:
    """
    A custom ClickHouse fixture that only runs EAP (Events Analytics Platform) migrations and Outcomes migrations (for storage routing).
    This is much faster than running all migrations for tests that only need EAP tables.

    Use this with @pytest.mark.eap marker.
    """
    if not request.node.get_closest_marker("eap"):
        pytest.fail("Need to use eap marker if eap fixture is used")

    try:
        reset_dataset_factory()
        # Run only SYSTEM migrations (required for migrations table) and EAP migrations
        runner = Runner()
        runner.run_all(group=MigrationGroup.EVENTS_ANALYTICS_PLATFORM, force=True)
        runner.run_all(group=MigrationGroup.OUTCOMES, force=True)
        yield
    finally:
        # Import here to avoid circular imports
        from tests.conftest import _clear_db

        _clear_db()


# Hook to modify test collection
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Custom setup to handle eap marker."""
    if item.get_closest_marker("eap"):
        # Remove block_clickhouse_db if it was added by parent conftest
        fixturenames = getattr(item, "fixturenames", None)
        if fixturenames is not None:
            if "block_clickhouse_db" in fixturenames:
                fixturenames.remove("block_clickhouse_db")
            # Add our custom fixture if not already present
            if "eap" not in fixturenames:
                fixturenames.append("eap")
