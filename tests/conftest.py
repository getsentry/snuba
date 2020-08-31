from typing import Iterator

import pytest

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.environment import setup_sentry
from snuba.utils.clock import Clock, TestingClock
from snuba.utils.streams.dummy import DummyBroker
from snuba.utils.streams.types import TPayload


def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    setup_sentry()

    # There is only one cluster in test, so fetch the host from there.
    cluster = settings.CLUSTERS[0]

    connection = ClickhousePool(
        cluster["host"], cluster["port"], "default", "", "default",
    )

    database_name = cluster["database"]
    connection.execute(f"DROP DATABASE IF EXISTS {database_name};")
    connection.execute(f"CREATE DATABASE {database_name};")


@pytest.fixture
def clock() -> Iterator[Clock]:
    yield TestingClock()


@pytest.fixture
def broker(clock: TestingClock) -> Iterator[DummyBroker[TPayload]]:
    yield DummyBroker(clock)
