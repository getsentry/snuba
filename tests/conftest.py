from typing import Iterator

import pytest

from snuba import settings
from snuba.clickhouse.native import ClickhousePool
from snuba.environment import setup_sentry
from snuba.state import delete_config, set_config
from snuba.utils.clock import Clock, TestingClock
from snuba.utils.streams.dummy import DummyBroker
from snuba.utils.streams.types import TPayload
from snuba.web.ast_rollout import ROLLOUT_RATE_CONFIG


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


@pytest.fixture(params=[0, 100], ids=["legacy", "ast"])
def query_type(request) -> None:
    set_config(ROLLOUT_RATE_CONFIG, request.param)
    yield
    delete_config(ROLLOUT_RATE_CONFIG)


@pytest.fixture
def clock() -> Iterator[Clock]:
    yield TestingClock()


@pytest.fixture
def broker(clock: TestingClock) -> Iterator[DummyBroker[TPayload]]:
    yield DummyBroker(clock)
