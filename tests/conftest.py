import json
from typing import Any, Callable, Generator, Iterator, Tuple, Union

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba import settings, state
from snuba.clickhouse.native import ClickhousePool
from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storages.factory import STORAGES, get_storage
from snuba.environment import setup_sentry
from snuba.redis import redis_client


def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    assert (
        settings.TESTING
    ), "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

    setup_sentry()

    for cluster in settings.CLUSTERS:
        connection = ClickhousePool(
            cluster["host"], cluster["port"], "default", "", "default",
        )
        database_name = cluster["database"]
        connection.execute(f"DROP DATABASE IF EXISTS {database_name};")
        connection.execute(f"CREATE DATABASE {database_name};")


@pytest.fixture(autouse=True)
def run_migrations() -> Iterator[None]:
    from snuba.migrations.runner import Runner

    Runner().run_all(force=True)

    yield

    for storage_key in STORAGES:
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        database = cluster.get_database()

        schema = storage.get_schema()
        if isinstance(schema, WritableTableSchema):
            table_name = schema.get_local_table_name()

            nodes = [*cluster.get_local_nodes(), *cluster.get_distributed_nodes()]
            for node in nodes:
                connection = cluster.get_node_connection(
                    ClickhouseClientSettings.MIGRATE, node
                )
                connection.execute(f"TRUNCATE TABLE IF EXISTS {database}.{table_name}")

    redis_client.flushdb()


@pytest.fixture
def convert_legacy_to_snql() -> Callable[[str, str], str]:
    def convert(data: str, entity: str) -> str:
        legacy = json.loads(data)
        sdk_output = json_to_snql(legacy, entity)
        return sdk_output.snuba()

    return convert


@pytest.fixture(params=["legacy", "snql"])
def _build_snql_post_methods(
    request: Any,
    test_entity: Union[str, Tuple[str, str]],
    test_app: Any,
    convert_legacy_to_snql: Callable[[str, str], str],
) -> Callable[..., Any]:
    dataset = entity = ""
    if isinstance(test_entity, tuple):
        entity, dataset = test_entity
    else:
        dataset = entity = test_entity

    endpoint = "/query" if request.param == "legacy" else f"/{dataset}/snql"

    def simple_post(data: str, entity: str = entity, referrer: str = "test") -> Any:
        if request.param == "snql":
            data = convert_legacy_to_snql(data, entity)
        return test_app.post(endpoint, data=data, headers={"referer": referrer})

    return simple_post


@pytest.fixture
def disable_query_cache() -> Generator[None, None, None]:
    cache, readthrough = state.get_configs(
        [("use_cache", settings.USE_RESULT_CACHE), ("use_readthrough_query_cache", 1)]
    )
    state.set_configs({"use_cache": 0, "use_readthrough_query_cache": 0})
    yield
    state.set_configs({"use_cache": cache, "use_readthrough_query_cache": readthrough})
