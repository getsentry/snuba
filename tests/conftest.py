import json
import traceback
from typing import Any, Callable, Dict, Generator, List, Sequence, Tuple, Union

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba import settings, state
from snuba.clusters.cluster import (
    ClickhouseClientSettings,
    ClickhouseCluster,
    ClickhouseNode,
)
from snuba.core.initialize import initialize_snuba
from snuba.datasets.factory import reset_dataset_factory
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storages.factory import get_all_storage_keys, get_storage
from snuba.environment import setup_sentry
from snuba.redis import all_redis_clients

MIGRATIONS_CACHE: Dict[Tuple[ClickhouseCluster, ClickhouseNode], Dict[str, str]] = {}


def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    assert (
        settings.TESTING
    ), "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

    initialize_snuba()
    setup_sentry()
    initialize_snuba()


@pytest.fixture(scope="session")
def create_databases() -> None:
    for cluster in settings.CLUSTERS:
        clickhouse_cluster = ClickhouseCluster(
            host=cluster["host"],
            port=cluster["port"],
            user="default",
            password="",
            database="default",
            http_port=cluster["http_port"],
            storage_sets=cluster["storage_sets"],
            single_node=cluster["single_node"],
            cluster_name=cluster["cluster_name"] if "cluster_name" in cluster else None,
            distributed_cluster_name=cluster["distributed_cluster_name"]
            if "distributed_cluster_name" in cluster
            else None,
        )

        database_name = cluster["database"]
        nodes = [
            *clickhouse_cluster.get_local_nodes(),
            *clickhouse_cluster.get_distributed_nodes(),
        ]

        for node in nodes:
            connection = clickhouse_cluster.get_node_connection(
                ClickhouseClientSettings.MIGRATE, node
            )
            connection.execute(f"DROP DATABASE IF EXISTS {database_name};")
            connection.execute(f"CREATE DATABASE {database_name};")


def pytest_collection_modifyitems(items: Sequence[Any]) -> None:
    for item in items:
        if item.get_closest_marker("clickhouse_db"):
            item.fixturenames.append("clickhouse_db")
        else:
            item.fixturenames.append("block_clickhouse_db")

        if item.get_closest_marker("redis_db"):
            item.fixturenames.append("redis_db")
        else:
            item.fixturenames.append("block_redis_db")


class BlockedObject:
    def __init__(self, message: str) -> None:
        self.__failures: List[List[str]] = []
        self.__message = message

    def snuba_test_teardown(self) -> None:
        if self.__failures:
            lines = "\n".join(self.__failures[0])
            pytest.fail(f"{self.__message}, stacktrace: \n{lines}")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # record stacktrace and print it during teardown so there's no chance
        # of the exception being caught down somehow
        self.__failures.append(traceback.format_stack())
        pytest.fail(self.__message)


@pytest.fixture
def block_redis_db(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    from snuba.redis import _redis_clients

    blocked = BlockedObject(
        "attempted to access redis in test that does not use @pytest.mark.redis_db"
    )

    for key in _redis_clients:
        monkeypatch.setattr(_redis_clients[key], "execute_command", blocked)

    # Patch out Snuba settings so that random config access does not hit redis
    # (setting config still requires redis_db marker)
    monkeypatch.setattr("snuba.state.get_raw_configs", dict)

    yield

    blocked.snuba_test_teardown()


@pytest.fixture
def block_clickhouse_db(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    from snuba.clusters.cluster import ClickhouseCluster

    blocked = BlockedObject(
        "attempted to access clickhouse in test that does not use @pytest.mark.clickhouse_db"
    )

    monkeypatch.setattr(ClickhouseCluster, "get_query_connection", blocked)
    monkeypatch.setattr(ClickhouseCluster, "get_node_connection", blocked)
    monkeypatch.setattr(ClickhouseCluster, "get_batch_writer", blocked)
    monkeypatch.setattr(ClickhouseCluster, "get_reader", blocked)

    yield

    blocked.snuba_test_teardown()


@pytest.fixture
def redis_db(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    if not request.node.get_closest_marker("redis_db"):
        # Make people use the marker explicitly so `-m` works on CLI
        pytest.fail("Need to use redis_db marker if redis_db fixture is used")

    for redis_client in all_redis_clients():
        redis_client.flushdb()

    yield


def _build_migrations_cache() -> None:
    for storage_key in get_all_storage_keys():
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        database = cluster.get_database()
        nodes = [*cluster.get_local_nodes(), *cluster.get_distributed_nodes()]
        for node in nodes:
            if (cluster, node) not in MIGRATIONS_CACHE:
                connection = cluster.get_node_connection(
                    ClickhouseClientSettings.MIGRATE, node
                )
                rows = connection.execute(
                    f"SELECT name, create_table_query FROM system.tables WHERE database='{database}'"
                )
                for table_name, create_table_query in rows.results:
                    MIGRATIONS_CACHE.setdefault((cluster, node), {})[
                        table_name
                    ] = create_table_query


def _clear_db() -> None:
    for storage_key in get_all_storage_keys():
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


@pytest.fixture
def clickhouse_db(
    request: pytest.FixtureRequest, create_databases: None
) -> Generator[None, None, None]:
    if not request.node.get_closest_marker("clickhouse_db"):
        # Make people use the marker explicitly so `-m` works on CLI
        pytest.fail("Need to use clickhouse_db marker if clickhouse_db fixture is used")

    from snuba.migrations.runner import Runner

    try:
        reset_dataset_factory()
        if not MIGRATIONS_CACHE or request.module.__name__.startswith(
            "tests.migrations"
        ):
            Runner().run_all(force=True)
            # build cache once
            if not MIGRATIONS_CACHE:
                _build_migrations_cache()
        else:
            # apply migrations from cache
            applied_nodes = set()
            for (cluster, node), tables in MIGRATIONS_CACHE.items():
                connection = cluster.get_node_connection(
                    ClickhouseClientSettings.MIGRATE, node
                )
                for table_name, create_table_query in tables.items():
                    if (node, table_name) in applied_nodes:
                        continue
                    create_table_query = create_table_query.replace(
                        "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"
                    ).replace(
                        "CREATE MATERIALIZED VIEW",
                        "CREATE MATERIALIZED VIEW IF NOT EXISTS",
                    )
                    print(f"Creating table {table_name} on {node}")
                    connection.execute(create_table_query)
                applied_nodes.add((node, table_name))
        yield
    finally:
        _clear_db()


@pytest.fixture(autouse=True)
def clear_recorded_metrics() -> Generator[None, None, None]:
    from snuba.utils.metrics.backends.testing import clear_recorded_metric_calls

    yield

    clear_recorded_metric_calls()


@pytest.fixture
def convert_legacy_to_snql() -> Callable[[str, str], str]:
    def convert(data: str, entity: str) -> str:
        legacy = json.loads(data)
        sdk_output = json_to_snql(legacy, entity)
        return json.dumps(sdk_output.to_dict())

    return convert


@pytest.fixture
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

    endpoint = f"/{dataset}/snql"

    def simple_post(data: str, entity: str = entity, referrer: str = "test") -> Any:
        data = convert_legacy_to_snql(data, entity)
        return test_app.post(endpoint, data=data, headers={"referer": referrer})

    return simple_post


SnubaSetConfig = Callable[[str, Any], None]


@pytest.fixture
def snuba_set_config(request: pytest.FixtureRequest) -> SnubaSetConfig:
    finalizers_registered = set()

    def set_config(key: str, value: Any) -> None:
        # should register finalizer only once because 1) we don't have to undo
        # every single value change step-by-step 2) teardown-order via pytest
        # finalizers is poorly understood
        if key not in finalizers_registered:
            finalizers_registered.add(key)
            old_value = state.get_config(key)
            request.addfinalizer(lambda: state.set_config(key, old_value))

        state.set_config(key, value)

    return set_config


@pytest.fixture
def disable_query_cache(snuba_set_config: SnubaSetConfig, redis_db: None) -> None:
    snuba_set_config("use_cache", False)
    snuba_set_config("use_readthrough_query_cache", 0)
