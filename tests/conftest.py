import json
import traceback
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Generator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

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
from snuba.datasets.storages.storage_key import StorageKey
from snuba.environment import setup_sentry
from snuba.migrations.groups import MigrationGroup
from snuba.redis import all_redis_clients

NodeTableCache = Dict[Tuple[ClickhouseCluster, ClickhouseNode], Dict[str, str]]
CacheKey = Optional[FrozenSet[MigrationGroup]]

DB_MIGRATIONS_CACHE: Dict[CacheKey, NodeTableCache] = {}


def pytest_configure() -> None:
    """
    Set up the Sentry SDK to avoid errors hidden by configuration.
    Ensure the snuba_test database exists
    """
    assert settings.TESTING, "settings.TESTING is False, try `SNUBA_SETTINGS=test` or `make test`"

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
            secure=cluster["secure"],
            ca_certs=cluster["ca_certs"],
            verify=cluster["verify"],
            storage_sets=cluster["storage_sets"],
            single_node=cluster["single_node"],
            cluster_name=cluster["cluster_name"] if "cluster_name" in cluster else None,
            distributed_cluster_name=(
                cluster["distributed_cluster_name"]
                if "distributed_cluster_name" in cluster
                else None
            ),
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
        if item.get_closest_marker("eap"):
            item.fixturenames.append("eap")
        elif item.get_closest_marker("genmetrics_db"):
            item.fixturenames.append("genmetrics_db")
        elif item.get_closest_marker("events_db"):
            item.fixturenames.append("events_db")
        elif item.get_closest_marker("clickhouse_db"):
            item.fixturenames.append("clickhouse_db")
        elif item.get_closest_marker("custom_clickhouse_db"):
            item.fixturenames.append("custom_clickhouse_db")
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


def _build_db_cache(cache_key: CacheKey) -> None:
    """Snapshot current ClickHouse table definitions into DB_MIGRATIONS_CACHE[cache_key].
    Call immediately after Runner().run_all(...) to capture exactly the applied migrations."""
    node_table_cache: NodeTableCache = {}
    for storage_key in get_all_storage_keys():
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        database = cluster.get_database()
        nodes = [*cluster.get_local_nodes(), *cluster.get_distributed_nodes()]
        for node in nodes:
            if (cluster, node) in node_table_cache:
                continue
            connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
            rows = connection.execute(
                f"SELECT name, create_table_query FROM system.tables WHERE database='{database}'"
            )
            mv_tables = []
            non_mv_tables = []
            for table_name, create_table_query in rows.results:
                if "MATERIALIZED VIEW" in create_table_query:
                    mv_tables.append((table_name, create_table_query))
                else:
                    non_mv_tables.append((table_name, create_table_query))
            # non-MVs must be created before the materialized views that depend on them
            for table_name, create_table_query in non_mv_tables + mv_tables:
                node_table_cache.setdefault((cluster, node), {})[table_name] = create_table_query
    DB_MIGRATIONS_CACHE[cache_key] = node_table_cache


def _apply_db_cache(cache_key: CacheKey) -> None:
    """Re-apply cached table-creation DDL. Uses IF NOT EXISTS for idempotency."""
    for (cluster, node), tables in DB_MIGRATIONS_CACHE[cache_key].items():
        connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
        for table_name, create_table_query in tables.items():
            idempotent_query = create_table_query.replace(
                "CREATE TABLE", "CREATE TABLE IF NOT EXISTS"
            ).replace(
                "CREATE MATERIALIZED VIEW",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS",
            )
            connection.execute(idempotent_query)


def _run_db_fixture(
    request: pytest.FixtureRequest,
    marker_name: str,
    groups: Optional[Sequence[MigrationGroup]],
    cache_key: CacheKey,
) -> Generator[None, None, None]:
    """Shared body for clickhouse table creation fixtures.

    Args:
        request:     pytest FixtureRequest, used for marker validation.
        marker_name: The pytest marker name (e.g. "clickhouse_db").
        groups:      MigrationGroup(s) to run, or None to run all migrations.
        cache_key:   Key into DB_MIGRATIONS_CACHE. None means all migrations.
                     Pass a frozenset of MigrationGroup values for group-scoped fixtures.
                     Pass None to skip caching entirely.
    """
    from snuba.migrations.runner import Runner

    if not request.node.get_closest_marker(marker_name):
        pytest.fail(f"Need to use {marker_name} marker if {marker_name} fixture is used")

    try:
        reset_dataset_factory()
        if cache_key is not None and cache_key in DB_MIGRATIONS_CACHE:
            _apply_db_cache(cache_key)
        else:
            runner = Runner()
            if groups is None:
                runner.run_all(force=True)
            else:
                for group in groups:
                    runner.run_all(group=group, force=True)
            if cache_key is not None:
                _build_db_cache(cache_key)
        yield
    finally:
        _clear_db()


def _clear_db() -> None:
    for storage_key in get_all_storage_keys():
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        database = cluster.get_database()

        schema = storage.get_schema()
        if (
            isinstance(schema, WritableTableSchema)
            or storage_key == StorageKey.EAP_ITEMS_DOWNSAMPLE_8
            or storage_key == StorageKey.EAP_ITEMS_DOWNSAMPLE_64
            or storage_key == StorageKey.EAP_ITEMS_DOWNSAMPLE_512
            or storage_key == StorageKey.OUTCOMES_HOURLY
            or storage_key == StorageKey.OUTCOMES_DAILY
        ):
            table_name = schema.get_local_table_name()  # type: ignore

            nodes = [*cluster.get_local_nodes(), *cluster.get_distributed_nodes()]
            for node in nodes:
                connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
                connection.execute(f"TRUNCATE TABLE IF EXISTS {database}.{table_name}")


def _drop_tables() -> None:
    clusters: Set[ClickhouseCluster] = set()
    for storage_key in get_all_storage_keys():
        storage = get_storage(storage_key)
        cluster = storage.get_cluster()
        clusters.add(cluster)

    for cluster in clusters:
        database_name = cluster.get_database()

        nodes = [
            *cluster.get_local_nodes(),
            *cluster.get_distributed_nodes(),
        ]

        for node in nodes:
            connection = cluster.get_node_connection(ClickhouseClientSettings.MIGRATE, node)
            connection.execute(f"DROP DATABASE IF EXISTS {database_name};")
            connection.execute(f"CREATE DATABASE {database_name};")


@pytest.fixture
def custom_clickhouse_db(
    request: pytest.FixtureRequest,
) -> Generator[None, None, None]:
    if not request.node.get_closest_marker("custom_clickhouse_db"):
        # Make people use the marker explicitly so `-m` works on CLI
        pytest.fail(
            "Need to use custom_clickhouse_db marker if custom_clickhouse_db fixture is used"
        )
    try:
        _drop_tables()
        yield
    finally:
        _drop_tables()


@pytest.fixture
def clickhouse_db(
    request: pytest.FixtureRequest, create_databases: None
) -> Generator[None, None, None]:
    yield from _run_db_fixture(
        request=request,
        marker_name="clickhouse_db",
        groups=None,
        cache_key=frozenset(),
    )


@pytest.fixture
def events_db(
    request: pytest.FixtureRequest, create_databases: None
) -> Generator[None, None, None]:
    groups = [
        MigrationGroup.EVENTS,
        MigrationGroup.TRANSACTIONS,
        MigrationGroup.DISCOVER,
        MigrationGroup.GROUP_ATTRIBUTES,
        MigrationGroup.SEARCH_ISSUES,
    ]
    yield from _run_db_fixture(
        request=request,
        marker_name="events_db",
        groups=groups,
        cache_key=frozenset(groups),
    )


@pytest.fixture
def eap(request: pytest.FixtureRequest, create_databases: None) -> Generator[None, None, None]:
    groups = [MigrationGroup.EVENTS_ANALYTICS_PLATFORM, MigrationGroup.OUTCOMES]
    yield from _run_db_fixture(
        request=request,
        marker_name="eap",
        groups=groups,
        cache_key=frozenset(groups),
    )


@pytest.fixture
def genmetrics_db(
    request: pytest.FixtureRequest, create_databases: None
) -> Generator[None, None, None]:
    groups = [MigrationGroup.GENERIC_METRICS]
    yield from _run_db_fixture(
        request=request,
        marker_name="genmetrics_db",
        groups=groups,
        cache_key=frozenset(groups),
    )


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
