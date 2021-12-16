from dataclasses import dataclass
from typing import Dict, Optional, Sequence, Type

from snuba import settings
from snuba.clickhouse.native import ClickhousePool, ClickhouseResult
from snuba.clusters.cluster import ClickhouseClientSettings, ClickhouseCluster
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.utils.serializable_exception import SerializableException


class NonExistentSystemQuery(SerializableException):
    pass


class InvalidNodeError(SerializableException):
    pass


class InvalidStorageError(SerializableException):
    pass


class InvalidResultError(SerializableException):
    pass


class _QueryRegistry:
    """Keep a mapping of SystemQueries to their names"""

    def __init__(self) -> None:
        self.__mapping: Dict[str, Type["SystemQuery"]] = {}

    def register_class(self, cls: Type["SystemQuery"]) -> None:
        existing_class = self.__mapping.get(cls.__name__)
        if not existing_class:
            self.__mapping[cls.__name__] = cls

    def get_class_by_name(self, cls_name: str) -> Optional[Type["SystemQuery"]]:
        return self.__mapping.get(cls_name)

    @property
    def all_queries(self) -> Sequence[Type["SystemQuery"]]:
        return list(self.__mapping.values())


_QUERY_REGISTRY = _QueryRegistry()


@dataclass
class SystemQuery:
    sql: str

    @classmethod
    def to_json(cls) -> Dict[str, Optional[str]]:
        return {
            "sql": cls.sql,
            "description": cls.__doc__,
            "name": cls.__name__,
        }

    def __init_subclass__(cls) -> None:
        _QUERY_REGISTRY.register_class(cls)
        return super().__init_subclass__()

    @classmethod
    def from_name(cls, name: str) -> Optional[Type["SystemQuery"]]:
        return _QUERY_REGISTRY.get_class_by_name(name)

    @classmethod
    def all_queries(cls) -> Sequence[Type["SystemQuery"]]:
        return _QUERY_REGISTRY.all_queries


class CurrentMerges(SystemQuery):
    """Currently executing merges"""

    sql = """
        SELECT
          count(),
          is_currently_executing
        FROM system.replication_queue
        GROUP BY is_currently_executing
        """


class ActivePartitions(SystemQuery):
    sql = """ SELECT
        active,
        count()
    FROM system.parts
    GROUP BY active
    """


def _is_valid_node(host: str, port: int, cluster: ClickhouseCluster) -> bool:
    connection_id = cluster.get_connection_id()
    return host == connection_id.hostname and port == connection_id.tcp_port


def run_system_query_on_host_by_name(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    system_query_name: str,
) -> ClickhouseResult:
    query = SystemQuery.from_name(system_query_name)

    if not query:
        raise NonExistentSystemQuery(extra_data={"query_name": system_query_name})

    storage_key = None
    try:
        storage_key = StorageKey(storage_name)
    except ValueError:
        raise InvalidStorageError(extra_data={"storage_name": storage_name})

    storage = get_storage(storage_key)
    cluster = storage.get_cluster()

    if not _is_valid_node(clickhouse_host, clickhouse_port, cluster):
        raise InvalidNodeError(
            extra_data={"host": clickhouse_host, "port": clickhouse_port}
        )

    database = cluster.get_database()

    connection = ClickhousePool(
        clickhouse_host,
        clickhouse_port,
        settings.CLICKHOUSE_READONLY_USER,
        settings.CLICKHOUSE_READONLY_PASSWORD,
        database,
        # force read-only
        client_settings=ClickhouseClientSettings.QUERY.value.settings,
    )
    query_result = connection.execute(query=query.sql, with_column_types=True)
    connection.close()
    return query_result
