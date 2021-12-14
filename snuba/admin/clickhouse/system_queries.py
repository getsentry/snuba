from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence, Tuple, Type, Union, cast

from snuba.clickhouse.native import ClickhousePool
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.utils.serializable_exception import SerializableException

# from snuba.admin.views import application

# TODO (Vlad): we have to decouple getting a cluster from getting a connection
# for now though, I'm just going to make it so you can query the query node


class NonExistentSystemQuery(SerializableException):
    pass


class InvalidNodeError(SerializableException):
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


def _is_valid_node(host: str, port: int) -> bool:
    return (host == "localhost" or host == "127.0.0.1") and port == 9000


def run_system_query(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    system_query: Union[str, SystemQuery],
) -> Tuple[Sequence[Any], Sequence[Tuple[str, str]]]:
    query = (
        SystemQuery.from_name(system_query)
        if isinstance(system_query, str)
        else system_query
    )
    if not query:
        raise NonExistentSystemQuery(extra_data={"query": system_query})

    clickhouse_port = 9000 if clickhouse_port is None else clickhouse_port

    if not _is_valid_node(clickhouse_host, clickhouse_port):
        raise InvalidNodeError(
            extra_data={"host": clickhouse_host, "port": clickhouse_port}
        )

    storage_key = StorageKey(storage_name)
    storage = get_storage(storage_key)
    (clickhouse_user, clickhouse_password) = storage.get_cluster().get_credentials()
    database = storage.get_cluster().get_database()
    connection = ClickhousePool(
        clickhouse_host, clickhouse_port, clickhouse_user, clickhouse_password, database
    )
    query_result = connection.execute(query=query.sql, with_column_types=True)
    connection.close()
    return cast(Tuple[Sequence[Any], Sequence[Tuple[str, str]]], query_result,)
