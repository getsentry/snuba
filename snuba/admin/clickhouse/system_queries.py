import re
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


class InvalidCustomQuery(SerializableException):
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
    nodes = cluster.get_local_nodes()
    return any(node.host_name == host and node.port == port for node in nodes)


def _run_sql_query_on_host(
    clickhouse_host: str, clickhouse_port: int, storage_name: str, sql: str
) -> ClickhouseResult:
    """
    Run the SQL query. It should be validated before getting to this point
    """
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
    query_result = connection.execute(query=sql, with_column_types=True)
    connection.close()

    return query_result


def run_system_query_on_host_by_name(
    clickhouse_host: str,
    clickhouse_port: int,
    storage_name: str,
    system_query_name: str,
) -> ClickhouseResult:
    query = SystemQuery.from_name(system_query_name)

    if not query:
        raise NonExistentSystemQuery(extra_data={"query_name": system_query_name})

    return _run_sql_query_on_host(
        clickhouse_host, clickhouse_port, storage_name, query.sql
    )


SYSTEM_QUERY_RE = re.compile(
    r"""
        ^ # Start
        (SELECT|select)
        \s
        (?P<select_statement>[\w\s,\(\)]+|\*)
        \s
        (FROM|from)
        \s
        system.[a-z_]+
        (?P<extra>\s[\w\s,=+\(\)']+)?
        ;? # Optional semicolon
        $ # End
    """,
    re.VERBOSE,
)


def run_system_query_on_host_with_sql(
    clickhouse_host: str, clickhouse_port: int, storage_name: str, system_query_sql: str
) -> ClickhouseResult:
    validate_system_query(system_query_sql)
    return _run_sql_query_on_host(
        clickhouse_host, clickhouse_port, storage_name, system_query_sql
    )


def validate_system_query(sql_query: str) -> None:
    """
    Simple validation to ensure query only attempts to access system tables and not
    any others. Will be replaced by AST parser eventually.

    Raises InvalidCustomQuery if query is invalid or not allowed.
    """
    sql_query = " ".join(sql_query.split())

    disallowed_keywords = ["select", "insert", "join"]

    match = SYSTEM_QUERY_RE.match(sql_query)

    if match is None:
        raise InvalidCustomQuery("Query is invalid")

    select_statement = match.group("select_statement")

    # Extremely quick and dirty way of ensuring there is not a nested select, insert or a join
    for kw in disallowed_keywords:
        if kw in select_statement.lower():
            raise InvalidCustomQuery(f"{kw} is not allowed here")

    extra = match.group("extra")

    # Unfortunately "extra" is pretty permissive right now, just ensure
    # there is no attempt to do a select, insert or join in there
    if extra is not None:
        for kw in disallowed_keywords:
            if kw in extra.lower():
                raise InvalidCustomQuery(f"{kw} is not allowed here")
