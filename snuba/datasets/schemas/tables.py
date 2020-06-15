from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Mapping, NamedTuple, Optional, Sequence

from snuba.clickhouse.columns import ColumnSet, ColumnType
from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas import MandatoryCondition, RelationalSource, Schema


class TableSource(RelationalSource):
    """
    Relational datasource that represents a single table or view in the
    datamodel.
    """

    def __init__(
        self,
        table_name: str,
        columns: ColumnSet,
        mandatory_conditions: Optional[Sequence[MandatoryCondition]] = None,
        prewhere_candidates: Optional[Sequence[str]] = None,
    ) -> None:
        self.__table_name = table_name
        self.__columns = columns
        self.__mandatory_conditions = mandatory_conditions or []
        self.__prewhere_candidates = prewhere_candidates or []

    def format_from(self) -> str:
        return self.__table_name

    def get_columns(self) -> ColumnSet:
        return self.__columns

    def get_mandatory_conditions(self) -> Sequence[MandatoryCondition]:
        return self.__mandatory_conditions

    def get_prewhere_candidates(self) -> Sequence[str]:
        return self.__prewhere_candidates


class DDLStatement(NamedTuple):
    table_name: str
    statement: str


class TableSchema(Schema, ABC):
    """
    Represent a table-like schema. This means it represents either
    a Clickhouse table, a Clickhouse view or a Materialized view.

    Specifically a TableSchema is something we can read from through
    a simple select and that provides DDL operations.
    """

    def __init__(
        self,
        columns: ColumnSet,
        *,
        local_table_name: str,
        dist_table_name: str,
        storage_set_key: StorageSetKey,
        mandatory_conditions: Optional[Sequence[MandatoryCondition]] = None,
        prewhere_candidates: Optional[Sequence[str]] = None,
        migration_function: Optional[
            Callable[[str, Mapping[str, ColumnType]], Sequence[str]]
        ] = None,
    ):
        self.__migration_function = (
            migration_function if migration_function else lambda table, schema: []
        )
        self.__local_table_name = local_table_name
        self.__table_name = (
            local_table_name
            if get_cluster(storage_set_key).is_single_node()
            else dist_table_name
        )
        self.__table_source = TableSource(
            self.get_table_name(), columns, mandatory_conditions, prewhere_candidates,
        )

    def get_data_source(self) -> TableSource:
        """
        In this abstraction the from clause is just the same
        table we refer to for writes.
        """
        return self.__table_source

    def get_local_table_name(self) -> str:
        """
        This returns the local table name for a distributed environment.
        It is supposed to be used in DDL commands and for maintenance.
        """
        return self.__local_table_name

    def get_table_name(self) -> str:
        """
        This represents the table we interact with to send queries to Clickhouse.
        In distributed mode this will be a distributed table. In local mode it is a local table.
        """
        return self.__table_name

    def get_local_drop_table_statement(self) -> DDLStatement:
        return DDLStatement(
            self.get_local_table_name(),
            "DROP TABLE IF EXISTS %s" % self.get_local_table_name(),
        )

    @abstractmethod
    def get_local_table_definition(self) -> DDLStatement:
        """
        Returns the DDL statement to create the local table.
        """
        raise NotImplementedError

    def get_migration_statements(
        self,
    ) -> Callable[[str, Mapping[str, ColumnType]], Sequence[str]]:
        return self.__migration_function


class WritableTableSchema(TableSchema):
    """
    This class identifies a subset of TableSchemas we can write onto.
    While it does not provide any functionality by itself, it is used
    to allow the type checker to prevent us from returning a read only
    schema from StorageSchemas.
    """

    pass


class MergeTreeSchema(WritableTableSchema):
    def __init__(
        self,
        columns: ColumnSet,
        *,
        local_table_name: str,
        dist_table_name: str,
        storage_set_key: StorageSetKey,
        mandatory_conditions: Optional[Sequence[MandatoryCondition]] = None,
        prewhere_candidates: Optional[Sequence[str]] = None,
        order_by: str,
        partition_by: Optional[str],
        sample_expr: Optional[str] = None,
        ttl_expr: Optional[str] = None,
        settings: Optional[Mapping[str, str]] = None,
        migration_function: Optional[
            Callable[[str, Mapping[str, ColumnType]], Sequence[str]]
        ] = None,
    ):
        super(MergeTreeSchema, self).__init__(
            columns=columns,
            local_table_name=local_table_name,
            dist_table_name=dist_table_name,
            storage_set_key=storage_set_key,
            mandatory_conditions=mandatory_conditions,
            prewhere_candidates=prewhere_candidates,
            migration_function=migration_function,
        )
        self.__order_by = order_by
        self.__partition_by = partition_by
        self.__sample_expr = sample_expr
        self.__ttl_expr = ttl_expr
        self.__settings = settings

    def _get_engine_type(self) -> str:
        return "MergeTree()"

    def __get_local_engine(self) -> str:
        partition_by_clause = (
            f"PARTITION BY {self.__partition_by}" if self.__partition_by else ""
        )
        sample_clause = f"SAMPLE BY {self.__sample_expr}" if self.__sample_expr else ""
        ttl_clause = f"TTL {self.__ttl_expr}" if self.__ttl_expr else ""

        if self.__settings:
            settings_list = ["%s=%s" % (k, v) for k, v in self.__settings.items()]
            settings_clause = "SETTINGS %s" % ", ".join(settings_list)
        else:
            settings_clause = ""

        return f"""
            {self._get_engine_type()}
            {partition_by_clause}
            ORDER BY {self.__order_by}
            {sample_clause}
            {ttl_clause}
            {settings_clause};"""

    def __get_table_definition(self, name: str, engine: str) -> str:
        return """
        CREATE TABLE IF NOT EXISTS %(name)s (%(columns)s) ENGINE = %(engine)s""" % {
            "columns": self.get_columns().for_schema(),
            "engine": engine,
            "name": name,
        }

    def get_local_table_definition(self) -> DDLStatement:
        return DDLStatement(
            self.get_local_table_name(),
            self.__get_table_definition(
                self.get_local_table_name(), self.__get_local_engine()
            ),
        )


class ReplacingMergeTreeSchema(MergeTreeSchema):
    def __init__(
        self,
        columns: ColumnSet,
        *,
        local_table_name: str,
        dist_table_name: str,
        storage_set_key: StorageSetKey,
        mandatory_conditions: Optional[Sequence[MandatoryCondition]] = None,
        prewhere_candidates: Optional[Sequence[str]] = None,
        order_by: str,
        partition_by: Optional[str],
        version_column: str,
        sample_expr: Optional[str] = None,
        ttl_expr: Optional[str] = None,
        settings: Optional[Mapping[str, str]] = None,
        migration_function: Optional[
            Callable[[str, Mapping[str, ColumnType]], Sequence[str]]
        ] = None,
    ) -> None:
        super(ReplacingMergeTreeSchema, self).__init__(
            columns=columns,
            local_table_name=local_table_name,
            dist_table_name=dist_table_name,
            storage_set_key=storage_set_key,
            mandatory_conditions=mandatory_conditions,
            prewhere_candidates=prewhere_candidates,
            order_by=order_by,
            partition_by=partition_by,
            sample_expr=sample_expr,
            ttl_expr=ttl_expr,
            settings=settings,
            migration_function=migration_function,
        )
        self.__version_column = version_column

    def _get_engine_type(self) -> str:
        return "ReplacingMergeTree(%s)" % self.__version_column


class SummingMergeTreeSchema(MergeTreeSchema):
    def _get_engine_type(self) -> str:
        return "SummingMergeTree()"


class AggregatingMergeTreeSchema(MergeTreeSchema):
    def _get_engine_type(self) -> str:
        return "AggregatingMergeTree()"


class MaterializedViewSchema(TableSchema):
    def __init__(
        self,
        columns: ColumnSet,
        *,
        local_materialized_view_name: str,
        dist_materialized_view_name: str,
        storage_set_key: StorageSetKey,
        mandatory_conditions: Optional[Sequence[MandatoryCondition]] = None,
        prewhere_candidates: Optional[Sequence[str]] = None,
        query: str,
        local_source_table_name: str,
        local_destination_table_name: str,
        dist_source_table_name: str,
        dist_destination_table_name: str,
        migration_function: Optional[
            Callable[[str, Mapping[str, ColumnType]], Sequence[str]]
        ] = None,
    ) -> None:
        super().__init__(
            columns=columns,
            local_table_name=local_materialized_view_name,
            dist_table_name=dist_materialized_view_name,
            storage_set_key=storage_set_key,
            mandatory_conditions=mandatory_conditions,
            prewhere_candidates=prewhere_candidates,
            migration_function=migration_function,
        )

        # Make sure the caller has provided a source_table_name in the query
        assert query % {"source_table_name": local_source_table_name} != query

        self.__query = query
        self.__local_source_table_name = local_source_table_name
        self.__local_destination_table_name = local_destination_table_name
        self.__dist_source_table_name = dist_source_table_name
        self.__dist_destination_table_name = dist_destination_table_name

    def __get_local_source_table_name(self) -> str:
        return self.__local_source_table_name

    def __get_local_destination_table_name(self) -> str:
        return self.__local_destination_table_name

    def __get_table_definition(
        self, name: str, source_table_name: str, destination_table_name: str
    ) -> str:
        return (
            """
        CREATE MATERIALIZED VIEW IF NOT EXISTS %(name)s TO %(destination_table_name)s (%(columns)s) AS %(query)s"""
            % {
                "name": name,
                "destination_table_name": destination_table_name,
                "columns": self.get_columns().for_schema(),
                "query": self.__query,
            }
            % {"source_table_name": source_table_name}
        )

    def get_local_table_definition(self) -> DDLStatement:
        return DDLStatement(
            self.get_local_table_name(),
            self.__get_table_definition(
                self.get_local_table_name(),
                self.__get_local_source_table_name(),
                self.__get_local_destination_table_name(),
            ),
        )
