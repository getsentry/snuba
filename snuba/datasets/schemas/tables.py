from __future__ import annotations

from typing import Optional, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.clusters.cluster import get_cluster
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas import RelationalSource, Schema
from snuba.query.expressions import FunctionCall


class TableSource(RelationalSource):
    """
    Relational datasource that represents a single table or view in the
    datamodel.
    """

    def __init__(
        self,
        table_name: str,
        columns: ColumnSet,
        mandatory_conditions: Optional[Sequence[FunctionCall]] = None,
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

    def get_mandatory_conditions(self) -> Sequence[FunctionCall]:
        return self.__mandatory_conditions

    def get_prewhere_candidates(self) -> Sequence[str]:
        return self.__prewhere_candidates


class TableSchema(Schema):
    """
    Represent a table-like schema. This means it represents either
    a Clickhouse table, a Clickhouse view or a Materialized view.
    """

    def __init__(
        self,
        columns: ColumnSet,
        *,
        local_table_name: str,
        dist_table_name: str,
        storage_set_key: StorageSetKey,
        mandatory_conditions: Optional[Sequence[FunctionCall]] = None,
        prewhere_candidates: Optional[Sequence[str]] = None,
    ):
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
        It is supposed to be used in maintenance.
        """
        return self.__local_table_name

    def get_table_name(self) -> str:
        """
        This represents the table we interact with to send queries to Clickhouse.
        In distributed mode this will be a distributed table. In local mode it is a local table.
        """
        return self.__table_name


class WritableTableSchema(TableSchema):
    """
    This class identifies a subset of TableSchemas we can write onto.
    While it does not provide any functionality by itself, it is used
    to allow the type checker to prevent us from returning a read only
    schema from StorageSchemas.
    """

    pass
