from typing import Dict, Sequence

from snuba.clickhouse.columns import FlattenedColumn, SchemaModifiers
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.functions import AGGREGATION_FUNCTIONS
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


def _col_is_nullable(col: FlattenedColumn) -> bool:
    modifiers = col.type.get_modifiers()
    if isinstance(modifiers, SchemaModifiers):
        return modifiers.nullable
    return False


StorageKeyStr = str


class NullColumnCaster(ClickhouseQueryProcessor):
    """
    In the case of merge tables (e.g. discover), if the column is nullable on
    one of the tables but not nullable in the other, clickhouse can throw an error.

    Example:

    This query will fail:

    >>> SELECT uniq(sdk_version) AS _snuba_sdk_version
    >>> FROM discover_dist
    >>> WHERE
    >>>     in((project_id AS _snuba_project_id), tuple(5433960))
    >>> LIMIT 1
    >>> OFFSET 0

    >>> Error:
    >>> "Conversion from AggregateFunction(uniq, LowCardinality(String)) to"
    >>> "AggregateFunction(uniq, LowCardinality(Nullable(String))) is not supported"

    This QueryProcessor will find aggregations on mismatched nullable fields and cast them
    to nullable. This will turn the above query into:

    >>> SELECT uniq(cast(sdk_version, Nullable(String))) AS _snuba_sdk_version
    >>> FROM discover_dist
    >>> WHERE
    >>>     in((project_id AS _snuba_project_id), tuple(5433960))
    >>> LIMIT 1
    >>> OFFSET 0

    And clickhouse will not throw an error since the column will be interpreted as nullable


    Usage:
        The initialization arguments of this processor are the string
        representation of the storage keys e.g.

        NullColumnCaster(["errors", "transactions"])

    """

    def _find_mismatched_null_columns(self) -> Dict[str, FlattenedColumn]:
        # This has to be imported here since the storage factory will also initialize this query processor
        # and importing it at the top will create an import cycle

        # This is a strange query processor because it takes storages as argument. We don't have
        # good first-class support for merge tables in snuba atm (12/06/2022) which makes us rely on this hack
        from snuba.datasets.storages.factory import get_storage

        mismatched_col_name_to_col: Dict[str, FlattenedColumn] = {}
        col_name_to_nullable: Dict[str, bool] = {}
        for table_storage_key in self.__merge_table_sources_keys:
            table_storage = get_storage(StorageKey(table_storage_key))
            for col in table_storage.get_schema().get_columns():
                col_is_nullable = _col_is_nullable(col)
                other_storage_column_is_nullable = col_name_to_nullable.get(col.name, None)
                if (
                    other_storage_column_is_nullable is not None
                    and other_storage_column_is_nullable != col_is_nullable
                ):
                    mismatched_col_name_to_col[col.name] = col
                col_name_to_nullable[col.name] = col_is_nullable

        return mismatched_col_name_to_col

    def __init__(self, merge_table_sources: Sequence[StorageKeyStr]):
        """
        Args:
            merge_table_sources: sequence of the storage keys which make up the merge table,
            This is necessary to find which fields need to be cast to nullable

        """
        self.__merge_table_sources_keys = merge_table_sources
        self.__mismatched_null_columns: Dict[str, FlattenedColumn] = {}

    @property
    def mismatched_null_columns(self) -> Dict[str, FlattenedColumn]:
        # The first time the query processor is run, we calculate the mismatched null columns
        # which never change. We don't do this at initialization time because there is no guarantee that
        # all the storages will be loaded at the time this query processor is

        if not self.__mismatched_null_columns:
            self.__mismatched_null_columns = self._find_mismatched_null_columns()
        return self.__mismatched_null_columns

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def cast_column_to_nullable(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name in self.mismatched_null_columns:
                    # depending on the order of the storage, this dictionary will contain
                    # either the nullable or non-nullable version of the column. No matter
                    # which one is in there, due to the mismatch on the merge table it needs to
                    # be cast as nullable anyways
                    mismatched_column = self.mismatched_null_columns[exp.column_name]
                    col_is_nullable = _col_is_nullable(mismatched_column)
                    col_type = mismatched_column.type.for_schema()
                    cast_str = col_type if col_is_nullable else f"Nullable({col_type})"
                    return FunctionCall(
                        exp.alias,
                        "cast",
                        (
                            # move the alias up to the cast function
                            Column(
                                None,
                                table_name=exp.table_name,
                                column_name=exp.column_name,
                            ),
                            Literal(None, cast_str),
                        ),
                    )
            return exp

        def transform_aggregate_functions_with_mismatched_nullable_parameters(
            exp: Expression,
        ) -> Expression:
            if isinstance(exp, FunctionCall) and exp.function_name in AGGREGATION_FUNCTIONS:
                return exp.transform(cast_column_to_nullable)
            return exp

        query.transform_expressions(
            transform_aggregate_functions_with_mismatched_nullable_parameters
        )
