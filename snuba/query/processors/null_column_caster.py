from typing import Dict, Sequence

from snuba.clickhouse.columns import FlattenedColumn, SchemaModifiers
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.datasets.storage import ReadableTableStorage
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.functions import AGGREGATION_FUNCTIONS
from snuba.request.request_settings import RequestSettings


class NullColumnCaster(QueryProcessor):
    def _find_mismatched_null_columns(self) -> Dict[str, FlattenedColumn]:
        res: Dict[str, FlattenedColumn] = {}
        col_name_to_nullable: Dict[str, bool] = {}
        for table_storage in self.__merge_table_sources:
            for col in table_storage.get_schema().get_columns():
                col_is_nullable = False
                modifiers = col.type.get_modifiers()
                if isinstance(modifiers, SchemaModifiers):
                    col_is_nullable = modifiers.nullable
                other_storage_column_is_nullable = col_name_to_nullable.get(
                    col.name, None
                )
                if (
                    other_storage_column_is_nullable is not None
                    and other_storage_column_is_nullable != col_is_nullable
                ):
                    res[col.name] = col
                col_name_to_nullable[col.name] = col_is_nullable

        return res

    def __init__(self, merge_table_sources: Sequence[ReadableTableStorage]):
        self.__merge_table_sources = merge_table_sources
        self._mismatched_null_columns = self._find_mismatched_null_columns()

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def cast_column_to_nullable(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name in self._mismatched_null_columns:
                    return FunctionCall(
                        exp.alias,
                        "cast",
                        (
                            exp,
                            Literal(
                                None,
                                f"Nullable({self._mismatched_null_columns[exp.column_name].type.for_schema()})",
                            ),
                        ),
                    )
            return exp

        def transform_aggregate_functions_with_mismatched_nullable_parameters(
            exp: Expression,
        ) -> Expression:
            if (
                isinstance(exp, FunctionCall)
                and exp.function_name in AGGREGATION_FUNCTIONS
            ):
                return exp.transform(cast_column_to_nullable)
            return exp

        query.transform_expressions(
            transform_aggregate_functions_with_mismatched_nullable_parameters
        )
