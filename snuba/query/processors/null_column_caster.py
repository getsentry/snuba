from dataclasses import dataclass
from typing import Dict, Sequence, Set

from snuba.clickhouse.columns import SchemaModifiers
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.datasets.storage import ReadableTableStorage
from snuba.request.request_settings import RequestSettings
from snuba.query.functions import AGGREGATION_FUNCTIONS
from snuba.query.expressions import FunctionCall, Column, Expression, Literal

@dataclass
class MismatchedColumn:
    name: str
    schema_type: str


class NullColumnCaster(QueryProcessor):
    def _find_mismatched_null_columns(self) -> Dict[str, MismatchedColumn]:
        res: Dict[str, MismatchedColumn] = {}
        # need to save col.name and col.type.for_schema() for use in the actual casting
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
                    res[col.name] = MismatchedColumn(col.name, col.type.for_schema())
                col_name_to_nullable[col.name] = col_is_nullable

        return res

    def __init__(self, merge_table_sources: Sequence[ReadableTableStorage]):
        self.__merge_table_sources = merge_table_sources
        self._mismatched_null_columns = self._find_mismatched_null_columns()

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def cast_column_to_nullable(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name in self._mismatched_null_columns:
                    return FunctionCall(exp.alias, "cast", (exp, Literal(None, self._mismatched_null_columns[exp.column_name].schema_type)))  # TODO column type))
            return exp

        def transform_aggregate_functions_with_mismatched_nullable_parameters(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall) and exp.function_name in AGGREGATION_FUNCTIONS:
                return exp.transform(cast_column_to_nullable)
            return exp

        query.transform_expressions(transform_aggregate_functions_with_mismatched_nullable_parameters)
