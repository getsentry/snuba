from snuba.query.dsl import arrayElement, column, literal
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.utils.constants import ATTRIBUTE_BUCKETS
from snuba.utils.hashes import fnv_1a


class EAPMapSharder(LogicalQueryProcessor):
    """
    In eap_spans, we split up map columns for better performance.
    In the entity, attr_str Map(String, String) becomes
    attr_str_0 Map(String, String),
    attr_str_1 Map(String, String),
    etc.

    This transformer converts mapKeys(attr_str) to arrayConcat(mapKeys(attr_str_0), mapKeys(attr_str_1), ...)
    and the same for mapValues

    It converts mapExists(attr_str, 'blah') to mapExists(attr_str_{hash('blah')%20}, 'blah')

    It converts attr_str[blah] to CAST(arrayElement(attr_str_10, 'blah'), 'String')
    """

    def __init__(self, src_bucket_name: str, dest_bucket_name: str, data_type: str):
        super().__init__()
        self.src_bucket_name = src_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.data_type = data_type

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_map_keys_and_values_expression(exp: Expression) -> Expression:
            if not isinstance(exp, FunctionCall):
                return exp

            if len(exp.parameters) != 1:
                return exp

            param = exp.parameters[0]
            if not isinstance(param, Column):
                return exp

            if param.column_name != self.src_bucket_name:
                return exp

            if exp.function_name not in ("mapKeys", "mapValues"):
                return exp

            return FunctionCall(
                alias=exp.alias,
                function_name="arrayConcat",
                parameters=tuple(
                    FunctionCall(
                        None,
                        function_name=exp.function_name,
                        parameters=(
                            Column(
                                None,
                                column_name=f"{self.dest_bucket_name}_{i}",
                                table_name=param.table_name,
                            ),
                        ),
                    )
                    for i in range(ATTRIBUTE_BUCKETS)
                ),
            )

        def transform_map_contains_expression(exp: Expression) -> Expression:
            if not isinstance(exp, FunctionCall):
                return exp

            if len(exp.parameters) != 2:
                return exp

            column = exp.parameters[0]
            if not isinstance(column, Column):
                return exp

            if column.column_name != self.src_bucket_name:
                return exp

            if exp.function_name != "mapContains":
                return exp

            key = exp.parameters[1]
            if not isinstance(key, Literal) or not isinstance(key.value, str):
                return exp

            bucket_idx = fnv_1a(key.value.encode("utf-8")) % ATTRIBUTE_BUCKETS
            return FunctionCall(
                alias=exp.alias,
                function_name=exp.function_name,
                parameters=(
                    Column(
                        None,
                        None,
                        f"{self.dest_bucket_name}_{bucket_idx}",
                    ),
                    key,
                ),
            )

        def transform_subscript_expression(exp: Expression) -> Expression:
            if not isinstance(exp, SubscriptableReference):
                return exp

            if exp.column.column_name != self.src_bucket_name:
                return exp

            if not isinstance(exp.key, Literal) or not isinstance(exp.key.value, str):
                return exp

            bucket_idx = fnv_1a(exp.key.value.encode("utf-8")) % ATTRIBUTE_BUCKETS
            return FunctionCall(
                exp.alias,
                "CAST",
                (
                    arrayElement(
                        None,
                        column(f"{self.dest_bucket_name}_{bucket_idx}"),
                        exp.key,
                    ),
                    literal(self.data_type),
                ),
            )

        query.transform_expressions(transform_map_keys_and_values_expression)
        query.transform_expressions(transform_map_contains_expression)
        query.transform_expressions(transform_subscript_expression)
