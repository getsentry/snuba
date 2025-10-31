from typing import Sequence

from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.utils.hashes import fnv_1a


class HashBucketFunctionTransformer(LogicalQueryProcessor):
    """
    In eap_items, we split up map columns for better performance.
    In the entity, attr_str Map(String, String) becomes
    attr_str_0 Map(String, String),
    attr_str_1 Map(String, String),
    etc.

    This transformer converts mapKeys(attr_str) to arrayConcat(mapKeys(attr_str_0), mapKeys(attr_str_1), ...)
    and the same for mapValues

    It converts mapExists(attr_str, 'blah') to mapExists(attr_str_{hash('blah')%20}, 'blah')
    """

    def __init__(
        self,
        hash_bucket_names: Sequence[str],
        num_attribute_buckets: int,
    ):
        self.hash_bucket_names = hash_bucket_names
        self.num_attribute_buckets = num_attribute_buckets

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_map_keys_and_values_expression(exp: Expression) -> Expression:
            if not isinstance(exp, FunctionCall):
                return exp

            if len(exp.parameters) != 1:
                return exp

            param = exp.parameters[0]
            if not isinstance(param, Column):
                return exp

            if param.column_name not in self.hash_bucket_names:
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
                                column_name=f"{param.column_name}_{i}",
                                table_name=param.table_name,
                            ),
                        ),
                    )
                    for i in range(self.num_attribute_buckets)
                ),
            )

        def transform_map_contains_and_array_element_expression(
            exp: Expression,
        ) -> Expression:
            if not isinstance(exp, FunctionCall):
                return exp

            if len(exp.parameters) != 2:
                return exp

            column = exp.parameters[0]
            if not isinstance(column, Column):
                return exp

            if column.column_name not in self.hash_bucket_names:
                return exp

            if exp.function_name not in ("mapContains", "arrayElement"):
                return exp
            key = exp.parameters[1]
            if not isinstance(key, Literal) or not isinstance(key.value, str):
                return exp

            bucket_idx = fnv_1a(key.value.encode("utf-8")) % self.num_attribute_buckets
            return FunctionCall(
                alias=exp.alias,
                function_name=exp.function_name,
                parameters=(
                    Column(None, None, f"{column.column_name}_{bucket_idx}"),
                    key,
                ),
            )

        query.transform_expressions(transform_map_keys_and_values_expression)
        query.transform_expressions(transform_map_contains_and_array_element_expression)


class HashMapHasFunctionTransformer(LogicalQueryProcessor):
    """ """

    def __init__(
        self,
        hash_bucket_names: Sequence[str],
        num_attribute_buckets: int,
    ):
        self.hash_bucket_names = hash_bucket_names
        self.num_attribute_buckets = num_attribute_buckets

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_has_expressions(exp: Expression) -> Expression:
            if not isinstance(exp, FunctionCall):
                return exp

            if len(exp.parameters) != 2:
                return exp

            param = exp.parameters[0]
            if not isinstance(param, Column):
                return exp

            if param.column_name not in self.hash_bucket_names:
                return exp

            # TODO: also support hasAll
            if exp.function_name not in ("has",):
                return exp

            key = exp.parameters[1]
            if not isinstance(key, Literal) or not isinstance(key.value, str):
                return exp
            bucket_idx = fnv_1a(key.value.encode("utf-8")) % self.num_attribute_buckets

            return FunctionCall(
                alias=exp.alias,
                function_name="has",
                parameters=(
                    Column(
                        None,
                        column_name=f"{param.column_name}_{bucket_idx}",
                        table_name=param.table_name,
                    ),
                    key,
                ),
            )

        query.transform_expressions(transform_has_expressions)
