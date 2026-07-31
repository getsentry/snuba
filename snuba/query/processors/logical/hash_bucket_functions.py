from collections.abc import Sequence

from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.utils.hashes import fnv_1a


class HashBucketFunctionTransformer(LogicalQueryProcessor):
    """
    In eap_items, map columns are split into buckets for performance:
    attr_str Map(...) becomes attr_str_0, attr_str_1, ... attr_str_{N-1}.

    - mapKeys(attr_str) / mapValues(attr_str) -> arrayConcat over every bucket.
    - arrayElement(attr_str, 'k') and the has(mapKeys(attr_str), 'k') existence
      check (see snuba.query.dsl.map_key_exists) -> the single bucket the key
      hashes to. The existence form is handled before the mapKeys rewrite so it
      isn't expanded to every bucket.
    """

    def __init__(
        self,
        hash_bucket_names: Sequence[str],
        num_attribute_buckets: int,
    ):
        self.hash_bucket_names = hash_bucket_names
        self.num_attribute_buckets = num_attribute_buckets

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform_has_map_keys_existence(exp: Expression) -> Expression:
            # Route has(mapKeys(col), 'key') to the single bucket the key hashes
            # to. Runs before the mapKeys rewrite below so the inner mapKeys isn't
            # expanded to an arrayConcat over every bucket.
            if not isinstance(exp, FunctionCall) or exp.function_name != "has":
                return exp

            if len(exp.parameters) != 2:
                return exp

            map_keys, key = exp.parameters
            if (
                not isinstance(map_keys, FunctionCall)
                or map_keys.function_name != "mapKeys"
                or len(map_keys.parameters) != 1
            ):
                return exp

            map_column = map_keys.parameters[0]
            if (
                not isinstance(map_column, Column)
                or map_column.column_name not in self.hash_bucket_names
            ):
                return exp

            if not isinstance(key, Literal) or not isinstance(key.value, str):
                return exp

            bucket_idx = fnv_1a(key.value.encode("utf-8")) % self.num_attribute_buckets
            return FunctionCall(
                alias=exp.alias,
                function_name="has",
                parameters=(
                    FunctionCall(
                        None,
                        function_name="mapKeys",
                        parameters=(
                            Column(
                                None,
                                column_name=f"{map_column.column_name}_{bucket_idx}",
                                table_name=map_column.table_name,
                            ),
                        ),
                    ),
                    key,
                ),
            )

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

        def transform_array_element_expression(exp: Expression) -> Expression:
            if not isinstance(exp, FunctionCall):
                return exp

            if len(exp.parameters) != 2:
                return exp

            column = exp.parameters[0]
            if not isinstance(column, Column):
                return exp

            if column.column_name not in self.hash_bucket_names:
                return exp

            if exp.function_name != "arrayElement":
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

        query.transform_expressions(transform_has_map_keys_existence)
        query.transform_expressions(transform_map_keys_and_values_expression)
        query.transform_expressions(transform_array_element_expression)


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
