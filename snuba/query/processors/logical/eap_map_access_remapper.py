from typing import Mapping

from snuba.query.dsl import column, literal
from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings


class EAPClickhouseColumnRemapper(LogicalQueryProcessor):
    """
    In EAP entities, all attributes are hidden behind some virtual maps: attr_str, attr_i64, etc

    Sometimes a map access should refer to a 'real' column.
    For example, you can use this processor to convert
    attr_i64[duration_ms] to CAST(duration_ms, 'Int64')

    If data_type is the special value 'hex', the result is converted with the 'hex' function instead.

    If there is no matching column, the map access remains as-is:
    attr_str[derp] remains attr_str[derp]
    """

    def __init__(self, hash_bucket_name: str, keys: Mapping[str, str], data_type: str):
        super().__init__()
        self.hash_bucket_name = hash_bucket_name
        self.keys = keys
        self.data_type = data_type

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def transform(exp: Expression) -> Expression:
            if not isinstance(exp, SubscriptableReference):
                return exp

            if exp.column.column_name != self.hash_bucket_name:
                return exp

            if not isinstance(exp.key, Literal) or not isinstance(exp.key.value, str):
                return exp

            if exp.key.value not in self.keys:
                return exp

            if self.data_type == "hex":
                return FunctionCall(
                    alias=exp.alias,
                    function_name="hex",
                    parameters=(column(self.keys[exp.key.value]),),
                )
            return FunctionCall(
                alias=exp.alias,
                function_name="CAST",
                parameters=(
                    column(self.keys[exp.key.value]),
                    literal(self.data_type),
                ),
            )

        query.transform_expressions(transform)
