from collections import defaultdict
from typing import Dict, Optional, Sequence, Set, Tuple

from snuba.clickhouse.query import Query
from snuba.query.conditions import combine_and_conditions, combine_or_conditions
from snuba.query.expressions import Column as ColumnExpr
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.processors.physical.abstract_array_join_optimizer import (
    AbstractArrayJoinOptimizer,
)
from snuba.query.query_settings import QuerySettings


class BloomFilterOptimizer(AbstractArrayJoinOptimizer):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        single_filtered, multiple_filtered = self.get_filtered_arrays(query, self.key_columns)

        bloom_filter_condition = generate_bloom_filter_condition(
            self.column_name, single_filtered, multiple_filtered
        )

        if bloom_filter_condition:
            query.add_condition_to_ast(bloom_filter_condition)


def generate_bloom_filter_condition(
    column_name: str,
    single_filtered: Dict[str, Sequence[str]],
    multiple_filtered: Dict[Tuple[str, ...], Sequence[Tuple[str, ...]]],
) -> Optional[Expression]:
    """
    Generate the filters on the array columns to use the bloom filter index on
    the spans.op and spans.group columns in order to filter the transactions
    prior to the array join.

    The bloom filter index is requires the use of the has function, therefore
    the final condition is built up from a series of has conditions.
    """

    per_key_vals: Dict[str, Set[str]] = defaultdict(set)

    for key, single_filter in single_filtered.items():
        for val in single_filter:
            per_key_vals[key].add(val)

    for keys, multiple_filter in multiple_filtered.items():
        for val_tuple in multiple_filter:
            for key, val in zip(keys, val_tuple):
                per_key_vals[key].add(val)

    conditions = [
        combine_or_conditions(
            [
                FunctionCallExpr(
                    None,
                    "has",
                    (ColumnExpr(None, None, key), LiteralExpr(None, val)),
                )
                for val in sorted(vals)
            ]
        )
        for key, vals in per_key_vals.items()
    ]

    return combine_and_conditions(conditions) if conditions else None
