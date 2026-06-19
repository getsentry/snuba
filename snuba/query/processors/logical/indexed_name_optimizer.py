from typing import Optional

from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.query.conditions import (
    ConditionFunctions,
    get_first_level_and_conditions,
)
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


class IndexedNameOptimizer(LogicalQueryProcessor):
    """
    In eap_items, the per-item-type primary name attribute is promoted to a
    dedicated ``indexed_name`` String column with a bloom filter index:
    ``sentry.op`` for spans and ``sentry.metric.name`` for metrics. The same
    value is still written to the hashed ``attributes_string`` buckets.

    Filtering ``attributes_string['sentry.op']`` (resp.
    ``attributes_string['sentry.metric.name']``) would otherwise be translated
    into a bucket map lookup (``arrayElement(attributes_string_N, ...)``) which
    has no index. This processor rewrites those subscriptable accesses into a
    reference to the indexed ``indexed_name`` column so the bloom filter index
    can be used.

    The promotion is item-type specific, so the rewrite is only applied when the
    query is unambiguously scoped (via an ``item_type`` equality condition) to
    the item type that owns the attribute. Otherwise the access is left as is and
    falls back to the bucket lookup.
    """

    # item_type proto enum value -> attribute promoted into ``indexed_name``
    INDEXED_NAME_KEY_BY_ITEM_TYPE: dict[int, str] = {
        TraceItemType.TRACE_ITEM_TYPE_SPAN: "sentry.op",
        TraceItemType.TRACE_ITEM_TYPE_METRIC: "sentry.metric.name",
    }

    INDEXED_NAME_COLUMN = "indexed_name"
    ATTRIBUTES_STRING_COLUMN = "attributes_string"

    def _indexed_name_key(self, query: Query) -> Optional[str]:
        """Return the attribute name promoted into ``indexed_name`` for this
        query, or ``None`` if the query is not unambiguously scoped to a single
        supported item type."""
        condition = query.get_condition()
        if condition is None:
            return None

        item_types: set[int] = set()
        for cond in get_first_level_and_conditions(condition):
            if (
                not isinstance(cond, FunctionCall)
                or cond.function_name != ConditionFunctions.EQ
            ):
                continue
            if len(cond.parameters) != 2:
                continue
            lhs, rhs = cond.parameters
            if not isinstance(lhs, Column) or lhs.column_name != "item_type":
                continue
            if not isinstance(rhs, Literal) or not isinstance(rhs.value, int):
                continue
            item_types.add(rhs.value)

        if len(item_types) != 1:
            return None
        return self.INDEXED_NAME_KEY_BY_ITEM_TYPE.get(item_types.pop())

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        key = self._indexed_name_key(query)
        if key is None:
            return

        def transform(exp: Expression) -> Expression:
            if (
                isinstance(exp, SubscriptableReference)
                and exp.column.column_name == self.ATTRIBUTES_STRING_COLUMN
                and isinstance(exp.key, Literal)
                and exp.key.value == key
            ):
                return Column(
                    alias=exp.alias,
                    table_name=exp.column.table_name,
                    column_name=self.INDEXED_NAME_COLUMN,
                )
            return exp

        query.transform_expressions(transform)
