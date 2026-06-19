from typing import Optional

from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba import state
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

    A filter on ``attributes_string['sentry.op']`` (resp.
    ``attributes_string['sentry.metric.name']``) is otherwise translated into a
    bucket map lookup (``arrayElement(attributes_string_N, ...)``) which has no
    index. This processor rewrites that access into a reference to the indexed
    ``indexed_name`` column so the bloom filter index can be used::

        equals(arrayElement(attributes_string, 'sentry.op'), 'db.query')
        -> equals(indexed_name, 'db.query')

    The EAP RPC builder emits the ``arrayElement(attributes_string, key)`` form
    (see ``snuba.web.rpc.common.common._map_backed_operands``); the equivalent
    hand-written SnQL ``SubscriptableReference`` form is handled too. This runs
    before ``HashBucketFunctionTransformer``, so untouched accesses still get
    mapped to their hashed bucket column downstream.

    ``indexed_name`` was added (with no backfill) by migration
    ``0057_add_name_column_and_index``, so rows written before that migration —
    or before the value started being populated — have an empty ``indexed_name``
    while still carrying the value in ``attributes_string``. Reading
    ``indexed_name`` for those rows would silently drop them, so the rewrite is
    gated behind the ``CONFIG_KEY`` runtime flag (default off) and must only be
    enabled once ``indexed_name`` is fully populated for the live retention
    window.

    The promotion is item-type specific, so the rewrite is only applied when the
    query is unambiguously scoped (via an ``item_type`` equality condition) to
    the item type that owns the attribute. Otherwise the access is left as is and
    falls back to the bucket lookup.
    """

    # Runtime flag gating the rewrite. Off (0) until ``indexed_name`` has been
    # backfilled across the retention window; flipping it on lets the bloom
    # filter index serve ``sentry.op`` / ``sentry.metric.name`` filters.
    CONFIG_KEY = "eap_items_use_indexed_name"

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
            if not isinstance(cond, FunctionCall) or cond.function_name != ConditionFunctions.EQ:
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

    def _indexed_name_ref(self, exp: Expression, key: str) -> Optional[Column]:
        """If ``exp`` is an ``attributes_string[key]`` access — either the
        ``arrayElement(attributes_string, key)`` form emitted by the EAP RPC
        builder or the hand-written ``SubscriptableReference`` form — return the
        equivalent reference to the ``indexed_name`` column (preserving the alias
        and table name). Otherwise return ``None``."""
        if (
            isinstance(exp, FunctionCall)
            and exp.function_name == "arrayElement"
            and len(exp.parameters) == 2
            and isinstance(exp.parameters[0], Column)
            and exp.parameters[0].column_name == self.ATTRIBUTES_STRING_COLUMN
            and isinstance(exp.parameters[1], Literal)
            and exp.parameters[1].value == key
        ):
            return Column(exp.alias, exp.parameters[0].table_name, self.INDEXED_NAME_COLUMN)

        if (
            isinstance(exp, SubscriptableReference)
            and exp.column.column_name == self.ATTRIBUTES_STRING_COLUMN
            and isinstance(exp.key, Literal)
            and exp.key.value == key
        ):
            return Column(exp.alias, exp.column.table_name, self.INDEXED_NAME_COLUMN)

        return None

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        if state.get_int_config(self.CONFIG_KEY, 0) == 0:
            return

        key = self._indexed_name_key(query)
        if key is None:
            return

        def transform(exp: Expression) -> Expression:
            indexed_ref = self._indexed_name_ref(exp, key)
            return indexed_ref if indexed_ref is not None else exp

        query.transform_expressions(transform)
