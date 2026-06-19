from typing import Optional

from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

from snuba.query.conditions import (
    ConditionFunctions,
    combine_and_conditions,
    get_first_level_and_conditions,
)
from snuba.query.dsl import or_cond
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
    index. This processor rewrites such a filter into an ``OR`` that also probes
    the indexed ``indexed_name`` column, so the bloom filter index can prune
    granules::

        equals(arrayElement(attributes_string, 'sentry.op'), 'db.query')
        ->
        or(
            equals(indexed_name, 'db.query'),
            equals(arrayElement(attributes_string, 'sentry.op'), 'db.query'),
        )

    The bucket lookup is intentionally kept as a fallback: ``indexed_name`` was
    added (with no backfill) by migration ``0057_add_name_column_and_index``, so
    rows written before that migration — or before the value started being
    populated — have an empty ``indexed_name`` while still carrying the value in
    ``attributes_string``. Filtering on ``indexed_name`` alone would silently
    drop those rows; the ``OR`` keeps the query correct while still letting the
    index help for the rows that do have it.

    Only the WHERE clause is rewritten (the bloom filter index only helps
    filtering), and only the positive ``equals`` / ``in`` operators against
    non-empty string literals are touched — for those the ``OR`` fallback is
    correctness-preserving (see ``_is_index_safe_value`` for why the empty-string
    default is excluded). Everything else (negations, guarded forms, comparisons
    to the column default, SELECT/GROUP BY references) keeps reading the bucket
    and is left untouched.

    The EAP RPC builder emits the ``arrayElement(attributes_string, key)`` form
    (see ``snuba.web.rpc.common.common._map_backed_operands``); the equivalent
    hand-written SnQL ``SubscriptableReference`` form is handled too. This runs
    before ``HashBucketFunctionTransformer`` so the bucket branch still gets
    mapped to its hashed bucket column downstream.

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

    # Operators whose granule pruning the ``indexed_name`` bloom filter can serve
    # and for which an OR fallback to the bucket lookup is correctness-preserving.
    OPTIMIZABLE_OPS = frozenset({ConditionFunctions.EQ, ConditionFunctions.IN})

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
        equivalent reference to the ``indexed_name`` column (carrying over the
        table name). Otherwise return ``None``."""
        if (
            isinstance(exp, FunctionCall)
            and exp.function_name == "arrayElement"
            and len(exp.parameters) == 2
            and isinstance(exp.parameters[0], Column)
            and exp.parameters[0].column_name == self.ATTRIBUTES_STRING_COLUMN
            and isinstance(exp.parameters[1], Literal)
            and exp.parameters[1].value == key
        ):
            return Column(None, exp.parameters[0].table_name, self.INDEXED_NAME_COLUMN)

        if (
            isinstance(exp, SubscriptableReference)
            and exp.column.column_name == self.ATTRIBUTES_STRING_COLUMN
            and isinstance(exp.key, Literal)
            and exp.key.value == key
        ):
            return Column(None, exp.column.table_name, self.INDEXED_NAME_COLUMN)

        return None

    def _is_index_safe_value(self, function_name: str, rhs: Expression) -> bool:
        """An empty string is the ``String`` column default, which non-backfilled
        rows (empty ``indexed_name``) also read as — so ``indexed_name = ''`` would
        match every such row regardless of its real value. Only probe
        ``indexed_name`` for non-empty string literals, where a match means the
        value was actually populated; the bucket fallback still covers the rest.
        This mirrors ``_comparison_can_match_column_default`` on the RPC side: the
        cases it guards are exactly the ones we must not push onto the index."""

        def safe_literal(exp: Expression) -> bool:
            return isinstance(exp, Literal) and isinstance(exp.value, str) and exp.value != ""

        if function_name == ConditionFunctions.EQ:
            return safe_literal(rhs)
        # IN: the right-hand side is an ``array(...)`` of literals.
        if function_name == ConditionFunctions.IN:
            return (
                isinstance(rhs, FunctionCall)
                and rhs.function_name == "array"
                and len(rhs.parameters) > 0
                and all(safe_literal(p) for p in rhs.parameters)
            )
        return False

    def _maybe_rewrite_conjunct(self, conjunct: Expression, key: str) -> Expression:
        """Rewrite a top-level ``equals``/``in`` filter on the indexed attribute
        into ``or(<op on indexed_name>, <original op on the bucket>)``. Any other
        conjunct is returned unchanged."""
        if (
            not isinstance(conjunct, FunctionCall)
            or conjunct.function_name not in self.OPTIMIZABLE_OPS
            or len(conjunct.parameters) != 2
        ):
            return conjunct

        indexed_ref = self._indexed_name_ref(conjunct.parameters[0], key)
        if indexed_ref is None:
            return conjunct

        if not self._is_index_safe_value(conjunct.function_name, conjunct.parameters[1]):
            return conjunct

        indexed_branch = FunctionCall(
            None,
            conjunct.function_name,
            (indexed_ref, conjunct.parameters[1]),
        )
        return or_cond(indexed_branch, conjunct)

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        key = self._indexed_name_key(query)
        if key is None:
            return

        condition = query.get_condition()
        if condition is None:
            return

        conjuncts = list(get_first_level_and_conditions(condition))
        rewritten = [self._maybe_rewrite_conjunct(cond, key) for cond in conjuncts]
        if rewritten == conjuncts:
            return

        query.set_ast_condition(combine_and_conditions(rewritten))
