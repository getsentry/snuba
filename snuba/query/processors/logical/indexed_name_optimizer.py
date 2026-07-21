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
from snuba.state.sentry_options import get_option


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
    gated behind two ``snuba``-namespace sentry options (either one enables it)
    and must only be turned on once ``indexed_name`` is fully populated for the
    queried data:

    - ``CONFIG_KEY`` (default off): a global on/off option.
    - ``ORGANIZATION_CONFIG_KEY`` (default 0): a single ``organization_id`` to
      enable the rewrite for, so it can be rolled out to one org first. It
      applies only when the query is scoped to exactly that org via an
      ``organization_id`` equality condition (which the EAP resolvers always
      inject).

    The promotion is item-type specific, so the rewrite is only applied when the
    query is unambiguously scoped (via an ``item_type`` equality condition) to
    the item type that owns the attribute. Otherwise the access is left as is and
    falls back to the bucket lookup.
    """

    # Global sentry option gating the rewrite. Off until ``indexed_name`` has
    # been backfilled across the retention window; turning it on lets the bloom
    # filter index serve ``sentry.op`` / ``sentry.metric.name`` filters.
    CONFIG_KEY = "eap_items_use_indexed_name"

    # A single organization_id to enable the rewrite for (0 = none). Lets the
    # rewrite be rolled out to one org before the global option is turned on.
    ORGANIZATION_CONFIG_KEY = "eap_items_use_indexed_name_organization_id"

    # item_type proto enum value -> attribute promoted into ``indexed_name``
    INDEXED_NAME_KEY_BY_ITEM_TYPE: dict[int, str] = {
        TraceItemType.TRACE_ITEM_TYPE_SPAN: "sentry.op",
        TraceItemType.TRACE_ITEM_TYPE_METRIC: "sentry.metric.name",
    }

    INDEXED_NAME_COLUMN = "indexed_name"
    ATTRIBUTES_STRING_COLUMN = "attributes_string"

    def _single_equals_int(self, query: Query, column_name: str) -> int | None:
        """Return the integer value from a single ``equals(<column_name>, N)``
        top-level condition, or ``None`` if the query is not unambiguously scoped
        by exactly one such condition."""
        condition = query.get_condition()
        if condition is None:
            return None

        values: set[int] = set()
        for cond in get_first_level_and_conditions(condition):
            if not isinstance(cond, FunctionCall) or cond.function_name != ConditionFunctions.EQ:
                continue
            if len(cond.parameters) != 2:
                continue
            lhs, rhs = cond.parameters
            if not isinstance(lhs, Column) or lhs.column_name != column_name:
                continue
            if not isinstance(rhs, Literal) or not isinstance(rhs.value, int):
                continue
            values.add(rhs.value)

        if len(values) != 1:
            return None
        return values.pop()

    def _is_enabled(self, query: Query) -> bool:
        """The rewrite is enabled by the global option, or for the single
        organization named by ``ORGANIZATION_CONFIG_KEY`` when the query is
        scoped to it."""
        if get_option(self.CONFIG_KEY, False):
            return True
        organization_id = get_option(self.ORGANIZATION_CONFIG_KEY, 0)
        return bool(organization_id) and (
            self._single_equals_int(query, "organization_id") == organization_id
        )

    def _indexed_name_key(self, query: Query) -> str | None:
        """Return the attribute name promoted into ``indexed_name`` for this
        query, or ``None`` if the query is not unambiguously scoped to a single
        supported item type."""
        item_type = self._single_equals_int(query, "item_type")
        if item_type is None:
            return None
        return self.INDEXED_NAME_KEY_BY_ITEM_TYPE.get(item_type)

    def _indexed_name_ref(self, exp: Expression, key: str) -> Column | None:
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
        if not self._is_enabled(query):
            return

        key = self._indexed_name_key(query)
        if key is None:
            return

        # Bind to a str-typed local: mypy widens a narrowed enclosing-scope
        # variable back to its declared Optional[str] inside the nested closure.
        indexed_key: str = key

        def transform(exp: Expression) -> Expression:
            indexed_ref = self._indexed_name_ref(exp, indexed_key)
            return indexed_ref if indexed_ref is not None else exp

        query.transform_expressions(transform)
