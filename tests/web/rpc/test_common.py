import uuid
from datetime import UTC, datetime, timedelta

import pytest
from google.protobuf import json_format, struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_options.testing import override_options
from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import (
    RequestMeta,
    TraceItemType,
)
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    Array,
    AttributeKey,
    AttributeValue,
    StrArray,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    AnyAttributeFilter,
    ComparisonFilter,
    ExistsFilter,
    TraceItemFilter,
)
from sentry_protos.snuba.v1.trace_item_pb2 import AnyValue

from snuba import settings
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.protos.common import (
    ATTRIBUTES_TO_COALESCE,
    MalformedAttributeException,
    type_array_to_membership_array_expression_from_typed_columns,
)
from snuba.query.dsl import Functions as f
from snuba.query.dsl import and_cond, column
from snuba.query.expressions import (
    Column as ColumnExpr,
)
from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.web.rpc.common.common import (
    _any_attribute_filter_to_expression,
    attribute_key_to_expression,
    dedupe_and_conditions,
    next_monday,
    prev_monday,
    trace_item_filters_to_expression,
    treeify_or_and_conditions,
    use_sampling_factor,
)
from snuba.web.rpc.common.exceptions import (
    BadSnubaRPCRequestException,
    RPCAllocationPolicyException,
    convert_rpc_exception_to_proto,
)
from snuba.web.rpc.v1.endpoint_trace_item_table import EndpointTraceItemTable
from tests.conftest import SnubaSetConfig
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message


class TestCommon:
    def test_timestamp_rounding(self) -> None:
        start = datetime(2025, 3, 10)
        end = datetime(2025, 3, 17)

        tmp = start.replace()
        for _ in range(7):
            assert prev_monday(tmp) == start
            assert next_monday(tmp) == end
            tmp += timedelta(days=1)

    @pytest.mark.redis_db
    def test_use_sampling_factor(self, snuba_set_config: SnubaSetConfig) -> None:
        assert use_sampling_factor(
            RequestMeta(
                start_timestamp=Timestamp(seconds=settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS)
            )
        )
        assert not use_sampling_factor(
            RequestMeta(
                start_timestamp=Timestamp(
                    seconds=settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS - 1
                )
            )
        )
        with override_options("snuba", {"use_sampling_factor_timestamp_seconds": 10}):
            assert use_sampling_factor(RequestMeta(start_timestamp=Timestamp(seconds=10)))
            assert not use_sampling_factor(RequestMeta(start_timestamp=Timestamp(seconds=9)))


class TestTraceItemFiltersArrayLike:
    def _make_like_filter(
        self,
        attr_name: str,
        attr_type: AttributeKey.Type.ValueType,
        pattern: str,
        op: ComparisonFilter.Op.ValueType = ComparisonFilter.OP_LIKE,
        ignore_case: bool = False,
    ) -> TraceItemFilter:
        return TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=attr_type, name=attr_name),
                op=op,
                value=AttributeValue(val_str=pattern),
                ignore_case=ignore_case,
            )
        )

    def test_like_on_array_key(self) -> None:
        item_filter = self._make_like_filter("my_tags", AttributeKey.Type.TYPE_ARRAY, "%error%")
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        # First param is a Lambda with like
        lam = result.parameters[0]
        assert isinstance(lam, Lambda)
        assert lam.parameters == ("x",)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "like"
        # Second param is the array expression (from attribute_key_to_expression)
        assert isinstance(result.parameters[1], FunctionCall)

    def test_like_on_array_key_ignore_case(self) -> None:
        item_filter = self._make_like_filter(
            "my_tags", AttributeKey.Type.TYPE_ARRAY, "%error%", ignore_case=True
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        lam = result.parameters[0]
        assert isinstance(lam, Lambda)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "ilike"

    def test_not_like_on_array_key(self) -> None:
        item_filter = self._make_like_filter(
            "my_tags",
            AttributeKey.Type.TYPE_ARRAY,
            "%error%",
            op=ComparisonFilter.OP_NOT_LIKE,
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        # Result should be NOT(arrayExists(...))
        assert isinstance(result, FunctionCall)
        assert result.function_name == "not"
        inner = result.parameters[0]
        assert isinstance(inner, FunctionCall)
        assert inner.function_name == "arrayExists"
        lam = inner.parameters[0]
        assert isinstance(lam, Lambda)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "like"

    def test_not_like_on_array_key_ignore_case(self) -> None:
        item_filter = self._make_like_filter(
            "my_tags",
            AttributeKey.Type.TYPE_ARRAY,
            "%error%",
            op=ComparisonFilter.OP_NOT_LIKE,
            ignore_case=True,
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(result, FunctionCall)
        assert result.function_name == "not"
        inner = result.parameters[0]
        assert isinstance(inner, FunctionCall)
        assert inner.function_name == "arrayExists"
        lam = inner.parameters[0]
        assert isinstance(lam, Lambda)
        assert isinstance(lam.transformation, FunctionCall)
        assert lam.transformation.function_name == "ilike"

    def test_like_on_array_key_non_string_value_raises(self) -> None:
        item_filter = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_ARRAY, name="my_tags"),
                op=ComparisonFilter.OP_LIKE,
                value=AttributeValue(val_int=42),
            )
        )
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="LIKE/NOT_LIKE on array keys requires a string pattern",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    def test_equals_on_array_key_with_str_array_value_raises(self) -> None:
        item_filter = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_ARRAY, name="my_tags"),
                op=ComparisonFilter.OP_EQUALS,
                value=AttributeValue(val_str_array=StrArray(values=["a", "b"])),
            )
        )
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="OP_EQUALS/OP_NOT_EQUALS on array keys require a scalar value",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    def test_like_on_int_key_raises(self) -> None:
        item_filter = self._make_like_filter("my_int", AttributeKey.Type.TYPE_INT, "%something%")
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="LIKE comparison is only supported on string and array keys",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    def test_not_like_on_int_key_raises(self) -> None:
        item_filter = self._make_like_filter(
            "my_int",
            AttributeKey.Type.TYPE_INT,
            "%something%",
            op=ComparisonFilter.OP_NOT_LIKE,
        )
        with pytest.raises(
            BadSnubaRPCRequestException,
            match="NOT LIKE comparison is only supported on string and array keys",
        ):
            trace_item_filters_to_expression(item_filter, attribute_key_to_expression)


def _collect_column_names(expr: Expression) -> set[str]:
    names: set[str] = set()

    def visit(node: Expression) -> Expression:
        if isinstance(node, ColumnExpr):
            names.add(node.column_name)
        return node

    expr.transform(visit)
    return names


def _collect_function_names(expr: Expression) -> set[str]:
    names: set[str] = set()

    def visit(node: Expression) -> Expression:
        if isinstance(node, FunctionCall):
            names.add(node.function_name)
        return node

    expr.transform(visit)
    return names


def _collect_literal_values(expr: Expression) -> list[object]:
    values: list[object] = []

    def visit(node: Expression) -> Expression:
        if isinstance(node, Literal):
            values.append(node.value)
        return node

    expr.transform(visit)
    return values


_TYPED_ARRAY_COLUMNS = {
    "attributes_array_string",
    "attributes_array_int",
    "attributes_array_float",
    "attributes_array_bool",
}


class TestTraceItemFiltersArrayMapColumns:
    """Array predicates always read the typed ``attributes_array_*`` map columns. An
    element-typed array key (TYPE_ARRAY_STRING/INT/DOUBLE/BOOL) resolves to its single
    column and compares natively; the deprecated untyped ``TYPE_ARRAY`` has no element
    type, so a ``val_str`` is coerced to each native type it parses as and matched against
    every column it could live in (a numeric string searches int and float too, a
    ``true``/``false`` string the bool column). A value-less exists filter over a
    deprecated key reads all four."""

    def _array_filter(
        self,
        op: ComparisonFilter.Op.ValueType,
        value: AttributeValue,
        key_type: AttributeKey.Type.ValueType = AttributeKey.Type.TYPE_ARRAY,
    ) -> TraceItemFilter:
        return TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=key_type, name="my_tags"),
                op=op,
                value=value,
            )
        )

    def test_like_on_string_array_key_uses_only_string_typed_column(self) -> None:
        # A LIKE pattern can only match string elements.
        result = trace_item_filters_to_expression(
            self._array_filter(
                ComparisonFilter.OP_LIKE,
                AttributeValue(val_str="%error%"),
                AttributeKey.Type.TYPE_ARRAY_STRING,
            ),
            attribute_key_to_expression,
        )
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        assert _collect_column_names(result) == {"attributes_array_string"}
        assert "like" in _collect_function_names(result)
        # No stringify-everything normalization of the stored elements.
        assert {"arrayConcat", "arrayMap", "toString"}.isdisjoint(_collect_function_names(result))

    def test_equals_on_string_array_key_uses_only_string_typed_column(self) -> None:
        result = trace_item_filters_to_expression(
            self._array_filter(
                ComparisonFilter.OP_EQUALS,
                AttributeValue(val_str="error"),
                AttributeKey.Type.TYPE_ARRAY_STRING,
            ),
            attribute_key_to_expression,
        )
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        assert _collect_column_names(result) == {"attributes_array_string"}
        assert "error" in _collect_literal_values(result)
        assert {"arrayConcat", "arrayMap", "toString"}.isdisjoint(_collect_function_names(result))

    def test_equals_on_int_array_key_uses_only_int_typed_column_natively(self) -> None:
        # An element-typed int array names its column exactly, so a val_str "12" is
        # coerced to a native int and matched only against attributes_array_int.
        result = trace_item_filters_to_expression(
            self._array_filter(
                ComparisonFilter.OP_EQUALS,
                AttributeValue(val_str="12"),
                AttributeKey.Type.TYPE_ARRAY_INT,
            ),
            attribute_key_to_expression,
        )
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        assert _collect_column_names(result) == {"attributes_array_int"}
        assert any(type(x) is int and x == 12 for x in _collect_literal_values(result))

    def test_equals_on_bool_array_key_uses_only_bool_typed_column_natively(self) -> None:
        result = trace_item_filters_to_expression(
            self._array_filter(
                ComparisonFilter.OP_EQUALS,
                AttributeValue(val_str="true"),
                AttributeKey.Type.TYPE_ARRAY_BOOL,
            ),
            attribute_key_to_expression,
        )
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"
        assert _collect_column_names(result) == {"attributes_array_bool"}
        assert any(type(x) is bool and x is True for x in _collect_literal_values(result))

    def test_exists_filter_on_int_array_key_uses_only_int_column(self) -> None:
        item_filter = TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_ARRAY_INT, name="my_tags")
            )
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        # Existence is notEmpty(arrayElement(attributes_array_int, 'my_tags')).
        assert isinstance(result, FunctionCall)
        assert result.function_name == "notEmpty"
        assert _collect_column_names(result) == {"attributes_array_int"}

    def test_equals_numeric_string_on_deprecated_array_matches_int_and_float_natively(
        self,
    ) -> None:
        # The deprecated untyped TYPE_ARRAY has no element type: "12" parses as an int and
        # a float, so it searches both numeric columns plus the string column natively.
        result = trace_item_filters_to_expression(
            self._array_filter(ComparisonFilter.OP_EQUALS, AttributeValue(val_str="12")),
            attribute_key_to_expression,
        )
        assert isinstance(result, FunctionCall)
        assert result.function_name == "or"
        assert _collect_column_names(result) == {
            "attributes_array_string",
            "attributes_array_int",
            "attributes_array_float",
        }
        values = _collect_literal_values(result)
        assert "12" in values  # string column, raw string
        assert any(type(x) is int and x == 12 for x in values)  # int column, native int
        assert any(type(x) is float and x == 12.0 for x in values)  # float column, native float
        assert {"arrayConcat", "arrayMap", "toString"}.isdisjoint(_collect_function_names(result))

    def test_not_equals_numeric_string_on_deprecated_array_negates_native_membership(
        self,
    ) -> None:
        result = trace_item_filters_to_expression(
            self._array_filter(ComparisonFilter.OP_NOT_EQUALS, AttributeValue(val_str="12")),
            attribute_key_to_expression,
        )
        assert isinstance(result, FunctionCall)
        assert result.function_name == "not"
        inner = result.parameters[0]
        assert isinstance(inner, FunctionCall)
        assert inner.function_name == "or"
        assert _collect_column_names(inner) == {
            "attributes_array_string",
            "attributes_array_int",
            "attributes_array_float",
        }

    def test_exists_filter_on_deprecated_array_key_reads_all_typed_columns(self) -> None:
        item_filter = TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_ARRAY, name="my_tags")
            )
        )
        result = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        # Existence is notEmpty(arrayConcat(...)) over the four typed columns.
        assert isinstance(result, FunctionCall)
        assert result.function_name == "notEmpty"
        inner = result.parameters[0]
        assert isinstance(inner, FunctionCall)
        assert inner.function_name == "arrayConcat"
        assert _collect_column_names(inner) == _TYPED_ARRAY_COLUMNS

    def test_typed_membership_function_rejects_non_array(self) -> None:
        with pytest.raises(MalformedAttributeException):
            type_array_to_membership_array_expression_from_typed_columns(
                AttributeKey(type=AttributeKey.Type.TYPE_STRING, name="my_tags")
            )


class TestExistsFilterCoalesced:
    """exists_filter on coalesced attributes must check all deprecated keys."""

    @staticmethod
    def _collect_existence_keys(expr: FunctionCall) -> set[str]:
        """Recursively collect all keys from a (possibly nested) OR of
        has(mapKeys(col), key) existence checks."""
        keys: set[str] = set()
        if expr.function_name == "has":
            key_literal = expr.parameters[1]
            assert isinstance(key_literal, Literal)
            assert isinstance(key_literal.value, str)
            keys.add(key_literal.value)
        elif expr.function_name == "or":
            for param in expr.parameters:
                assert isinstance(param, FunctionCall)
                keys.update(TestExistsFilterCoalesced._collect_existence_keys(param))
        return keys

    def test_exists_filter_on_coalesced_string_attribute(self) -> None:
        """End-to-end: exists_filter through trace_item_filters_to_expression
        for a canonical key that has deprecated aliases (string type)."""
        canonical = "db.system.name"
        assert canonical in ATTRIBUTES_TO_COALESCE, "test precondition: key must be coalesced"
        deprecated_keys = ATTRIBUTES_TO_COALESCE[canonical]

        item_filter = TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_STRING, name=canonical)
            )
        )
        expr = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(expr, FunctionCall)
        assert expr.function_name == "or"
        checked_keys = self._collect_existence_keys(expr)
        expected_keys = {canonical, *deprecated_keys}
        assert checked_keys == expected_keys

    def test_exists_filter_on_non_coalesced_attribute_unchanged(self) -> None:
        """Non-coalesced attributes should still produce a single has(mapKeys(...))."""
        item_filter = TraceItemFilter(
            exists_filter=ExistsFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_STRING, name="some.custom.tag")
            )
        )
        expr = trace_item_filters_to_expression(item_filter, attribute_key_to_expression)
        assert isinstance(expr, FunctionCall)
        assert expr.function_name == "has"


class TestSentryTimestampFilter:
    """Range filters on `sentry.timestamp` must target the raw DateTime `timestamp`
    column (index- and partition-prunable) rather than CAST(timestamp, 'Float64')."""

    @staticmethod
    def _range_filter(op: "ComparisonFilter.Op.ValueType") -> TraceItemFilter:
        return TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_DOUBLE, name="sentry.timestamp"),
                op=op,
                value=AttributeValue(val_double=1781040732),
            )
        )

    @pytest.mark.parametrize(
        "op,expected_function",
        [
            (ComparisonFilter.OP_LESS_THAN, "less"),
            (ComparisonFilter.OP_LESS_THAN_OR_EQUALS, "lessOrEquals"),
            (ComparisonFilter.OP_GREATER_THAN, "greater"),
            (ComparisonFilter.OP_GREATER_THAN_OR_EQUALS, "greaterOrEquals"),
        ],
    )
    def test_range_filter_uses_raw_timestamp_column(
        self, op: "ComparisonFilter.Op.ValueType", expected_function: str
    ) -> None:
        expr = trace_item_filters_to_expression(self._range_filter(op), attribute_key_to_expression)
        assert isinstance(expr, FunctionCall)
        assert expr.function_name == expected_function

        # LHS is the bare `timestamp` column, not a CAST.
        from snuba.query.expressions import Column as SnubaColumn

        lhs = expr.parameters[0]
        assert isinstance(lhs, SnubaColumn)
        assert lhs.column_name == "timestamp"

        # RHS is toDateTime(value) so it compares against a DateTime, enabling
        # primary-key and partition (toMonday(timestamp)) pruning.
        rhs = expr.parameters[1]
        assert isinstance(rhs, FunctionCall)
        assert rhs.function_name == "toDateTime"

    @pytest.mark.parametrize(
        "op,expected_second",
        [
            # `timestamp` is second-resolution, so fractional bounds round to the
            # integer second that preserves `CAST(timestamp, 'Float64') OP value`:
            # `<`/`>=` round up, `<=`/`>` round down.
            (ComparisonFilter.OP_LESS_THAN, 1781040733),
            (ComparisonFilter.OP_LESS_THAN_OR_EQUALS, 1781040732),
            (ComparisonFilter.OP_GREATER_THAN, 1781040732),
            (ComparisonFilter.OP_GREATER_THAN_OR_EQUALS, 1781040733),
        ],
    )
    def test_fractional_range_filter_rounds_to_preserve_semantics(
        self, op: "ComparisonFilter.Op.ValueType", expected_second: int
    ) -> None:
        fractional = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_DOUBLE, name="sentry.timestamp"),
                op=op,
                value=AttributeValue(val_double=1781040732.7),
            )
        )
        expr = trace_item_filters_to_expression(fractional, attribute_key_to_expression)
        assert isinstance(expr, FunctionCall)
        rhs = expr.parameters[1]
        assert isinstance(rhs, FunctionCall)
        assert rhs.function_name == "toDateTime"
        literal = rhs.parameters[0]
        assert isinstance(literal, Literal)
        expected = datetime.fromtimestamp(expected_second, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
        assert literal.value == expected

    def test_equals_filter_unchanged(self) -> None:
        """Non-range comparisons keep the existing CAST-based behavior."""
        expr = trace_item_filters_to_expression(
            self._range_filter(ComparisonFilter.OP_EQUALS), attribute_key_to_expression
        )
        # equals path wraps in the null-aware OR; the LHS of the equals is still the CAST.
        assert isinstance(expr, FunctionCall)
        assert expr.function_name == "or"
        equals_expr = expr.parameters[0]
        assert isinstance(equals_expr, FunctionCall)
        assert equals_expr.function_name == "equals"
        cast_expr = equals_expr.parameters[0]
        assert isinstance(cast_expr, FunctionCall)
        assert cast_expr.function_name == "cast"


class TestDedupeAndConditions:
    """dedupe_and_conditions removes structurally-identical top-level AND conjuncts.
    treeify_or_and_conditions runs it, so every endpoint that treeifies gets it."""

    @staticmethod
    def _conjuncts(condition: object) -> list[FunctionCall]:
        out: list[FunctionCall] = []
        if isinstance(condition, FunctionCall) and condition.function_name == "and":
            for param in condition.parameters:
                out.extend(TestDedupeAndConditions._conjuncts(param))
        elif isinstance(condition, FunctionCall):
            out.append(condition)
        return out

    def test_duplicate_conjunct_removed(self) -> None:
        a = f.greaterOrEquals(column("timestamp"), f.toDateTime("2026-06-09 21:30:00"))
        b = f.equals(column("project_id"), 1)
        query = Query(from_clause=None, condition=and_cond(a, b, a))

        dedupe_and_conditions(query)

        conjuncts = self._conjuncts(query.get_condition())
        assert len(conjuncts) == 2
        assert a in conjuncts and b in conjuncts

    def test_no_duplicates_leaves_condition_untouched(self) -> None:
        a = f.greaterOrEquals(column("timestamp"), f.toDateTime("2026-06-09 21:30:00"))
        b = f.less(column("timestamp"), f.toDateTime("2026-06-10 21:40:00"))
        original = and_cond(a, b)
        query = Query(from_clause=None, condition=original)

        dedupe_and_conditions(query)

        # Unchanged object identity: we only rebuild when a duplicate is found.
        assert query.get_condition() is original

    def test_treeify_also_dedupes(self) -> None:
        a = f.equals(column("project_id"), 1)
        b = f.equals(column("organization_id"), 2)
        query = Query(from_clause=None, condition=and_cond(a, b, a))

        treeify_or_and_conditions(query)

        conjuncts = self._conjuncts(query.get_condition())
        assert len(conjuncts) == 2


class TestAnalyzerSafeFilters:
    """Per-key filters on map-backed attributes are built from a NULL-free
    (has(mapKeys(...)), arrayElement) pair instead of the legacy
    if(<exists>, arrayElement, NULL) + isNull(...) idiom, so the new ClickHouse
    analyzer names the aggregate column consistently (SNUBA-B62/B6C/A13). The
    has(mapKeys(...)) existence guard preserves the absent-vs-empty distinction.
    """

    @staticmethod
    def _walk(expr: Expression) -> list[Expression]:
        nodes: list[Expression] = []

        def visit(node: Expression) -> Expression:
            nodes.append(node)
            return node

        expr.transform(visit)
        return nodes

    def _fn_names(self, expr: Expression) -> set[str]:
        return {n.function_name for n in self._walk(expr) if isinstance(n, FunctionCall)}

    def _assert_clean(self, expr: Expression) -> None:
        # No NULL literal, no leftover SubscriptableReference, no foldable if(...).
        for n in self._walk(expr):
            assert not (isinstance(n, Literal) and n.value is None)
            assert not isinstance(n, SubscriptableReference)
            assert not (isinstance(n, FunctionCall) and n.function_name == "if")

    def _build(
        self,
        op: ComparisonFilter.Op.ValueType,
        *,
        name: str = "some.custom.tag",
        value: str | None = None,
        values: list[str] | None = None,
        is_null: bool = False,
        ignore_case: bool = False,
    ) -> Expression:
        if values is not None:
            av = AttributeValue(val_str_array=StrArray(values=values))
        elif is_null:
            av = AttributeValue(val_null=True)
        else:
            av = AttributeValue(val_str=value or "")
        item_filter = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_STRING, name=name),
                op=op,
                value=av,
                ignore_case=ignore_case,
            )
        )
        return trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    # --- pruned forms: literal != column default, so no existence guard ---

    def test_in_pruned(self) -> None:
        expr = self._build(ComparisonFilter.OP_IN, values=["a", "b"])
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "in"
        assert "mapKeys" not in self._fn_names(expr)

    def test_not_in_pruned(self) -> None:
        expr = self._build(ComparisonFilter.OP_NOT_IN, values=["a", "b"])
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "not"
        names = self._fn_names(expr)
        assert {"not", "in", "arrayElement"} <= names
        assert "has" not in names and "mapKeys" not in names

    def test_equals_pruned(self) -> None:
        expr = self._build(ComparisonFilter.OP_EQUALS, value="ok")
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "equals"
        assert "mapKeys" not in self._fn_names(expr)

    def test_not_equals_pruned(self) -> None:
        expr = self._build(ComparisonFilter.OP_NOT_EQUALS, value="ok")
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "notEquals"
        assert "mapKeys" not in self._fn_names(expr)

    def test_coalesced_pruned_uses_multi_if_no_outer_guard(self) -> None:
        # Coalesced keys resolve via a NULL-free multiIf over per-key existence
        # checks (never coalesce(if(..., NULL), ...)); with a non-default literal
        # the outer existence OR-guard is dropped too.
        assert "transaction" in ATTRIBUTES_TO_COALESCE
        expr = self._build(ComparisonFilter.OP_EQUALS, name="transaction", value="t")
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "equals"
        names = self._fn_names(expr)
        assert "multiIf" in names and "coalesce" not in names
        assert "or" not in names  # no outer exists guard

    def test_in_ignore_case_lowers_array_element(self) -> None:
        expr = self._build(ComparisonFilter.OP_NOT_IN, values=["A", "B"], ignore_case=True)
        self._assert_clean(expr)
        (in_call,) = [
            n for n in self._walk(expr) if isinstance(n, FunctionCall) and n.function_name == "in"
        ]
        operand = in_call.parameters[0]
        assert isinstance(operand, FunctionCall) and operand.function_name == "lower"
        inner = operand.parameters[0]
        assert isinstance(inner, FunctionCall) and inner.function_name == "arrayElement"

    # --- guarded forms: literal IS the column default, so the existence guard is kept ---

    def test_equals_default_value_keeps_guard(self) -> None:
        expr = self._build(ComparisonFilter.OP_EQUALS, value="")  # '' could match an absent key
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "and"
        assert {"and", "has", "mapKeys", "equals", "arrayElement"} <= self._fn_names(expr)

    def test_in_with_default_value_keeps_guard(self) -> None:
        expr = self._build(ComparisonFilter.OP_IN, values=["", "x"])
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "and"
        assert {"and", "in", "has", "mapKeys"} <= self._fn_names(expr)

    def test_coalesced_default_value_keeps_guard(self) -> None:
        expr = self._build(ComparisonFilter.OP_EQUALS, name="transaction", value="")
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "and"
        assert {"and", "or", "equals", "multiIf", "has", "mapKeys"} <= self._fn_names(expr)

    # --- always-guarded forms: null comparisons and LIKE/NOT_LIKE ---

    def test_equals_null_is_not_map_contains(self) -> None:
        expr = self._build(ComparisonFilter.OP_EQUALS, is_null=True)  # attr = null <=> absent
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "not"
        assert self._fn_names(expr) == {"not", "has", "mapKeys"}

    def test_not_equals_null_is_map_contains(self) -> None:
        expr = self._build(ComparisonFilter.OP_NOT_EQUALS, is_null=True)  # attr != null <=> present
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "has"

    def test_in_null_value_builds_without_raising(self) -> None:
        # null isn't the column default, so the guard helper must treat it as a
        # non-default scalar rather than raising (see _scalar_value / val_null).
        expr = self._build(ComparisonFilter.OP_IN, is_null=True)
        assert "in" in self._fn_names(expr)

    def test_not_in_null_value_builds_without_raising(self) -> None:
        expr = self._build(ComparisonFilter.OP_NOT_IN, is_null=True)
        assert {"not", "in"} <= self._fn_names(expr)

    def test_like_keeps_guard(self) -> None:
        expr = self._build(ComparisonFilter.OP_LIKE, value="%ok%")
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "and"
        assert {"and", "has", "mapKeys", "like", "arrayElement"} <= self._fn_names(expr)

    def test_not_like_keeps_guard_and_negates_positive_like(self) -> None:
        expr = self._build(ComparisonFilter.OP_NOT_LIKE, value="%ok%")
        self._assert_clean(expr)
        assert isinstance(expr, FunctionCall) and expr.function_name == "not"
        names = self._fn_names(expr)
        assert {"not", "and", "has", "mapKeys", "like", "arrayElement"} <= names
        assert "notLike" not in names  # positive like under the negation
        ilike = self._build(ComparisonFilter.OP_NOT_LIKE, value="%ok%", ignore_case=True)
        assert "ilike" in self._fn_names(ilike) and "notILike" not in self._fn_names(ilike)


class TestBooleanAttributeFilters:
    """Boolean attributes live in the ``attributes_bool`` map but resolve to a bare
    ``arrayElement`` (their map has no hash buckets), so they must still be routed through
    the map-backed ``(exists, value)`` path. Otherwise a missing key reads as the ``false``
    column default and ``attr:false`` matches items that lack the attribute entirely
    (getsentry/sentry#119735).
    """

    @staticmethod
    def _walk(expr: Expression) -> list[Expression]:
        nodes: list[Expression] = []

        def visit(node: Expression) -> Expression:
            nodes.append(node)
            return node

        expr.transform(visit)
        return nodes

    def _fn_names(self, expr: Expression) -> set[str]:
        return {n.function_name for n in self._walk(expr) if isinstance(n, FunctionCall)}

    def _build(
        self,
        op: ComparisonFilter.Op.ValueType,
        *,
        value: AttributeValue,
        name: str = "hasCodeTag",
    ) -> Expression:
        item_filter = TraceItemFilter(
            comparison_filter=ComparisonFilter(
                key=AttributeKey(type=AttributeKey.Type.TYPE_BOOLEAN, name=name),
                op=op,
                value=value,
            )
        )
        return trace_item_filters_to_expression(item_filter, attribute_key_to_expression)

    def test_equals_false_keeps_existence_guard(self) -> None:
        # `attr:false` must not match items missing the attribute: `false` is the column
        # default that an absent key also reads as, so the existence guard is required.
        expr = self._build(ComparisonFilter.OP_EQUALS, value=AttributeValue(val_bool=False))
        assert isinstance(expr, FunctionCall) and expr.function_name == "and"
        assert {"and", "has", "mapKeys", "equals", "arrayElement"} <= self._fn_names(expr)

    def test_equals_true_needs_no_guard(self) -> None:
        # `true` is never the column default, so an absent key can't match and no guard is
        # needed — the bare equality is emitted.
        expr = self._build(ComparisonFilter.OP_EQUALS, value=AttributeValue(val_bool=True))
        assert isinstance(expr, FunctionCall) and expr.function_name == "equals"
        assert "mapKeys" not in self._fn_names(expr)

    def test_not_equals_false_negates_guarded_equality(self) -> None:
        expr = self._build(ComparisonFilter.OP_NOT_EQUALS, value=AttributeValue(val_bool=False))
        assert isinstance(expr, FunctionCall) and expr.function_name == "not"
        assert {"not", "and", "has", "mapKeys", "equals", "arrayElement"} <= self._fn_names(expr)

    def test_uses_attributes_bool_column(self) -> None:
        expr = self._build(ComparisonFilter.OP_EQUALS, value=AttributeValue(val_bool=False))
        columns = {n.column_name for n in self._walk(expr) if isinstance(n, ColumnExpr)}
        assert columns == {"attributes_bool"}


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_convert_rpc_exception_to_proto_packs_details() -> None:
    routing_decision_log_dict = {
        "can_run": False,
        "is_throttled": True,
        "strategy": "OutcomesBasedRoutingStrategy",
        "source_request_id": "21df09b9-41a8-4529-9fc7-d75c851adbfe",
        "extra_info": {
            "sampling_in_storage_estimation_time_overhead": {
                "type": "timing",
                "value": 10,
                "tags": None,
            }
        },
        "clickhouse_settings": {"max_threads": 0},
        "result_info": {},
        "routed_tier": "TIER_1",
        "allocation_policies_recommendations": {
            "RejectionPolicy": {
                "can_run": False,
                "max_threads": 0,
                "explanation": {
                    "reason": "policy rejects all queries",
                    "storage_key": "doesntmatter",
                },
                "is_throttled": True,
                "throttle_threshold": 1000000000000,
                "rejection_threshold": 1000000000000,
                "quota_used": 0,
                "quota_unit": "no_units",
                "suggestion": "no_suggestion",
                "max_bytes_to_read": 0,
            }
        },
    }

    exc = RPCAllocationPolicyException(
        "Query cannot be run due to routing strategy deciding it cannot run, most likely due to allocation policies",
        routing_decision_log_dict,
    )

    proto = convert_rpc_exception_to_proto(exc)
    assert isinstance(proto, ErrorProto)
    assert proto.code == 429
    assert (
        proto.message
        == "Query cannot be run due to routing strategy deciding it cannot run, most likely due to allocation policies"
    )

    s = struct_pb2.Struct()
    assert proto.details[0].Unpack(s)
    unpacked = json_format.MessageToDict(s)
    assert unpacked == routing_decision_log_dict


class TestAnyAttributeFilter:
    def test_like_on_non_string_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_LIKE,
            value=AttributeValue(val_int=42),
        )
        with pytest.raises(BadSnubaRPCRequestException, match="LIKE/NOT_LIKE"):
            _any_attribute_filter_to_expression(filt)

    def test_not_like_on_non_string_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_LIKE,
            value=AttributeValue(val_int=42),
        )
        with pytest.raises(BadSnubaRPCRequestException, match="LIKE/NOT_LIKE"):
            _any_attribute_filter_to_expression(filt)

    def test_no_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
        )
        with pytest.raises(BadSnubaRPCRequestException, match="does not have a value"):
            _any_attribute_filter_to_expression(filt)

    def test_ignore_case_on_non_string_equals_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_int=42),
            ignore_case=True,
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="Cannot ignore case on non-string values"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_ignore_case_on_non_string_in_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_IN,
            value=AttributeValue(
                val_array=Array(values=[AttributeValue(val_int=1), AttributeValue(val_int=2)])
            ),
            ignore_case=True,
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="Cannot ignore case on non-string values"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_in_with_scalar_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_IN,
            value=AttributeValue(val_str="hello"),
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="IN/NOT_IN operations require an array value type"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_not_in_with_scalar_value_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_IN,
            value=AttributeValue(val_int=42),
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="IN/NOT_IN operations require an array value type"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_in_with_empty_array_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_IN,
            value=AttributeValue(val_array=Array(values=[])),
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="IN/NOT_IN operations require a non-empty array"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_not_in_with_empty_array_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_NOT_IN,
            value=AttributeValue(val_array=Array(values=[])),
        )
        with pytest.raises(
            BadSnubaRPCRequestException, match="IN/NOT_IN operations require a non-empty array"
        ):
            _any_attribute_filter_to_expression(filt)

    def test_unsupported_value_type_raises(self) -> None:
        filt = AnyAttributeFilter(
            op=AnyAttributeFilter.OP_EQUALS,
            value=AttributeValue(val_null=True),
        )
        with pytest.raises(BadSnubaRPCRequestException, match="does not have a value"):
            _any_attribute_filter_to_expression(filt)

    @staticmethod
    def _in_over_arrays(expr: Expression) -> list[FunctionCall]:
        return [
            e
            for e in expr
            if isinstance(e, FunctionCall)
            and e.function_name == "in"
            and len(e.parameters) == 2
            and isinstance(e.parameters[1], FunctionCall)
            and e.parameters[1].function_name == "array"
        ]

    @staticmethod
    def _has_over(expr: Expression, values: list[str]) -> list[FunctionCall]:
        return [
            e
            for e in expr
            if isinstance(e, FunctionCall)
            and e.function_name == "has"
            and isinstance(e.parameters[0], FunctionCall)
            and e.parameters[0].function_name == "array"
            and [p.value for p in e.parameters[0].parameters if isinstance(p, Literal)] == values
        ]

    @pytest.mark.parametrize("op", [AnyAttributeFilter.OP_IN, AnyAttributeFilter.OP_NOT_IN])
    def test_membership_as_has_for_select_clause(self, op: AnyAttributeFilter.Op.ValueType) -> None:
        """Regression guard for SNUBA-9W6 / SNUBA-A1W (mixed-version distributed reads).

        An any-attribute ``IN``/``NOT_IN`` builds ``in(x, array(...))`` inside an
        ``arrayExists`` lambda. When that filter lands in a SELECT-clause aggregate, the
        constant ``IN`` set's ``__set_<hash>`` identifier leaks into the result-block
        column name and breaks reads across mixed-version ``Remote`` nodes. With
        ``membership_as_has=True`` the comparison must be ``has(array(...), x)`` instead;
        the default (WHERE) keeps ``in()`` so the prepared set still drives pruning.
        """
        values = ["error", "internal_error"]
        filt = AnyAttributeFilter(
            op=op,
            value=AttributeValue(
                val_array=Array(values=[AttributeValue(val_str=v) for v in values])
            ),
        )

        # Default (WHERE) keeps the in() set inside arrayExists.
        where_expr = _any_attribute_filter_to_expression(filt)
        assert self._in_over_arrays(where_expr), "WHERE form must keep in() over the constant array"

        # SELECT-clause form uses has(array, x) and builds no in() set.
        select_expr = _any_attribute_filter_to_expression(filt, membership_as_has=True)
        assert not self._in_over_arrays(select_expr), (
            "membership_as_has must replace in() over a constant array with has()"
        )
        assert self._has_over(select_expr, values), "expected has(array(values), x) in the lambda"


@pytest.mark.eap
@pytest.mark.redis_db
class TestAnyAttributeFilterIntegration:
    """Integration tests that insert spans into ClickHouse and query them
    with AnyAttributeFilter via EndpointTraceItemTable."""

    UNIQUE_VALUE = f"needle-{uuid.uuid4().hex[:8]}"

    @pytest.fixture(autouse=True)
    def setup(self, eap: None, redis_db: None) -> None:
        self.base_time = datetime.now(tz=UTC).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(hours=1)
        self.start_ts = Timestamp(seconds=int((self.base_time - timedelta(hours=1)).timestamp()))
        self.end_ts = Timestamp(seconds=int((self.base_time + timedelta(hours=2)).timestamp()))

        # Span 0: the target — has the unique needle value on "haystack" attr
        # Span 1: a decoy with a different value
        # Span 2: another decoy with no "haystack" attribute at all
        messages = [
            gen_item_message(
                start_timestamp=self.base_time,
                attributes={
                    "haystack": AnyValue(string_value=self.UNIQUE_VALUE),
                    "color": AnyValue(string_value="red"),
                },
            ),
            gen_item_message(
                start_timestamp=self.base_time + timedelta(minutes=1),
                attributes={
                    "haystack": AnyValue(string_value="decoy-value"),
                    "color": AnyValue(string_value="blue"),
                },
            ),
            gen_item_message(
                start_timestamp=self.base_time + timedelta(minutes=2),
                attributes={
                    "color": AnyValue(string_value="green"),
                },
            ),
        ]
        storage = get_writable_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(storage, messages)

    def _execute(self, filt: TraceItemFilter) -> list[str]:
        """Run a TraceItemTable query with the given filter, returning
        the matched 'color' attribute values as a sorted list."""
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test",
                start_timestamp=self.start_ts,
                end_timestamp=self.end_ts,
                request_id=uuid.uuid4().hex,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=filt,
            columns=[
                Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color")),
            ],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color"))
                )
            ],
            limit=100,
        )
        response = EndpointTraceItemTable().execute(message)
        if not response.column_values:
            return []
        return sorted(r.val_str for r in response.column_values[0].results)

    def test_equals_finds_target_span(self) -> None:
        """OP_EQUALS on a unique string value should return only the target span."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_EQUALS,
                    value=AttributeValue(val_str=self.UNIQUE_VALUE),
                )
            )
        )
        assert colors == ["red"]

    def test_like_finds_target_span(self) -> None:
        """OP_LIKE with a pattern matching the unique value finds the target."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_LIKE,
                    value=AttributeValue(val_str=f"%{self.UNIQUE_VALUE}%"),
                )
            )
        )
        assert colors == ["red"]

    def test_not_equals_excludes_target_span(self) -> None:
        """OP_NOT_EQUALS on the unique value should return the other two spans."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_NOT_EQUALS,
                    value=AttributeValue(val_str=self.UNIQUE_VALUE),
                )
            )
        )
        assert colors == ["blue", "green"]

    def test_in_finds_target_span(self) -> None:
        """OP_IN with an array containing the unique value finds the target."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_IN,
                    value=AttributeValue(
                        val_array=Array(
                            values=[
                                AttributeValue(val_str=self.UNIQUE_VALUE),
                                AttributeValue(val_str="no-match"),
                            ]
                        )
                    ),
                )
            )
        )
        assert colors == ["red"]

    def test_equals_ignore_case(self) -> None:
        """OP_EQUALS with ignore_case matches regardless of casing."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_EQUALS,
                    value=AttributeValue(val_str=self.UNIQUE_VALUE.upper()),
                    ignore_case=True,
                )
            )
        )
        assert colors == ["red"]

    def test_no_match_returns_empty(self) -> None:
        """Searching for a value that doesn't exist returns nothing."""
        colors = self._execute(
            TraceItemFilter(
                any_attribute_filter=AnyAttributeFilter(
                    op=AnyAttributeFilter.OP_EQUALS,
                    value=AttributeValue(val_str="value-that-does-not-exist"),
                )
            )
        )
        assert colors == []


@pytest.mark.eap
@pytest.mark.redis_db
class TestEmptyVsAbsentComparison:
    """End-to-end: the migrated per-key comparisons (EQUALS / NOT_EQUALS / LIKE /
    NOT_LIKE) must keep distinguishing a *stored empty value* from an *absent
    key*, since arrayElement reads both as '' and only the has(mapKeys(...))
    existence guard tells them apart. Three spans, by the attribute under test:
      - present, value "ok"  (color "red")
      - present, empty ""    (color "blue")
      - absent               (color "green")

    A unique per-run batch tag (present on all three rows and AND-ed into every
    query) isolates these rows, so the absent-key cases aren't polluted by other
    rows in the shared table (which also lack the attribute under test).
    """

    # Custom attribute names not present in gen_item_message's default set
    # (which injects e.g. status="ok"), so we fully control present/empty/absent.
    ATTR = "test.empty_vs_absent.status"
    BATCH_ATTR = "test.empty_vs_absent.batch"

    @pytest.fixture(autouse=True)
    def setup(self, eap: None, redis_db: None) -> None:
        self.batch = f"batch-{uuid.uuid4().hex}"
        self.base_time = datetime.now(tz=UTC).replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(hours=1)
        self.start_ts = Timestamp(seconds=int((self.base_time - timedelta(hours=1)).timestamp()))
        self.end_ts = Timestamp(seconds=int((self.base_time + timedelta(hours=2)).timestamp()))
        batch = AnyValue(string_value=self.batch)
        messages = [
            gen_item_message(
                start_timestamp=self.base_time,
                attributes={
                    self.BATCH_ATTR: batch,
                    self.ATTR: AnyValue(string_value="ok"),
                    "color": AnyValue(string_value="red"),
                },
            ),
            gen_item_message(
                start_timestamp=self.base_time + timedelta(minutes=1),
                attributes={
                    self.BATCH_ATTR: batch,
                    self.ATTR: AnyValue(string_value=""),
                    "color": AnyValue(string_value="blue"),
                },
            ),
            gen_item_message(
                start_timestamp=self.base_time + timedelta(minutes=2),
                attributes={
                    self.BATCH_ATTR: batch,
                    "color": AnyValue(string_value="green"),
                },
            ),
        ]
        storage = get_writable_storage(StorageKey("eap_items"))
        write_raw_unprocessed_events(storage, messages)

    def _execute(
        self,
        op: ComparisonFilter.Op.ValueType,
        *,
        value: str | None = None,
        is_null: bool = False,
    ) -> list[str]:
        av = AttributeValue(val_null=True) if is_null else AttributeValue(val_str=value or "")
        filt = TraceItemFilter(
            and_filter=AndFilter(
                filters=[
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name=self.BATCH_ATTR),
                            op=ComparisonFilter.OP_EQUALS,
                            value=AttributeValue(val_str=self.batch),
                        )
                    ),
                    TraceItemFilter(
                        comparison_filter=ComparisonFilter(
                            key=AttributeKey(type=AttributeKey.TYPE_STRING, name=self.ATTR),
                            op=op,
                            value=av,
                        )
                    ),
                ]
            )
        )
        message = TraceItemTableRequest(
            meta=RequestMeta(
                project_ids=[1],
                organization_id=1,
                cogs_category="test",
                referrer="test",
                start_timestamp=self.start_ts,
                end_timestamp=self.end_ts,
                request_id=uuid.uuid4().hex,
                trace_item_type=TraceItemType.TRACE_ITEM_TYPE_SPAN,
            ),
            filter=filt,
            columns=[Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color"))],
            order_by=[
                TraceItemTableRequest.OrderBy(
                    column=Column(key=AttributeKey(type=AttributeKey.TYPE_STRING, name="color"))
                )
            ],
            limit=100,
        )
        response = EndpointTraceItemTable().execute(message)
        if not response.column_values:
            return []
        return sorted(r.val_str for r in response.column_values[0].results)

    def test_equals_empty_matches_only_stored_empty(self) -> None:
        # The critical case: `status = ''` must match the stored empty, NOT the
        # absent key (arrayElement reads both as '').
        assert self._execute(ComparisonFilter.OP_EQUALS, value="") == ["blue"]

    def test_equals_value_matches_only_that_value(self) -> None:
        assert self._execute(ComparisonFilter.OP_EQUALS, value="ok") == ["red"]

    def test_equals_null_matches_only_absent(self) -> None:
        assert self._execute(ComparisonFilter.OP_EQUALS, is_null=True) == ["green"]

    def test_not_equals_value_includes_empty_and_absent(self) -> None:
        assert self._execute(ComparisonFilter.OP_NOT_EQUALS, value="ok") == ["blue", "green"]

    def test_not_equals_empty_includes_value_and_absent(self) -> None:
        assert self._execute(ComparisonFilter.OP_NOT_EQUALS, value="") == ["green", "red"]

    def test_not_equals_null_matches_only_present(self) -> None:
        assert self._execute(ComparisonFilter.OP_NOT_EQUALS, is_null=True) == ["blue", "red"]

    def test_like_wildcard_matches_present_not_absent(self) -> None:
        # '%' matches both present rows (incl. the stored empty) but never the
        # absent key.
        assert self._execute(ComparisonFilter.OP_LIKE, value="%") == ["blue", "red"]

    def test_not_like_wildcard_matches_only_absent(self) -> None:
        # Present rows all `like '%'`, so only the absent key survives NOT LIKE.
        assert self._execute(ComparisonFilter.OP_NOT_LIKE, value="%") == ["green"]


class TestAnyAttributeFilterOption:
    """The `enable_any_attribute_filter` sentry-option gates whether
    any_attribute_filter is translated into a predicate or treated as
    always-true. It replaces the former `enable_any_attribute_filter`
    runtime config."""

    @staticmethod
    def _filter() -> TraceItemFilter:
        return TraceItemFilter(
            any_attribute_filter=AnyAttributeFilter(
                op=AnyAttributeFilter.OP_EQUALS,
                value=AttributeValue(val_str="foo"),
            )
        )

    def test_enabled_by_default_translates_filter(self) -> None:
        # Schema default is true: the filter is translated, not short-circuited.
        result = trace_item_filters_to_expression(self._filter(), attribute_key_to_expression)
        assert isinstance(result, FunctionCall)
        assert result.function_name == "arrayExists"

    def test_disabled_returns_always_true(self) -> None:
        with override_options("snuba", {"enable_any_attribute_filter": False}):
            result = trace_item_filters_to_expression(self._filter(), attribute_key_to_expression)
        assert isinstance(result, Literal)
        assert result.value is True
