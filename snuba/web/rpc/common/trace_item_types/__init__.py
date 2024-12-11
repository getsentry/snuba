from abc import ABC, abstractmethod
from typing import Sequence

from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    VirtualColumnContext,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    ComparisonFilter,
    TraceItemFilter,
)

from snuba.query import Expression, Query
from snuba.query.data_source.simple import Entity
from snuba.query.dsl import Functions as f
from snuba.query.dsl import (
    and_cond,
    in_cond,
    literal,
    literals_array,
    not_cond,
    or_cond,
)
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException


class SnubaRPCBridge(ABC):
    """
    Classes that implement this abstract class are used to map RPC data to/from snuba data
    in a dataset-specific way.

    For example, the RPC filter 'exists(trace_id)' might go to `mapContains(attr_str, trace_id)` in one
    dataset, and `trace_id is not null` in another.
    """

    @abstractmethod
    def attribute_key_to_expression(self, attr_key: AttributeKey) -> Expression:
        pass

    @abstractmethod
    def get_snuba_entity(self) -> Entity:
        pass

    @abstractmethod
    def _attribute_key_exists_expression(self, attr_key: AttributeKey) -> Expression:
        pass

    def trace_item_filters_to_expression(
        self, item_filter: TraceItemFilter
    ) -> Expression:
        """
        Trace Item Filters are things like (span.id=12345 AND start_timestamp >= "june 4th, 2024")
        This maps those filters into an expression which can be used in a WHERE clause
        :param item_filter: the RPC-formatted filters
        :return: a snuba expression that can be sent onwards to clickhouse
        """
        if item_filter.HasField("and_filter"):
            filters = item_filter.and_filter.filters
            if len(filters) == 0:
                return literal(True)
            if len(filters) == 1:
                return self.trace_item_filters_to_expression(filters[0])
            return and_cond(
                *(self.trace_item_filters_to_expression(x) for x in filters)
            )

        if item_filter.HasField("or_filter"):
            filters = item_filter.or_filter.filters
            if len(filters) == 0:
                raise BadSnubaRPCRequestException(
                    "Invalid trace item filter, empty 'or' clause"
                )
            if len(filters) == 1:
                return self.trace_item_filters_to_expression(filters[0])
            return or_cond(*(self.trace_item_filters_to_expression(x) for x in filters))

        if item_filter.HasField("comparison_filter"):
            k = item_filter.comparison_filter.key
            k_expression = self.attribute_key_to_expression(k)
            op = item_filter.comparison_filter.op
            v = item_filter.comparison_filter.value

            value_type = v.WhichOneof("value")
            if value_type is None:
                raise BadSnubaRPCRequestException(
                    "comparison does not have a right hand side"
                )

            match value_type:
                case "val_bool":
                    v_expression: Expression = literal(v.val_bool)
                case "val_str":
                    v_expression = literal(v.val_str)
                case "val_float":
                    v_expression = literal(v.val_float)
                case "val_int":
                    v_expression = literal(v.val_int)
                case "val_null":
                    v_expression = literal(None)
                case "val_str_array":
                    v_expression = literals_array(
                        None, list(map(lambda x: literal(x), v.val_str_array.values))
                    )
                case "val_int_array":
                    v_expression = literals_array(
                        None, list(map(lambda x: literal(x), v.val_int_array.values))
                    )
                case "val_float_array":
                    v_expression = literals_array(
                        None, list(map(lambda x: literal(x), v.val_float_array.values))
                    )
                case default:
                    raise NotImplementedError(
                        f"translation of AttributeValue type {default} is not implemented"
                    )

            if op == ComparisonFilter.OP_EQUALS:
                return f.equals(k_expression, v_expression)
            if op == ComparisonFilter.OP_NOT_EQUALS:
                return f.notEquals(k_expression, v_expression)
            if op == ComparisonFilter.OP_LIKE:
                if k.type != AttributeKey.Type.TYPE_STRING:
                    raise BadSnubaRPCRequestException(
                        "the LIKE comparison is only supported on string keys"
                    )
                return f.like(k_expression, v_expression)
            if op == ComparisonFilter.OP_NOT_LIKE:
                if k.type != AttributeKey.Type.TYPE_STRING:
                    raise BadSnubaRPCRequestException(
                        "the NOT LIKE comparison is only supported on string keys"
                    )
                return f.notLike(k_expression, v_expression)
            if op == ComparisonFilter.OP_LESS_THAN:
                return f.less(k_expression, v_expression)
            if op == ComparisonFilter.OP_LESS_THAN_OR_EQUALS:
                return f.lessOrEquals(k_expression, v_expression)
            if op == ComparisonFilter.OP_GREATER_THAN:
                return f.greater(k_expression, v_expression)
            if op == ComparisonFilter.OP_GREATER_THAN_OR_EQUALS:
                return f.greaterOrEquals(k_expression, v_expression)
            if op == ComparisonFilter.OP_IN:
                return in_cond(k_expression, v_expression)
            if op == ComparisonFilter.OP_NOT_IN:
                return not_cond(in_cond(k_expression, v_expression))

            raise BadSnubaRPCRequestException(
                f"Invalid string comparison, unknown op: {item_filter.comparison_filter}"
            )

        if item_filter.HasField("exists_filter"):
            return self._attribute_key_exists_expression(item_filter.exists_filter.key)

        return literal(True)

    @abstractmethod
    def apply_virtual_columns(
        self, query: Query, virtual_column_contexts: Sequence[VirtualColumnContext]
    ) -> None:
        """Injects virtual column mappings into the clickhouse query. Works with NORMALIZED_COLUMNS on the table or
        dynamic columns in attr_str

        attr_num not supported because mapping on floats is a bad idea

        Example:

            SELECT
              project_name AS `project_name`,
              attr_str['release'] AS `release`,
              attr_str['sentry.sdk.name'] AS `sentry.sdk.name`,
            ... rest of query

            contexts:
                [   {from_column_name: project_id, to_column_name: project_name, value_map: {1: "sentry", 2: "snuba"}} ]


            Query will be transformed into:

            SELECT
            -- see the project name column transformed and the value mapping injected
              transform( CAST( project_id, 'String'), array( '1', '2'), array( 'sentry', 'snuba'), 'unknown') AS `project_name`,
            --
              attr_str['release'] AS `release`,
              attr_str['sentry.sdk.name'] AS `sentry.sdk.name`,
            ... rest of query

        """
        pass
