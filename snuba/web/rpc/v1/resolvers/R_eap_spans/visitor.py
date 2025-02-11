from abc import ABC, abstractmethod
from dataclasses import replace
from typing import Iterable

from sentry_protos.snuba.v1.endpoint_trace_item_table_pb2 import (
    AggregationFilter,
    Column,
    TraceItemTableRequest,
)
from sentry_protos.snuba.v1.request_common_pb2 import PageToken, RequestMeta
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeKey,
    VirtualColumnContext,
)
from sentry_protos.snuba.v1.trace_item_filter_pb2 import TraceItemFilter

from snuba.query.expressions import Expression
from snuba.query.logical import Query
from snuba.web.rpc.common.common import base_conditions_and
from snuba.web.rpc.common.exceptions import BadSnubaRPCRequestException
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.common import (
    attribute_key_to_expression,
)
from snuba.web.rpc.v1.resolvers.common.aggregation import (
    aggregation_to_expression,
)
from snuba.query.dsl import Functions as f


class TraceItemTableRequestVisitor(ABC):
    @abstractmethod
    def visit_trace_item_table_request(self, request: TraceItemTableRequest) -> None:
        pass

    @abstractmethod
    def visit_meta(self, meta: RequestMeta) -> None:
        pass

    @abstractmethod
    def visit_columns(self, columns: Iterable[Column]) -> None:
        pass

    @abstractmethod
    def visit_filter(self, filter: TraceItemFilter) -> None:
        pass

    @abstractmethod
    def visit_order_by(self, order_by: Iterable[TraceItemTableRequest.OrderBy]) -> None:
        pass

    @abstractmethod
    def visit_group_by(self, group_by: Iterable[AttributeKey]) -> None:
        pass

    @abstractmethod
    def visit_limit(self, limit: int) -> None:
        pass

    @abstractmethod
    def visit_page_token(self, page_token: PageToken) -> None:
        pass

    @abstractmethod
    def visit_virtual_column_contexts(
        self, virtual_column_contexts: Iterable[VirtualColumnContext]
    ) -> None:
        pass

    @abstractmethod
    def visit_aggregation_filter(self, aggregation_filter: AggregationFilter) -> None:
        pass


class RequestToQueryTranslator(TraceItemTableRequestVisitor):
    OP_TO_EXPR = {
        Column.BinaryFormula.OP_ADD: f.plus,
        Column.BinaryFormula.OP_SUBTRACT: f.minus,
        Column.BinaryFormula.OP_MULTIPLY: f.multiply,
        Column.BinaryFormula.OP_DIVIDE: f.divide,
    }

    def __init__(self) -> None:
        self.query = Query(from_clause=None)

    def visit_trace_item_table_request(self, request: TraceItemTableRequest) -> None:
        self.visit_meta(request.meta)
        self.visit_columns(request.columns)
        self.visit_filter(request.filter)
        self.visit_order_by(request.order_by)
        self.visit_group_by(request.group_by)
        self.visit_limit(request.limit)
        self.visit_page_token(request.page_token)
        self.visit_virtual_column_contexts(request.virtual_column_contexts)
        self.visit_aggregation_filter(request.aggregation_filter)

    def visit_meta(self, meta: RequestMeta) -> None:
        self.query.set_ast_condition(base_conditions_and(meta))

    def visit_columns(self, columns: Iterable[Column]) -> None:
        for column in columns:
            self.visit_column(column)

    def visit_column(self, column: Column) -> None:
        # The key_col expression alias may differ from the column label. That is okay
        # the attribute key name is used in the groupby, the column label is just the name of
        # the returned attribute value
        self.query.set_ast_selected_columns(
            self.query.get_selected_columns()
            + [
                SelectedExpression(
                    name=column.label, expression=self._column_to_expression(column)
                )
            ]
            + _get_reliability_context_columns(column),
        )

    def visit_filter(self, filter: TraceItemFilter) -> None:
        pass

    def visit_order_by(self, order_by: Iterable[TraceItemTableRequest.OrderBy]) -> None:
        pass

    def visit_group_by(self, group_by: Iterable[AttributeKey]) -> None:
        pass

    def visit_limit(self, limit: int) -> None:
        pass

    def visit_page_token(self, page_token: PageToken) -> None:
        pass

    def visit_virtual_column_contexts(
        self, virtual_column_contexts: Iterable[VirtualColumnContext]
    ) -> None:
        pass

    def visit_aggregation_filter(self, aggregation_filter: AggregationFilter) -> None:
        pass

    def _column_to_expression(self, column: Column) -> Expression:
        """
        Given a column protobuf object, translates it into a Expression object and returns it.
        """
        if column.HasField("key"):
            return attribute_key_to_expression(column.key)
        elif column.HasField("aggregation"):
            function_expr = aggregation_to_expression(column.aggregation)
            # aggregation label may not be set and the column label takes priority anyways.
            function_expr = replace(function_expr, alias=column.label)
            return function_expr
        elif column.HasField("formula"):
            formula_expr = self._formula_to_expression(column.formula)
            formula_expr = replace(formula_expr, alias=column.label)
            return formula_expr
        else:
            raise BadSnubaRPCRequestException(
                "Column is not one of: aggregate, attribute key, or formula"
            )

    def _formula_to_expression(self, formula: Column.BinaryFormula) -> Expression:
        return self.OP_TO_EXPR[formula.op](
            self._column_to_expression(formula.left),
            self._column_to_expression(formula.right),
        )
