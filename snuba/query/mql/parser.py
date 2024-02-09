from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union

import sentry_sdk
from parsimonious.nodes import Node, NodeVisitor
from snuba_sdk.metrics_visitors import AGGREGATE_ALIAS
from snuba_sdk.mql.mql import MQL_GRAMMAR

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    build_match,
    combine_and_conditions,
)
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.indexer.resolver import resolve_mappings
from snuba.query.logical import Query as LogicalQuery
from snuba.query.mql.mql_context import MQLContext
from snuba.query.parser.exceptions import ParsingException
from snuba.query.query_settings import QuerySettings
from snuba.query.snql.anonymize import format_snql_anonymized
from snuba.query.snql.parser import (
    MAX_LIMIT,
    POST_PROCESSORS,
    VALIDATORS,
    _post_process,
    _replace_time_condition,
    _treeify_or_and_conditions,
)
from snuba.state import explain_meta
from snuba.util import parse_datetime
from snuba.utils.constants import GRANULARITIES_AVAILABLE

# The parser returns a bunch of different types, so create a single aggregate type to
# capture everything.
MQLSTUFF = Dict[str, Union[str, list[SelectedExpression], list[Expression]]]
logger = logging.getLogger("snuba.mql.parser")


@dataclass
class InitialParseResult:
    expression: SelectedExpression | None = None
    formula: str | None = None
    parameters: list[InitialParseResult] | None = None
    groupby: list[SelectedExpression] | None = None
    conditions: list[Expression] | None = None
    mri: str | None = None
    public_name: str | None = None


ARITHMETIC_OPERATORS_MAPPING = {
    "+": "plus",
    "-": "minus",
    "*": "multiply",
    "/": "divide",
}

COMMON_AGGREGATION_FUNCTIONS = [
    "count",
    "min",
    "max",
    "sum",
    "avg",
    "last",
    "uniq",
]


class MQLVisitor(NodeVisitor):  # type: ignore
    """
    Builds the arguments for a Snuba AST from the MQL Parsimonious parse tree.
    """

    def visit_expression(
        self,
        node: Node,
        children: Tuple[
            InitialParseResult,
            Any,
        ],
    ) -> InitialParseResult:
        term, zero_or_more_others = children
        if zero_or_more_others:
            _, term_operator, _, coefficient, *_ = zero_or_more_others[0]
            return self._visit_formula(term_operator, term, coefficient)
        return term

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        return ARITHMETIC_OPERATORS_MAPPING[node.text]

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> Any:
        return ARITHMETIC_OPERATORS_MAPPING[node.text]

    def _build_timeseries_formula_param(
        self, param: InitialParseResult
    ) -> SelectedExpression:
        """
        Timeseries inside a formula need to have three things done to them:
        1. Add the -If suffix to their aggregate function
        2. Put all the filters for the timeseries inside the aggregate expression
        3. Add a metric_id condition to the conditions in the aggregate

        Given an input parse result from `sum(mri){x:y}`, this should output
        an expression like `sumIf(value, x = y AND metric_id = mri)`.

        If the timeseries inside the formula query is different entity type, then
        the select expression should be left alone. But we don't know this until later.
        For now, we update the expression with aggIf, then overwrite it later.
        """
        assert param.expression is not None
        exp = param.expression.expression
        assert isinstance(exp, (FunctionCall, CurriedFunctionCall))

        conditions = param.conditions or []
        metric_id_condition = binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "metric_id"),
            Literal(None, param.mri or param.public_name),
        )
        conditions.append(metric_id_condition)
        value_column = exp.parameters[0]
        if isinstance(exp, FunctionCall):
            return SelectedExpression(
                None,
                FunctionCall(
                    None,
                    f"{exp.function_name}If",
                    parameters=(
                        value_column,
                        combine_and_conditions(conditions),
                    ),
                ),
            )
        else:
            return SelectedExpression(
                None,
                CurriedFunctionCall(
                    None,
                    FunctionCall(
                        exp.internal_function.alias,
                        f"{exp.internal_function.function_name}If",
                        exp.internal_function.parameters,
                    ),
                    (
                        value_column,
                        combine_and_conditions(conditions),
                    ),
                ),
            )

    def _visit_formula(
        self,
        term_operator: str,
        term: InitialParseResult,
        coefficient: InitialParseResult,
    ) -> InitialParseResult:
        # TODO: If the formula has filters/group by, where do those appear?

        # If the parameters of the query are timeseries, extract the expressions from the result
        if isinstance(term, InitialParseResult) and term.expression is not None:
            term = replace(term, expression=self._build_timeseries_formula_param(term))
        if (
            isinstance(coefficient, InitialParseResult)
            and coefficient.expression is not None
        ):
            coefficient = replace(
                coefficient,
                expression=self._build_timeseries_formula_param(coefficient),
            )

        if (
            isinstance(term, InitialParseResult)
            and isinstance(coefficient, InitialParseResult)
            and term.groupby != coefficient.groupby
        ):
            raise InvalidQueryException(
                "All terms in a formula must have the same groupby"
            )

        return InitialParseResult(
            expression=None,
            formula=term_operator,
            parameters=[term, coefficient],
            groupby=term.groupby,
        )

    def visit_term(
        self,
        node: Node,
        children: Tuple[InitialParseResult, Any],
    ) -> InitialParseResult:
        term, zero_or_more_others = children
        if zero_or_more_others:
            _, term_operator, _, coefficient, *_ = zero_or_more_others[0]
            return self._visit_formula(term_operator, term, coefficient)

        return term

    def visit_coefficient(
        self,
        node: Node,
        children: Tuple[InitialParseResult],
    ) -> InitialParseResult:
        return children[0]

    def visit_number(self, node: Node, children: Sequence[Any]) -> float:
        return float(node.text)

    def visit_filter(
        self,
        node: Node,
        children: Tuple[
            InitialParseResult,
            Any,
            Sequence[Any],
            Any,
            Sequence[Any],
            Any,
        ],
    ) -> InitialParseResult:
        target, _, packed_filters, _, packed_groupbys, *_ = children
        if packed_filters:
            assert isinstance(packed_filters, list)
            _, filter_expr, *_ = packed_filters[0]
            if target.formula is not None:

                def pushdown_filter(param: InitialParseResult) -> InitialParseResult:
                    if param.formula is not None:
                        parameters = param.parameters or []
                        for p in parameters:
                            pushdown_filter(p)
                    elif param.expression is not None:
                        exp = param.expression.expression
                        assert isinstance(exp, (FunctionCall, CurriedFunctionCall))
                        exp = replace(
                            exp,
                            parameters=(
                                exp.parameters[0],
                                binary_condition("and", filter_expr, exp.parameters[1]),
                            ),
                        )
                        param.expression = replace(param.expression, expression=exp)
                    else:
                        raise InvalidQueryException("Could not parse formula")

                    return param

                if target.parameters is not None:
                    for param in target.parameters:
                        pushdown_filter(param)
            else:
                if target.conditions is not None:
                    target.conditions = target.conditions + [filter_expr]
                else:
                    target.conditions = [filter_expr]

        if packed_groupbys:
            assert isinstance(packed_groupbys, list)
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            if target.groupby is not None:
                target.groupby = target.groupby + group_by
            else:
                target.groupby = group_by

        return target

    def _filter(self, children: Sequence[Any], operator: str) -> FunctionCall:
        first, zero_or_more_others = children
        filters: Sequence[FunctionCall] = [
            first,
            *(v for _, _, _, v in zero_or_more_others),
        ]
        if len(filters) == 1:
            return filters[0]
        else:
            # We flatten all filters into a single condition since Snuba supports it.
            return FunctionCall(None, operator, tuple(filters))

    def visit_filter_expr(self, node: Node, children: Sequence[Any]) -> Any:
        return self._filter(children, BooleanFunctions.OR)

    def visit_filter_term(self, node: Node, children: Sequence[Any]) -> Any:
        return self._filter(children, BooleanFunctions.AND)

    def visit_filter_factor(
        self,
        node: Node,
        children: Tuple[Sequence[Union[str, Sequence[str]]] | FunctionCall, Any],
    ) -> FunctionCall:
        factor, *_ = children
        if isinstance(factor, FunctionCall):
            # If we have a parenthesized expression, we just return it.
            return factor
        condition_op, lhs, _, _, _, rhs = factor
        condition_op_value = (
            "!" if len(condition_op) == 1 and condition_op[0] == "!" else ""
        )
        if isinstance(rhs, list):
            if not condition_op_value:
                op = ConditionFunctions.IN
            elif condition_op_value == "!":
                op = ConditionFunctions.NOT_IN
            return FunctionCall(
                None,
                op,
                (
                    Column(None, None, lhs[0]),
                    FunctionCall(
                        None,
                        "tuple",
                        tuple(Literal(None, value) for value in rhs),
                    ),
                ),
            )
        else:
            assert isinstance(rhs, str)
            if not condition_op_value:
                op = ConditionFunctions.EQ
            elif condition_op_value == "!":
                op = ConditionFunctions.NEQ
            return FunctionCall(
                None,
                op,
                (
                    Column(None, None, lhs[0]),
                    Literal(None, rhs),
                ),
            )

    def visit_nested_expr(self, node: Node, children: Sequence[Any]) -> Any:
        _, _, filter_expr, *_ = children
        return filter_expr

    def visit_function(
        self,
        node: Node,
        children: Tuple[
            Tuple[
                InitialParseResult,
            ],
            Sequence[list[SelectedExpression]],
        ],
    ) -> InitialParseResult:
        targets, packed_groupbys = children
        target = targets[0]
        if packed_groupbys:
            group_by = packed_groupbys[0]
            target.groupby = group_by

        return target

    def visit_group_by(
        self,
        node: Node,
        children: Tuple[Any, Any, Any, Sequence[Sequence[str]]],
    ) -> list[SelectedExpression]:
        *_, groupbys = children
        groupby = groupbys[0]
        if isinstance(groupby, str):
            groupby = [groupby]
        columns = [
            SelectedExpression(
                column_name,
                Column(
                    alias=column_name,
                    table_name=None,
                    column_name=column_name,
                ),
            )
            for column_name in groupby
        ]
        return columns

    def visit_condition_op(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_tag_key(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_tag_value(
        self, node: Node, children: Sequence[Sequence[str]]
    ) -> Union[str, Sequence[str]]:
        tag_value = children[0]
        return tag_value

    def visit_unquoted_string(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return str(node.text)

    def visit_quoted_string(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        match = str(node.text[1:-1]).replace('\\"', '"')
        return match

    def visit_string_tuple(self, node: Node, children: Sequence[Any]) -> Sequence[str]:
        _, _, first, zero_or_more_others, _, _ = children
        return [first[0], *(v[0] for _, _, _, v in zero_or_more_others)]

    def visit_group_by_name(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_group_by_name_tuple(
        self, node: Node, children: Sequence[Any]
    ) -> Sequence[str]:
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_target(
        self,
        node: Node,
        children: Sequence[Union[InitialParseResult, Sequence[InitialParseResult]]],
    ) -> InitialParseResult:
        target = children[0]
        if isinstance(children[0], list):
            target = children[0][0]

        assert isinstance(target, InitialParseResult)
        return target

    def visit_variable(self, node: Node, children: Sequence[Any]) -> str:
        raise InvalidQueryException("Variables are not supported yet")

    def visit_nested_expression(
        self, node: Node, children: Tuple[Any, Any, InitialParseResult]
    ) -> InitialParseResult:
        return children[2]

    def visit_aggregate(
        self,
        node: Node,
        children: Tuple[
            str,
            Tuple[
                Any,
                Any,
                InitialParseResult,
                Any,
            ],
        ],
    ) -> InitialParseResult:
        aggregate_name, zero_or_one = children
        _, _, target, *_ = zero_or_one
        selected_aggregate = SelectedExpression(
            AGGREGATE_ALIAS,
            expression=FunctionCall(
                AGGREGATE_ALIAS,
                function_name=aggregate_name,
                parameters=tuple(Column(None, None, "value")),
            ),
        )
        target.expression = selected_aggregate
        return target

    def visit_curried_aggregate(
        self,
        node: Node,
        children: Tuple[
            str,
            Tuple[Any, Any, Sequence[Sequence[Union[str, int, float]]], Any, Any],
            Tuple[Any, Any, InitialParseResult, Any, Any],
        ],
    ) -> InitialParseResult:
        aggregate_name, agg_params, zero_or_one = children
        _, _, target, _, *_ = zero_or_one
        _, _, agg_param_list, _, *_ = agg_params
        aggregate_params = agg_param_list[0] if agg_param_list else []
        selected_aggregate_column = SelectedExpression(
            AGGREGATE_ALIAS,
            CurriedFunctionCall(
                AGGREGATE_ALIAS,
                FunctionCall(
                    None,
                    aggregate_name,
                    tuple(
                        Literal(alias=None, value=param) for param in aggregate_params
                    ),
                ),
                (Column(None, None, "value"),),
            ),
        )
        target.expression = selected_aggregate_column
        return target

    def visit_arbitrary_function(
        self,
        node: Node,
        children: Tuple[
            str,
            Tuple[
                Any,
                Sequence[InitialParseResult],
                Sequence[Sequence[Union[str, int, float]]],
                Any,
            ],
        ],
    ) -> InitialParseResult:
        arbitrary_function_name, zero_or_one = children
        _, expr, params, *_ = zero_or_one
        _, target, _ = expr
        arbitrary_function_params = [
            Literal(alias=None, value=param[-1]) for param in params
        ]
        assert isinstance(target.expression, SelectedExpression)
        assert isinstance(target.expression.expression, FunctionCall)
        aggregate = FunctionCall(
            alias=None,
            function_name=target.expression.expression.function_name,
            parameters=target.expression.expression.parameters,
        )
        arbitrary_function = SelectedExpression(
            name=target.expression.name,
            expression=FunctionCall(
                alias=target.expression.name,
                function_name=arbitrary_function_name,
                parameters=(aggregate, *arbitrary_function_params),
            ),
        )
        target.expression = arbitrary_function
        return target

    def visit_curried_arbitrary_function(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        curried_arbitrary_function_name, agg_params, zero_or_one = children
        _, _, agg_param_list, *_ = agg_params
        aggregate_params = agg_param_list[0] if agg_param_list else []
        _, _, expr, params, *_ = zero_or_one
        _, target, _ = expr
        curried_arbitrary_function_params = [
            Literal(alias=None, value=param[-1]) for param in params
        ]
        assert isinstance(target, InitialParseResult)
        assert isinstance(target.expression, SelectedExpression)
        curried_arbitrary_function = SelectedExpression(
            name=target.expression.name,
            expression=CurriedFunctionCall(
                alias=None,
                internal_function=FunctionCall(
                    None,
                    curried_arbitrary_function_name,
                    tuple(
                        Literal(alias=None, value=param) for param in aggregate_params
                    ),
                ),
                parameters=(
                    target.expression.expression,
                    *curried_arbitrary_function_params,
                ),
            ),
        )
        target.expression = curried_arbitrary_function
        return target

    def visit_inner_filter(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        """
        Given a metric, set its children filters and groupbys, then return a Timeseries.
        """
        target, _, packed_filters, _, packed_groupbys, *_ = children
        target = target[0]
        assert isinstance(target, InitialParseResult)
        if not packed_filters and not packed_groupbys:
            return target
        if packed_filters:
            _, filter_condition, *_ = packed_filters[0]
            target.conditions = [filter_condition]
        if packed_groupbys:
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            target.groupby = group_by
        return target

    def visit_param(
        self, node: Node, children: Tuple[Union[str, int, float], Any]
    ) -> Union[str, int, float]:
        param, *_ = children
        return param

    def visit_param_expression(
        self, node: Node, children: Tuple[Union[str, int, float], Any]
    ) -> Union[str, int, float]:
        param = children[0]
        return param

    def visit_aggregate_list(
        self,
        node: Node,
        children: Tuple[list[Union[str, int, float]], Optional[Union[str, int, float]]],
    ) -> Sequence[Union[str, int, float]]:
        agg_params, param = children
        if param is not None:
            agg_params.append(param)
        assert isinstance(agg_params, list)
        return agg_params

    def visit_aggregate_name(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_curried_aggregate_name(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_arbitrary_function_name(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_curried_arbitrary_function_name(
        self, node: Node, children: Sequence[Any]
    ) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_quoted_mri(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        assert isinstance(node.text, str)
        return InitialParseResult(mri=str(node.text[1:-1]))

    def visit_unquoted_mri(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        assert isinstance(node.text, str)
        return InitialParseResult(mri=str(node.text))

    def visit_quoted_public_name(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        assert isinstance(node.text, str)
        return InitialParseResult(public_name=str(node.text[1:-1]))

    def visit_unquoted_public_name(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        assert isinstance(node.text, str)
        return InitialParseResult(public_name=str(node.text))

    def visit_identifier(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def generic_visit(self, node: Node, children: Sequence[Any]) -> Any:
        """The generic visit method."""
        return children


def parse_mql_query_body(
    body: str,
) -> LogicalQuery:
    """
    Parse the MQL to create an InitialParseResult. Then augments that query using the context
    information provided.
    """
    try:
        """
        Example of parsed tree for:
        'max(transaction.user{dist:["dist1", "dist2"]}) by transaction',

        InitialParseResult(
            'expression': SelectedExpression(name='aggregate_value', expression=max(value) AS `max(d:transactions/duration@millisecond)`),
            'formula': None,
            'parameters': None,
            'groupby': [SelectedExpression(name='transaction', Column('transaction')],
            'conditions': [in(Column('dist'), tuple('dist1', 'dist2'))],
            'mri': None,
            'public_name': 'transaction.user',
        )
        """
        exp_tree = MQL_GRAMMAR.parse(body)
        parsed: InitialParseResult = MQLVisitor().visit(exp_tree)
        if not parsed.expression and not parsed.formula:
            raise ParsingException(
                "No aggregate/expression or formula specified in MQL query"
            )
    except Exception as e:
        raise e
    return parsed


def populate_query_ast_initial(
    parsed: InitialParseResult, mql_context_dict: dict[str, Any]
) -> Tuple[LogicalQuery | CompositeQuery, dict[str, EntityKey]]:
    """
    Determines whether if query contains a formula or timeseries. If the formula is present,
    determine if it is a single or multiple entity type formula. Depending on which type it is,
    populate the query AST (LogicalQuery or CompositeQuery) appropriately.
    """

    def extract_expression(param: InitialParseResult | Any) -> Expression:
        if not isinstance(param, InitialParseResult):
            return Literal(None, param)
        elif param.expression is not None:
            return param.expression.expression
        elif param.formula:
            parameters = param.parameters or []
            return FunctionCall(
                None,
                param.formula,
                tuple(extract_expression(p) for p in parameters),
            )
        else:
            raise InvalidQueryException("Could not parse formula")

    one_or_more_entities = extract_entity(mql_context_dict)
    if len(one_or_more_entities) == 0:
        raise ParsingException("No entity specified in MQL query")

    if parsed.formula:
        parameters = parsed.parameters or []
        selected_columns = [
            SelectedExpression(
                name=AGGREGATE_ALIAS,
                expression=FunctionCall(
                    alias=AGGREGATE_ALIAS,
                    function_name=parsed.formula,
                    parameters=tuple(extract_expression(p) for p in parameters),
                ),
            )
        ]
        if parsed.groupby:
            selected_columns.extend(parsed.groupby)
        groupby = [g.expression for g in parsed.groupby] if parsed.groupby else None

        if len(one_or_more_entities) == 1:
            query = LogicalQuery(
                from_clause=None,
                selected_columns=selected_columns,
                groupby=groupby,
            )
        else:
            query = CompositeQuery(
                from_clause=None,
                selected_columns=selected_columns,
                condition=None,
                groupby=groupby,
            )
            query = process_table_names_in_selected_expressions(
                query, one_or_more_entities
            )
    if parsed.expression:
        selected_columns = [parsed.expression]
        if parsed.groupby:
            selected_columns.extend(parsed.groupby)
        groupby = [g.expression for g in parsed.groupby] if parsed.groupby else None

        metric_value = parsed.mri or parsed.public_name
        conditions: list[Expression] = [
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, None, "metric_id"),
                Literal(None, metric_value),
            )
        ]
        if parsed.conditions:
            conditions.extend(parsed.conditions)

        final_conditions = combine_and_conditions(conditions) if conditions else None

        query = LogicalQuery(
            from_clause=None,
            selected_columns=selected_columns,
            condition=final_conditions,
            groupby=groupby,
        )

    return query, one_or_more_entities


def process_table_names_in_selected_expressions(
    query: CompositeQuery, entities: dict[str, EntityKey]
) -> CompositeQuery:
    """
    This function is responsible for processing formulas that contain multiple entity types.
    It sets the appropriate table names for the selected columns in the query.
    """
    metric_id_match = build_match(col="metric_id", param_type=str)
    value_match = build_match(
        col="value",
        ops=[f"{agg_name}If" for agg_name in COMMON_AGGREGATION_FUNCTIONS],
        rhs_function_call=True,
    )

    def update_metric_id_column_with_table_name(exp: Expression) -> Expression:
        if match := metric_id_match.match(exp):
            metric_name = match.expression("rhs").value
            entity = entities.get(metric_name)
            if not entity:
                raise ParsingException(f"Entity not found for {metric_name}")
            entity_name = entity.value
            assert isinstance(exp, FunctionCall)
            metric_id_column = exp.parameters[0]
            new_metric_id_column = replace(metric_id_column, table_name=entity_name)
            rhs = exp.parameters[1]
            return replace(exp, parameters=(new_metric_id_column, rhs))
        return exp

    def update_columns_with_table_name(exp: Expression) -> Expression:
        def find_metric_id_column(exp: Expression) -> Optional[str]:
            if isinstance(exp, FunctionCall):
                for param in exp.parameters:
                    match = find_metric_id_column(param)
                    if match:
                        return match
            match = metric_id_match.match(exp)
            if match:
                metric_name = exp.parameters[1].value
                return metric_name
            return None

        def update_inner_columns(exp: Expression, entity: str) -> Optional[str]:
            if isinstance(exp, FunctionCall):
                new_parameters = [
                    update_inner_columns(param, entity) for param in exp.parameters
                ]
                return replace(exp, parameters=tuple(new_parameters))
            elif isinstance(exp, Column):
                return replace(exp, table_name=entity)
            else:
                return exp

        if value_match.match(exp):
            assert isinstance(exp, FunctionCall)
            rhs = exp.parameters[1]
            metric_name = find_metric_id_column(rhs)
            if metric_name:
                entity = entities.get(metric_name)
                if not entity:
                    raise ParsingException(
                        "Entity not found when updating column's table name."
                    )
                # replace value column
                value_column = exp.parameters[0]
                new_value_column = replace(value_column, table_name=entity.value)

                # replace column inside the parameters
                new_rhs = update_inner_columns(rhs, entity.value)
                return replace(exp, parameters=(new_value_column, new_rhs))
        return exp

    def update_tags_column_with_table_name(exp: Expression) -> Expression:
        value_match = build_match(col="tags_raw")
        if value_match.match(exp):
            return replace(exp)
        return exp

    query.transform_expressions(update_metric_id_column_with_table_name)
    query.transform_expressions(update_columns_with_table_name)
    query.transform_expressions(update_tags_column_with_table_name)
    return query


def populate_start_end_time(
    query: LogicalQuery,
    mql_context: MQLContext,
    entity_keys: list[EntityKey],
) -> None:
    try:
        start = parse_datetime(mql_context.start)
        end = parse_datetime(mql_context.end)
    except Exception as e:
        raise ParsingException("Invalid start or end time") from e

    for entity_key in entity_keys:
        entity = get_entity(entity_key)
        required_timestamp_column = (
            entity.required_time_column if entity.required_time_column else "timestamp"
        )
        filters = []
        filters.append(
            binary_condition(
                ConditionFunctions.GTE,
                Column(None, entity_key.value, column_name=required_timestamp_column),
                Literal(None, value=start),
            ),
        )
        filters.append(
            binary_condition(
                ConditionFunctions.LT,
                Column(None, entity_key.value, column_name=required_timestamp_column),
                Literal(None, value=end),
            ),
        )
        query.add_condition_to_ast(combine_and_conditions(filters))


def populate_scope(
    query: LogicalQuery, mql_context: MQLContext, entity_keys: list[EntityKey]
) -> None:
    filters = []
    for entity_key in entity_keys:
        filters.append(
            binary_condition(
                ConditionFunctions.IN,
                Column(
                    alias=None, table_name=entity_key.value, column_name="project_id"
                ),
                FunctionCall(
                    alias=None,
                    function_name="tuple",
                    parameters=tuple(
                        Literal(alias=None, value=project_id)
                        for project_id in mql_context.scope.project_ids
                    ),
                ),
            )
        )
        filters.append(
            binary_condition(
                ConditionFunctions.IN,
                Column(alias=None, table_name=entity_key.value, column_name="org_id"),
                FunctionCall(
                    alias=None,
                    function_name="tuple",
                    parameters=tuple(
                        Literal(alias=None, value=int(org_id))
                        for org_id in mql_context.scope.org_ids
                    ),
                ),
            )
        )
        filters.append(
            binary_condition(
                ConditionFunctions.EQ,
                Column(
                    alias=None, table_name=entity_key.value, column_name="use_case_id"
                ),
                Literal(alias=None, value=mql_context.scope.use_case_id),
            )
        )
    query.add_condition_to_ast(combine_and_conditions(filters))


def populate_rollup(
    query: LogicalQuery, mql_context: MQLContext, entity_keys: list[EntityKey]
) -> None:
    rollup = mql_context.rollup

    # Validate/populate granularity
    if rollup.granularity not in GRANULARITIES_AVAILABLE:
        raise ParsingException(
            f"granularity '{rollup.granularity}' is not valid, must be one of {GRANULARITIES_AVAILABLE}"
        )

    for entity_key in entity_keys:
        query.add_condition_to_ast(
            binary_condition(
                ConditionFunctions.EQ,
                Column(None, entity_key.value, "granularity"),
                Literal(None, rollup.granularity),
            )
        )

    # Validate totals/orderby
    if rollup.with_totals is not None and rollup.with_totals not in ("True", "False"):
        raise ParsingException("with_totals must be a string, either 'True' or 'False'")
    if rollup.orderby is not None and rollup.orderby not in ("ASC", "DESC"):
        raise ParsingException("orderby must be either 'ASC' or 'DESC'")
    if rollup.interval is not None and rollup.orderby is not None:
        raise ParsingException("orderby is not supported when interval is specified")
    if rollup.interval and (
        rollup.interval < GRANULARITIES_AVAILABLE[0]
        or rollup.interval < rollup.granularity
    ):
        raise ParsingException(
            f"interval {rollup.interval} must be greater than or equal to granularity {rollup.granularity}"
        )

    with_totals = rollup.with_totals == "True"
    if rollup.interval:
        # If an interval is specified, then we need to group the time by that interval,
        # return the time in the select, and order the results by that time.
        for entity_key in entity_keys:
            time_expression = FunctionCall(
                "time",
                "toStartOfInterval",
                parameters=(
                    Column(None, entity_key.value, "timestamp"),
                    FunctionCall(
                        None,
                        "toIntervalSecond",
                        (Literal(None, rollup.interval),),
                    ),
                    Literal(None, "Universal"),
                ),
            )
            selected = list(query.get_selected_columns())
            selected.append(SelectedExpression("time", time_expression))
            query.set_ast_selected_columns(selected)

            groupby = query.get_groupby()
            if groupby:
                query.set_ast_groupby(list(groupby) + [time_expression])
            else:
                query.set_ast_groupby([time_expression])

            new_order_by = OrderBy(OrderByDirection.ASC, time_expression)
            order_by = query.get_orderby()
            if order_by:
                query.set_ast_orderby(list(order_by) + [new_order_by])
            else:
                query.set_ast_orderby([new_order_by])

        if with_totals:
            query.set_totals(True)
    elif rollup.orderby is not None:
        direction = (
            OrderByDirection.ASC if rollup.orderby == "ASC" else OrderByDirection.DESC
        )
        orderby = OrderBy(direction, Column(None, None, AGGREGATE_ALIAS))
        query.set_ast_orderby([orderby])


def populate_limit(query: LogicalQuery, mql_context: MQLContext) -> None:
    limit = 1000
    if mql_context.limit:
        if mql_context.limit > MAX_LIMIT:
            raise ParsingException(
                "queries cannot have a limit higher than 10000", should_report=False
            )
        limit = mql_context.limit

    query.set_limit(limit)


def populate_offset(query: LogicalQuery, mql_context: MQLContext) -> None:
    if mql_context.offset:
        if mql_context.offset < 0:
            raise ParsingException("offset must be greater than or equal to 0")
        query.set_offset(mql_context.offset)


def extract_entity(mql_context_dict: dict[str, Any]) -> dict[str, EntityKey]:
    mql_context = MQLContext.from_dict(mql_context_dict)
    entities = {}
    try:
        for metric_name, entity_name in mql_context.entity.items():
            entities[metric_name] = EntityKey(entity_name)
        return entities
    except Exception as e:
        raise ParsingException(f"Invalid entity {mql_context.entity}") from e


def populate_query_from_mql_context(
    query: LogicalQuery | CompositeQuery,
    mql_context_dict: dict[str, Any],
    one_or_more_entities: dict[str, EntityKey],
) -> tuple[LogicalQuery, MQLContext]:
    mql_context = MQLContext.from_dict(mql_context_dict)
    entity_keys = list(set(one_or_more_entities.values()))
    if len(entity_keys) == 1:
        metric_name = next(iter(one_or_more_entities))
        query.set_from_clause(
            QueryEntity(
                key=one_or_more_entities[metric_name],
                schema=get_entity(
                    key=one_or_more_entities[metric_name]
                ).get_data_model(),
            )
        )
    else:
        assert len(entity_keys) >= 2
        join_clause = JoinClause(
            left_node=IndividualNode(
                entity_keys[0].value,
                QueryEntity(
                    key=entity_keys[0],
                    schema=get_entity(entity_keys[0]).get_data_model(),
                ),
            ),
            right_node=IndividualNode(
                entity_keys[1].value,
                QueryEntity(
                    key=entity_keys[1],
                    schema=get_entity(entity_keys[1]).get_data_model(),
                ),
            ),
            keys=[
                JoinCondition(
                    JoinConditionExpression(entity_keys[0].value, "timestamp"),
                    JoinConditionExpression(entity_keys[1].value, "timestamp"),
                )
            ],
            join_type=JoinType.INNER,
        )
        for i in range(2, len(entity_keys)):
            entity_key = entity_keys[i]
            join_clause = JoinClause(
                left_node=join_clause,
                right_node=IndividualNode(
                    entity_key.value,
                    QueryEntity(
                        key=entity_key,
                        schema=get_entity(entity_key).get_data_model(),
                    ),
                ),
                keys=[
                    JoinCondition(
                        JoinConditionExpression(entity_key.value, "timestamp"),
                        JoinConditionExpression(entity_keys[i - 1].value, "timestamp"),
                    )
                ],
                join_type=JoinType.INNER,
            )
        query.set_from_clause(join_clause)

    populate_start_end_time(query, mql_context, entity_keys)
    populate_scope(query, mql_context, entity_keys)
    populate_rollup(query, mql_context, entity_keys)
    populate_limit(query, mql_context)
    populate_offset(query, mql_context)
    print(query.__dict__)

    return query, mql_context


CustomProcessors = Sequence[
    Callable[[Union[CompositeQuery[QueryEntity], LogicalQuery]], None]
]


def parse_mql_query(
    body: str,
    mql_context_dict: dict[str, Any],
    dataset: Dataset,
    custom_processing: Optional[CustomProcessors] = None,
    settings: QuerySettings | None = None,
) -> Tuple[Union[CompositeQuery[QueryEntity], LogicalQuery], str]:
    with sentry_sdk.start_span(op="parser", description="parse_mql_query_initial"):
        initial_parse_result = parse_mql_query_body(body)
    with sentry_sdk.start_span(op="parser", description="populate_query_ast_initial"):
        query_initial, one_or_more_entities = populate_query_ast_initial(
            initial_parse_result, mql_context_dict
        )
    with sentry_sdk.start_span(
        op="parser", description="populate_query_ast_from_mql_context"
    ):
        query, mql_context = populate_query_from_mql_context(
            query_initial, mql_context_dict, one_or_more_entities
        )
    with sentry_sdk.start_span(op="processor", description="resolve_indexer_mappings"):
        resolve_mappings(query, mql_context.indexer_mappings, dataset)

    if settings and settings.get_dry_run():
        explain_meta.set_original_ast(str(query))

    # NOTE (volo): The anonymizer that runs after this function call chokes on
    # OR and AND clauses with multiple parameters so we have to treeify them
    # before we run the anonymizer and the rest of the post processors
    with sentry_sdk.start_span(op="processor", description="treeify_conditions"):
        _post_process(query, [_treeify_or_and_conditions], settings)

    # TODO: Figure out what to put for the anonymized string
    with sentry_sdk.start_span(op="parser", description="anonymize_snql_query"):
        snql_anonymized = format_snql_anonymized(query).get_sql()

    with sentry_sdk.start_span(op="processor", description="post_processors"):
        _post_process(
            query,
            POST_PROCESSORS,
            settings,
        )

    # Custom processing to tweak the AST before validation
    with sentry_sdk.start_span(op="processor", description="custom_processing"):
        if custom_processing is not None:
            _post_process(query, custom_processing, settings)

    # Time based processing
    with sentry_sdk.start_span(op="processor", description="time_based_processing"):
        _post_process(query, [_replace_time_condition], settings)

    # Validating
    with sentry_sdk.start_span(op="validate", description="expression_validators"):
        _post_process(query, VALIDATORS)

    return query, snql_anonymized
