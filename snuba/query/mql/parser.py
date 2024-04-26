from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union

import sentry_sdk
from parsimonious.exceptions import IncompleteParseError
from parsimonious.nodes import Node, NodeVisitor
from snuba_sdk.metrics_visitors import AGGREGATE_ALIAS
from snuba_sdk.mql.mql import MQL_GRAMMAR

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset_name
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import arrayElement
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
from snuba.query.processors.logical.filter_in_select_optimizer import (
    FilterInSelectOptimizer,
)
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
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


ARITHMETIC_OPERATORS_MAPPING = {
    "+": "plus",
    "-": "minus",
    "*": "multiply",
    "/": "divide",
}

UNARY_OPERATORS = {
    "-": "negate",
}


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

    def visit_unary_op(self, node: Node, children: Sequence[Any]) -> Any:
        return UNARY_OPERATORS[node.text]

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
        """
        assert param.expression is not None
        exp = param.expression.expression
        assert isinstance(exp, (FunctionCall, CurriedFunctionCall))

        conditions = param.conditions or []
        metric_id_condition = binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "metric_id"),
            Literal(None, param.mri),
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
        operand_1: InitialParseResult,
        operand_2: InitialParseResult | None = None,
    ) -> InitialParseResult:
        # TODO: If the formula has filters/group by, where do those appear?

        # If the parameters of the query are timeseries, extract the expressions from the result
        if (
            isinstance(operand_1, InitialParseResult)
            and operand_1.expression is not None
        ):
            operand_1 = replace(
                operand_1, expression=self._build_timeseries_formula_param(operand_1)
            )
        if (
            operand_2 is not None
            and isinstance(operand_2, InitialParseResult)
            and operand_2.expression is not None
        ):
            operand_2 = replace(
                operand_2,
                expression=self._build_timeseries_formula_param(operand_2),
            )

        if (
            operand_2 is not None
            and isinstance(operand_1, InitialParseResult)
            and isinstance(operand_2, InitialParseResult)
            and operand_1.groupby != operand_2.groupby
        ):
            raise InvalidQueryException(
                "All terms in a formula must have the same groupby"
            )

        groupby = (
            operand_1.groupby if isinstance(operand_1, InitialParseResult) else None
        )
        parameters = [operand_1]
        if operand_2 is not None:
            parameters.append(operand_2)
        return InitialParseResult(
            expression=None,
            formula=term_operator,
            parameters=parameters,
            groupby=groupby,
        )

    def visit_term(
        self,
        node: Node,
        children: Tuple[InitialParseResult, Any],
    ) -> InitialParseResult:
        term, zero_or_more_others = children
        if zero_or_more_others:
            _, term_operator, _, unary, *_ = zero_or_more_others[0]
            return self._visit_formula(term_operator, term, unary)

        return term

    def visit_unary(self, node: Node, children: Sequence[Any]) -> Any:
        unary_op, coefficient = children
        if unary_op:
            if isinstance(coefficient, float) or isinstance(coefficient, int):
                return -coefficient
            elif isinstance(coefficient, InitialParseResult):
                return self._visit_formula(unary_op[0], coefficient)
            else:
                raise InvalidQueryException(
                    f"Unary expression not supported for type {type(coefficient)}"
                )

        return coefficient

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
        target, filters, packed_groupbys, *_ = children
        packed_filters = filters[0][1] if filters else []  # strip braces
        if packed_filters:
            assert isinstance(packed_filters, list)
            _, filter_expr, *_ = packed_filters[0]
            if target.formula is not None:

                def pushdown_filter(n: InitialParseResult) -> None:
                    assert isinstance(n, InitialParseResult)
                    if n.formula is not None:
                        for param in n.parameters or []:
                            if isinstance(param, InitialParseResult):
                                # Only push down non-scalar values e.g. sum(mri) / 3600, 3600 will not be pushed down
                                # TODO: the type definition of InitialParseResult.parameters is inaccurate
                                pushdown_filter(param)
                    elif n.expression is not None:
                        exp = n.expression.expression
                        assert isinstance(exp, (FunctionCall, CurriedFunctionCall))
                        exp = replace(
                            exp,
                            parameters=(
                                exp.parameters[0],
                                binary_condition("and", filter_expr, exp.parameters[1]),
                            ),
                        )
                        n.expression = replace(n.expression, expression=exp)
                    else:
                        raise InvalidQueryException("Could not parse formula")

                pushdown_filter(target)
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
        target, filters, packed_groupbys, *_ = children
        target = target[0]
        assert isinstance(target, InitialParseResult)
        if not filters and not packed_groupbys:
            return target
        packed_filters = filters[0][1] if filters else []  # strip braces
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
        raise ParsingException("MQL endpoint only supports MRIs")

    def visit_unquoted_public_name(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        raise ParsingException("MQL endpoint only supports MRIs")

    def visit_identifier(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def generic_visit(self, node: Node, children: Sequence[Any]) -> Any:
        """The generic visit method."""
        return children


def parse_mql_query_body(body: str, dataset: Dataset) -> LogicalQuery:
    """
    Parse the MQL to create an initial query. Then augments that query using the context
    information provided.
    """
    try:
        """
        Example of parsed tree for:
        'max(transaction.user{dist:["dist1", "dist2"]}) by transaction',

        InitialParseResult(
            'public_name': 'transaction.user',
            'selected_aggregate': [SelectedExpression(name='aggregate_value', expression=sum(value) AS `sum(d:transactions/duration@millisecond)`)],
            'filters': [in(Column('dist'), tuple('dist1', 'dist2'))],
            'groupby': [SelectedExpression(name='transaction', Column('transaction')],
        )
        """
        try:
            exp_tree = MQL_GRAMMAR.parse(body)
            parsed: InitialParseResult = MQLVisitor().visit(exp_tree)
        except ParsingException as e:
            logger.warning(f"Invalid MQL query ({e}): {body}")
            raise e
        except IncompleteParseError as e:
            lines = body.split("\n")
            if e.line() > len(lines):
                line = body
            else:
                line = lines[e.line() - 1]

            idx = e.column()
            prefix = line[max(0, idx - 3) : idx]
            suffix = line[idx : (idx + 10)]
            raise ParsingException(
                f"Parsing error on line {e.line()} at '{prefix}{suffix}'"
            )
        except Exception as e:
            message = str(e)
            if "\n" in message:
                message, _ = message.split("\n", 1)
            raise ParsingException(message)

        if not parsed.expression and not parsed.formula:
            raise ParsingException(
                "No aggregate/expression or formula specified in MQL query"
            )

        if parsed.formula:

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

            def extract_mri(param: InitialParseResult | Any) -> str:
                if isinstance(param, InitialParseResult):
                    if param.mri:
                        return param.mri
                    elif param.formula:
                        for p in param.parameters or []:
                            try:
                                mri = extract_mri(p)
                                if mri:
                                    return mri
                            except ParsingException:
                                pass

                raise ParsingException("formula does not contain any MRIs")

            mri = extract_mri(parsed)  # Only works for single type formulas
            entity_key = select_entity(mri, dataset)

            query = LogicalQuery(
                from_clause=QueryEntity(
                    key=entity_key, schema=get_entity(entity_key).get_data_model()
                ),
                selected_columns=selected_columns,
                groupby=groupby,
            )
        if parsed.expression:
            selected_columns = [parsed.expression]
            if parsed.groupby:
                selected_columns.extend(parsed.groupby)
            groupby = [g.expression for g in parsed.groupby] if parsed.groupby else None

            metric_value = parsed.mri
            if not metric_value:
                raise ParsingException("no MRI specified in MQL query")

            conditions: list[Expression] = [
                binary_condition(
                    ConditionFunctions.EQ,
                    Column(None, None, "metric_id"),
                    Literal(None, metric_value),
                )
            ]
            if parsed.conditions:
                conditions.extend(parsed.conditions)

            final_conditions = (
                combine_and_conditions(conditions) if conditions else None
            )

            entity_key = select_entity(metric_value, dataset)

            query = LogicalQuery(
                from_clause=QueryEntity(
                    key=entity_key, schema=get_entity(entity_key).get_data_model()
                ),
                selected_columns=selected_columns,
                condition=final_conditions,
                groupby=groupby,
            )
    except Exception as e:
        raise e
    return query


METRICS_ENTITIES = {
    "c": EntityKey.METRICS_COUNTERS,
    "d": EntityKey.METRICS_DISTRIBUTIONS,
    "s": EntityKey.METRICS_SETS,
}

GENERIC_ENTITIES = {
    "c": EntityKey.GENERIC_METRICS_COUNTERS,
    "d": EntityKey.GENERIC_METRICS_DISTRIBUTIONS,
    "s": EntityKey.GENERIC_METRICS_SETS,
    "g": EntityKey.GENERIC_METRICS_GAUGES,
}


def select_entity(mri: str, dataset: Dataset) -> EntityKey:
    """
    Given an MRI, select the entity that it belongs to.
    """
    if get_dataset_name(dataset) == "metrics":
        if entity := METRICS_ENTITIES.get(mri[0]):
            return entity
    elif get_dataset_name(dataset) == "generic_metrics":
        if entity := GENERIC_ENTITIES.get(mri[0]):
            return entity

    raise ParsingException(f"invalid metric type {mri[0]}")


def populate_start_end_time(
    query: LogicalQuery, mql_context: MQLContext, entity_key: EntityKey
) -> None:
    try:
        start = parse_datetime(mql_context.start)
        end = parse_datetime(mql_context.end)
    except Exception as e:
        raise ParsingException("Invalid start or end time") from e

    entity = get_entity(entity_key)
    required_timestamp_column = (
        entity.required_time_column if entity.required_time_column else "timestamp"
    )
    filters = []
    filters.append(
        binary_condition(
            ConditionFunctions.GTE,
            Column(None, None, column_name=required_timestamp_column),
            Literal(None, value=start),
        ),
    )
    filters.append(
        binary_condition(
            ConditionFunctions.LT,
            Column(None, None, column_name=required_timestamp_column),
            Literal(None, value=end),
        ),
    )
    query.add_condition_to_ast(combine_and_conditions(filters))


def populate_scope(query: LogicalQuery, mql_context: MQLContext) -> None:
    filters = []
    filters.append(
        binary_condition(
            ConditionFunctions.IN,
            Column(alias=None, table_name=None, column_name="project_id"),
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
            Column(alias=None, table_name=None, column_name="org_id"),
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
            Column(alias=None, table_name=None, column_name="use_case_id"),
            Literal(alias=None, value=mql_context.scope.use_case_id),
        )
    )
    query.add_condition_to_ast(combine_and_conditions(filters))


def populate_rollup(query: LogicalQuery, mql_context: MQLContext) -> None:
    rollup = mql_context.rollup

    # Validate/populate granularity
    if rollup.granularity not in GRANULARITIES_AVAILABLE:
        raise ParsingException(
            f"granularity '{rollup.granularity}' is not valid, must be one of {GRANULARITIES_AVAILABLE}"
        )

    query.add_condition_to_ast(
        binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "granularity"),
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
        time_expression = FunctionCall(
            "time",
            "toStartOfInterval",
            parameters=(
                Column(None, None, "timestamp"),
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

        orderby = OrderBy(OrderByDirection.ASC, time_expression)
        query.set_ast_orderby([orderby])

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


def populate_query_from_mql_context(
    query: LogicalQuery, mql_context_dict: dict[str, Any]
) -> tuple[LogicalQuery, MQLContext]:
    mql_context = MQLContext.from_dict(mql_context_dict)
    entity_key = query.get_from_clause().key

    populate_start_end_time(query, mql_context, entity_key)
    populate_scope(query, mql_context)
    populate_rollup(query, mql_context)
    populate_limit(query, mql_context)
    populate_offset(query, mql_context)

    return query, mql_context


def quantiles_to_quantile(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery]
) -> None:
    """
    Changes quantiles(0.5)(...) to arrayElement(quantiles(0.5)(...), 1). This is to simplify
    the API (so that the arrays don't need to be unwrapped) and also avoids bugs where comparing
    arrays of values to values cause typing errors (e.g. [1] / 1).
    """

    def transform(exp: Expression) -> Expression:
        if isinstance(exp, CurriedFunctionCall):
            if exp.internal_function.function_name in ("quantiles", "quantilesIf"):
                if len(exp.internal_function.parameters) == 1:
                    return arrayElement(
                        exp.alias, replace(exp, alias=None), Literal(None, 1)
                    )

        return exp

    query.transform_expressions(transform)


CustomProcessors = Sequence[
    Callable[[Union[CompositeQuery[QueryEntity], LogicalQuery]], None]
]

MQL_POST_PROCESSORS: CustomProcessors = POST_PROCESSORS + [
    quantiles_to_quantile,
]


def parse_mql_query(
    body: str,
    mql_context_dict: dict[str, Any],
    dataset: Dataset,
    custom_processing: Optional[CustomProcessors] = None,
    settings: QuerySettings | None = None,
) -> Tuple[Union[CompositeQuery[QueryEntity], LogicalQuery], str]:
    with sentry_sdk.start_span(op="parser", description="parse_mql_query_initial"):
        query = parse_mql_query_body(body, dataset)
    with sentry_sdk.start_span(
        op="parser", description="populate_query_from_mql_context"
    ):
        query, mql_context = populate_query_from_mql_context(query, mql_context_dict)
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
            MQL_POST_PROCESSORS,
            settings,
        )

    # Filter in select optimizer
    with sentry_sdk.start_span(op="processor", description="filter_in_select_optimize"):
        if settings is None:
            FilterInSelectOptimizer().process_query(query, HTTPQuerySettings())
        else:
            FilterInSelectOptimizer().process_query(query, settings)

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
