from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union

import sentry_sdk
from parsimonious.exceptions import IncompleteParseError
from parsimonious.nodes import Node, NodeVisitor
from snuba_sdk.metrics_visitors import AGGREGATE_ALIAS
from snuba_sdk.mql.mql import MQL_GRAMMAR

from snuba import settings as snuba_settings
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset_name
from snuba.query import SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
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
from snuba.query.mql.context_population import (
    limit_value,
    offset_value,
    rollup_expressions,
    scope_conditions,
    start_end_time_condition,
)
from snuba.query.mql.mql_context import MQLContext
from snuba.query.parser.exceptions import ParsingException
from snuba.query.processors.logical.filter_in_select_optimizer import (
    FilterInSelectOptimizer,
)
from snuba.query.query_settings import HTTPQuerySettings, QuerySettings
from snuba.query.snql.anonymize import format_snql_anonymized
from snuba.query.snql.parser import (
    POST_PROCESSORS,
    VALIDATORS,
    _post_process,
    _replace_time_condition,
    _treeify_or_and_conditions,
)
from snuba.state import explain_meta, get_int_config

# The parser returns a bunch of different types, so create a single aggregate type to
# capture everything.
MQLSTUFF = Dict[str, Union[str, list[SelectedExpression], list[Expression]]]
logger = logging.getLogger("snuba.mql.parser")


@dataclass
class InitialParseResult:
    expression: SelectedExpression | None = None
    formula: str | None = None
    parameters: list[FormulaParameter] | None = None
    groupby: list[SelectedExpression] | None = None
    conditions: list[Expression] | None = None
    mri: str | None = None
    table_alias: str | None = None


FormulaParameter = Union[InitialParseResult, int, float]


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

    def __init__(self) -> None:
        self.alias_count: dict[str, int] = {}

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
            return InitialParseResult(
                expression=None,
                formula=term_operator,
                parameters=[term, coefficient],
            )
        return term

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        return ARITHMETIC_OPERATORS_MAPPING[node.text]

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> Any:
        return ARITHMETIC_OPERATORS_MAPPING[node.text]

    def visit_unary_op(self, node: Node, children: Sequence[Any]) -> Any:
        return UNARY_OPERATORS[node.text]

    def visit_term(
        self,
        node: Node,
        children: Tuple[InitialParseResult, Any],
    ) -> InitialParseResult:
        term, zero_or_more_others = children
        if zero_or_more_others:
            _, term_operator, _, unary, *_ = zero_or_more_others[0]
            parameters: list[FormulaParameter] = [term]
            if unary is not None:
                parameters.append(unary)

            return InitialParseResult(
                expression=None,
                formula=term_operator,
                parameters=parameters,
            )

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

        new_condition = None
        packed_filters = filters[0][1] if filters else []  # strip braces
        if packed_filters:
            assert isinstance(packed_filters, list)
            _, new_condition, *_ = packed_filters[0]

        groupby = None
        if packed_groupbys:
            assert isinstance(packed_groupbys, list)
            groupby_val = packed_groupbys[0]
            groupby = groupby_val if isinstance(groupby_val, list) else [groupby_val]

        if new_condition or groupby:
            if target.formula is not None:
                # Push all the filters and groupbys of the formula down to all the leaf nodes
                # so they get correctly added to their subqueries.
                def pushdown_values(param: FormulaParameter) -> FormulaParameter:
                    if not isinstance(param, InitialParseResult):
                        return param
                    if param.formula is not None:
                        parameters = param.parameters or []
                        for p in parameters:
                            pushdown_values(p)
                    elif param.expression is not None:
                        if new_condition:
                            param.conditions = (
                                param.conditions + [new_condition]
                                if param.conditions
                                else [new_condition]
                            )
                        if groupby:
                            param.groupby = (
                                param.groupby + groupby if param.groupby else groupby
                            )
                    else:
                        raise InvalidQueryException("Could not parse formula")

                    return param

                for param in target.parameters or []:
                    pushdown_values(param)

            else:
                if new_condition:
                    target.conditions = (
                        target.conditions + [new_condition]
                        if target.conditions
                        else [new_condition]
                    )
                if groupby:
                    target.groupby = (
                        target.groupby + groupby if target.groupby else groupby
                    )

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

    def _generate_table_alias(self, mri: str) -> str:
        alias = mri[0]
        suffix = self.alias_count.setdefault(alias, -1) + 1
        self.alias_count[alias] = suffix
        return f"{alias}{suffix}"

    def visit_quoted_mri(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        assert isinstance(node.text, str)
        mri = str(node.text[1:-1])
        return InitialParseResult(mri=mri, table_alias=self._generate_table_alias(mri))

    def visit_unquoted_mri(
        self, node: Node, children: Sequence[Any]
    ) -> InitialParseResult:
        assert isinstance(node.text, str)
        mri = str(node.text)
        return InitialParseResult(mri=mri, table_alias=self._generate_table_alias(mri))

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


def convert_formula_to_query(
    parsed: InitialParseResult, dataset: Dataset
) -> LogicalQuery | CompositeQuery[QueryEntity]:
    """
    Look up all the referenced entities, and create a JoinClause for each of the entities
    referenced in the formula. Then map the correct table to each of the expressions in the formula.

    E.g. sum(d:transactions/duration) / sum(c:transactions/duration_ms) will produce a JoinClause
    with (d: generic_metrics_distributions) -[counters]-> (c: generic_metrics_counters)

    Most formulas do not operate on multiple entities, but they get the same treatment as if they
    did in order to keep consistency. In that case the table is joined on itself.

    If a formula has only a single MRI and some scalars e.g. sum(d:transactions/duration) + 1, then
    there's no need for a JoinClause, and this returns a simple LogicalQuery.
    """
    if parsed.formula is None:
        raise InvalidQueryException("Parsed formula has no formula name specified")

    def find_all_leaf_nodes(tree: FormulaParameter) -> list[InitialParseResult] | None:
        if isinstance(tree, InitialParseResult) and tree.formula is None:
            return [tree]
        elif isinstance(tree, InitialParseResult) and tree.formula is not None:
            nodes = []
            for p in tree.parameters or []:
                found = find_all_leaf_nodes(p)
                if found:
                    nodes.extend(found)
            return nodes
        else:
            return None

    join_nodes = find_all_leaf_nodes(parsed)
    if join_nodes is None:
        raise InvalidQueryException("Could not parse formula")

    if len(join_nodes) == 1:
        # This is a simple formula with only one MRI and some scalars, so we don't need to join
        raise Exception("handle this")

    # We use the group by for the ON conditions, so make sure they are all the same
    groupbys = join_nodes[0].groupby
    if not all(node.groupby == groupbys for node in join_nodes):
        raise InvalidQueryException("All terms in a formula must have the same groupby")

    # Used to cache query entities to avoid building multiple times
    entities: dict[EntityKey, QueryEntity] = {}

    def build_node(node: InitialParseResult) -> IndividualNode[QueryEntity]:
        if not node.mri:
            raise InvalidQueryException("No MRI found")
        if not node.table_alias:
            raise InvalidQueryException(f"No table alias found for MRI {node.mri}")

        entity_key = select_entity(node.mri, dataset)
        data_source = entities.setdefault(
            entity_key, QueryEntity(entity_key, get_entity(entity_key).get_data_model())
        )
        return IndividualNode(node.table_alias, data_source)

    # Build the JoinClause recursively, joining each node to the previous node.
    # Note one thing: this assumes that each node has the same group by, which is enforced earlier.
    first_node = build_node(join_nodes[0])

    def build_join_clause(
        prev_node: IndividualNode[QueryEntity], nodes: list[InitialParseResult]
    ) -> JoinClause[QueryEntity]:
        if not nodes:
            raise InvalidQueryException("Invalid join clause in formula query")

        node = nodes[0]
        if not node.table_alias:
            raise InvalidQueryException("Invalid table alias in formula query")
        lhs = build_node(node)

        conditions = []
        for groupby in node.groupby or []:
            column = groupby.expression
            assert isinstance(column, Column)
            conditions.append(
                JoinCondition(
                    left=JoinConditionExpression(lhs.alias, column.column_name),
                    right=JoinConditionExpression(prev_node.alias, column.column_name),
                )
            )

        left_side = build_join_clause(lhs, nodes[1:]) if len(nodes) > 1 else lhs
        return JoinClause(
            left_node=left_side,
            right_node=prev_node,
            keys=conditions,
            join_type=JoinType.INNER,
        )

    join_clause = build_join_clause(first_node, join_nodes[1:])
    query = CompositeQuery(
        from_clause=join_clause,
    )

    # Build SelectedExpression from root tree
    # When going through the selected expressions, populate the table aliases
    def extract_expression(param: InitialParseResult | Any) -> Expression:
        if not isinstance(param, InitialParseResult):
            return Literal(None, param)
        elif param.expression is not None:
            aggregate_function = param.expression.expression
            assert isinstance(aggregate_function, (FunctionCall, CurriedFunctionCall))
            # Metrics aggregates operate on a column
            column = aggregate_function.parameters[0]
            assert isinstance(column, Column)
            return replace(
                aggregate_function,
                parameters=(replace(column, table_name=param.table_alias),),
                alias=None,
            )
        elif param.formula:
            parameters = param.parameters or []
            return FunctionCall(
                None,
                param.formula,
                tuple(extract_expression(p) for p in parameters),
            )
        else:
            raise InvalidQueryException(
                "Could not build selected expression for formula"
            )

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

    # The groupbys are pushed down to all the nodes of the query. Add them to the groupby of the query
    groupby = []
    for leaf_node in join_nodes:
        if leaf_node.groupby:
            for group_exp in leaf_node.groupby:
                if isinstance(group_exp.expression, Column):
                    aliased_groupby = replace(
                        group_exp,
                        expression=replace(
                            group_exp.expression, table_name=leaf_node.table_alias
                        ),
                    )
                    selected_columns.append(aliased_groupby)
                    groupby.append(aliased_groupby.expression)

    if groupby:
        query.set_ast_groupby(groupby)

    query.set_ast_selected_columns(selected_columns)

    # Go through all the conditions, populate the conditions with the table alias, add them to the query conditions
    # also needs the metric ID conditions added
    def extract_filters(param: InitialParseResult | Any) -> list[FunctionCall]:
        if not isinstance(param, InitialParseResult):
            return []
        elif param.expression is not None:
            conditions = []
            for c in param.conditions or []:
                assert isinstance(c, FunctionCall)
                lhs = c.parameters[0]
                if isinstance(lhs, Column):
                    conditions.append(
                        replace(
                            c,
                            parameters=tuple(
                                [
                                    replace(lhs, table_name=param.table_alias),
                                    *c.parameters[1:],
                                ]
                            ),
                        )
                    )

            conditions.append(
                binary_condition(
                    "equals",
                    Column(None, param.table_alias, "metric_id"),
                    Literal(None, param.mri),
                )
            )
            return conditions
        elif param.formula:
            conditions = []
            for p in param.parameters or []:
                conditions.extend(extract_filters(p))
            return conditions
        else:
            raise InvalidQueryException("Could not extract valid filters for formula")

    conditions = []
    for p in parsed.parameters or []:
        conditions.extend(extract_filters(p))

    query.add_condition_to_ast(combine_and_conditions(conditions))

    return query


def convert_timeseries_to_query(
    parsed: InitialParseResult, dataset: Dataset
) -> LogicalQuery:
    if parsed.expression is None:
        raise InvalidQueryException("Parsed expression has no expression specified")

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

    final_conditions = combine_and_conditions(conditions) if conditions else None

    entity_key = select_entity(metric_value, dataset)

    query = LogicalQuery(
        from_clause=QueryEntity(
            key=entity_key, schema=get_entity(entity_key).get_data_model()
        ),
        selected_columns=selected_columns,
        condition=final_conditions,
        groupby=groupby,
    )
    return query


def parse_mql_query_body(
    body: str, dataset: Dataset
) -> LogicalQuery | CompositeQuery[QueryEntity]:
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
            query = convert_formula_to_query(parsed, dataset)

        else:
            query = convert_timeseries_to_query(parsed, dataset)

    except Exception as e:
        raise e
    return query


def populate_query_from_mql_context(
    query: LogicalQuery | CompositeQuery[QueryEntity], mql_context_dict: dict[str, Any]
) -> tuple[LogicalQuery | CompositeQuery[QueryEntity], MQLContext]:
    mql_context = MQLContext.from_dict(mql_context_dict)

    # List of entity key/alias tuples
    entity_data: list[tuple[EntityKey, str]] = []
    if isinstance(query, LogicalQuery):
        entity_data.append((query.get_from_clause().key, ""))
    elif isinstance(query, CompositeQuery):
        join_clause = query.get_from_clause()
        assert isinstance(join_clause, JoinClause)

        alias_node_map = join_clause.get_alias_node_map()
        for alias, node in alias_node_map.items():
            data_source = node.data_source
            assert isinstance(data_source, QueryEntity)
            entity_data.append((data_source.key, alias))

    selected_time_found = False
    for entity_key, alias in entity_data:
        time_condition = start_end_time_condition(mql_context, entity_key, alias)
        scope_condition = scope_conditions(mql_context, alias)
        granularity_condition, with_totals, orderby, selected_time = rollup_expressions(
            mql_context, alias
        )

        context_condition = combine_and_conditions(
            [time_condition, scope_condition, granularity_condition]
        )
        query.add_condition_to_ast(context_condition)

        query.set_totals(with_totals)
        # TODO: Not sure it is a good idea to have multiple orderby expressions
        query.set_ast_orderby([orderby])

        if selected_time:
            selected_time_found = True
            query.set_ast_selected_columns(
                list(query.get_selected_columns()) + [selected_time]
            )

            groupby = query.get_groupby()
            if groupby:
                query.set_ast_groupby(list(groupby) + [selected_time.expression])
            else:
                query.set_ast_groupby([selected_time.expression])

    if isinstance(query, CompositeQuery) and selected_time_found:
        # If the query is grouping by time, that needs to be added to the JoinClause keys to
        # ensure we correctly join the subqueries. The column names will be the same for all the
        # subqueries, so we just need to map all the table aliases.

        join_clause = query.get_from_clause()
        assert isinstance(join_clause, JoinClause)
        base_node_alias = join_clause.right_node.alias
        other_aliases = [d[1] for d in entity_data if d[1] != base_node_alias]
        new_conditions = []
        for other in other_aliases:
            condition = JoinCondition(
                left=JoinConditionExpression(base_node_alias, "time"),
                right=JoinConditionExpression(other, "time"),
            )
            new_conditions.append(condition)

        conditions = list(join_clause.keys)
        query.set_from_clause(replace(join_clause, keys=conditions + new_conditions))

    limit = limit_value(mql_context)
    offset = offset_value(mql_context)
    query.set_limit(limit)
    query.set_offset(offset) if offset else None

    return query, mql_context


def quantiles_to_quantile(query: CompositeQuery[QueryEntity] | LogicalQuery) -> None:
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
    feat_flag = get_int_config(
        "enable_filter_in_select_optimizer",
        default=snuba_settings.ENABLE_FILTER_IN_SELECT_OPTIMIZER,
    )
    if feat_flag:
        with sentry_sdk.start_span(
            op="processor", description="filter_in_select_optimize"
        ):
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
