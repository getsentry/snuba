from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

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
    combine_and_conditions,
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
MQLSTUFF = Dict[str, Union[str, List[SelectedExpression], List[Expression]]]
logger = logging.getLogger("snuba.mql.parser")


@dataclass
class InitialParseResult:
    aggregate: SelectedExpression | None = None
    groupby: List[SelectedExpression] | None = None
    conditions: List[Expression] | None = None
    mri: str | None = None
    public_name: str | None = None
    metric_id: int | None = None


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
        # zero_or_more_others is used for formulas, which aren't supported yet
        args, zero_or_more_others = children
        return args

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryException("Arithmetic function not supported yet")

    def visit_term(
        self,
        node: Node,
        children: Tuple[InitialParseResult, Any],
    ) -> InitialParseResult:
        term, zero_or_more_others = children
        if zero_or_more_others:
            raise InvalidQueryException("Arithmetic function not supported yet")
        return term

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> str:
        raise InvalidQueryException("Arithmetic function not supported yet")

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
            Sequence[Any],
            Sequence[Any],
            Any,
        ],
    ) -> InitialParseResult:
        target, packed_filters, packed_groupbys, *_ = children
        if packed_filters:
            assert isinstance(packed_filters, list)
            _, _, filter_expr, *_ = packed_filters[0]
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
        children: Tuple[Sequence[Union[str, Sequence[str]]], Any],
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
        return str(node.text[1:-1])

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
        target.aggregate = selected_aggregate
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
        target.aggregate = selected_aggregate_column
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
        exp_tree = MQL_GRAMMAR.parse(body)
        parsed: InitialParseResult = MQLVisitor().visit(exp_tree)
        if not parsed.aggregate:
            raise ParsingException("No aggregate specified in MQL query")

        selected_columns = [parsed.aggregate]
        if parsed.groupby:
            selected_columns.extend(parsed.groupby)
        groupby = [g.expression for g in parsed.groupby] if parsed.groupby else None

        id_value = parsed.metric_id or parsed.mri or parsed.public_name
        metric_id_condition = binary_condition(
            ConditionFunctions.EQ,
            Column(None, None, "metric_id"),
            Literal(None, id_value),
        )
        if parsed.conditions:
            conditions = combine_and_conditions(
                [metric_id_condition, *parsed.conditions]
            )
        else:
            conditions = metric_id_condition

        query = LogicalQuery(
            from_clause=None,
            selected_columns=selected_columns,
            condition=conditions,
            groupby=groupby,
        )
    except Exception as e:
        raise e

    return query


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

    try:
        entity_key = EntityKey(mql_context.entity)
        query.set_from_clause(
            QueryEntity(key=entity_key, schema=get_entity(entity_key).get_data_model())
        )
    except Exception as e:
        raise ParsingException(f"Invalid entity {mql_context.entity}") from e

    populate_start_end_time(query, mql_context, entity_key)
    populate_scope(query, mql_context)
    populate_rollup(query, mql_context)
    populate_limit(query, mql_context)
    populate_offset(query, mql_context)

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
        query = parse_mql_query_body(body)
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
