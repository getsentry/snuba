from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple, Union

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
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.indexer.resolver import resolve_mappings
from snuba.query.logical import Query as LogicalQuery
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

logger = logging.getLogger("snuba.mql.parser")


class MQLVisitor(NodeVisitor):  # type: ignore
    """
    Builds the arguments for a Snuba AST from the MQL Parsimonious parse tree.
    """

    def visit(self, node: Node) -> Any:
        """Walk a parse tree, transforming it into a MetricsQuery object.

        Recursively descend a parse tree, dispatching to the method named after
        the rule in the :class:`~parsimonious.grammar.Grammar` that produced
        each node. If, for example, a rule was... ::

            bold = '<b>'

        ...the ``visit_bold()`` method would be called.
        """
        method = getattr(self, "visit_" + node.expr_name, self.generic_visit)
        try:
            result = method(node, [self.visit(n) for n in node])
            return result
        except Exception as e:
            raise e

    def visit_expression(
        self, node: Node, children: Tuple[dict[str, str | dict[str, str]], Any]
    ) -> dict[str, str | dict[str, str]]:
        args, zero_or_more_others = children
        return args

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryException("Arithmetic function not supported yet")

    def visit_term(
        self, node: Node, children: Tuple[dict[str, str | dict[str, str]], Any]
    ) -> dict[str, str | dict[str, str]]:
        term, zero_or_more_others = children
        if zero_or_more_others:
            raise InvalidQueryException("Arithmetic function not supported yet")
        return term

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> str:
        raise InvalidQueryException("Arithmetic function not supported yet")

    def visit_coefficient(
        self, node: Node, children: Tuple[dict[str, str | dict[str, str]]]
    ) -> dict[str, str | dict[str, str]]:
        return children[0]

    def visit_number(self, node: Node, children: Sequence[Any]) -> float:
        return float(node.text)

    def visit_filter(
        self,
        node: Node,
        children: Tuple[
            dict[str, list[str]],
            Sequence[Any],
            Sequence[Any],
            Any,
        ],
    ) -> dict[str, list[str]]:
        target, packed_filters, packed_groupbys, *_ = children
        assert isinstance(target, dict)
        if packed_filters:
            assert isinstance(packed_filters, list)
            _, _, filter_expr, *_ = packed_filters[0]
            if "filters" in target:
                target["filters"] = target["filters"] + [filter_expr]
            else:
                target["filters"] = [filter_expr]

        if packed_groupbys:
            assert isinstance(packed_groupbys, list)
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            if "groupby" in target:
                target["groupby"] = target["groupby"] + group_by
            else:
                target["groupby"] = group_by

        return target

    def _filter(
        self, children: Sequence[Any], operator: BooleanFunctions
    ) -> FunctionCall:
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
        children: Tuple[
            Optional[list[ConditionFunctions]],
            Sequence[str],
            Any,
            Any,
            Any,
            Union[str, Sequence[str]],
        ],
    ) -> Tuple[str, str, Union[str, Sequence[str]]]:
        factor, *_ = children
        if isinstance(factor, FunctionCall):
            # If we have a parenthesized expression, we just return it.
            return factor
        condition_op, lhs, _, _, _, rhs = factor
        if isinstance(rhs, list):
            if not condition_op:
                op = ConditionFunctions.IN
            elif len(condition_op) == 1 and condition_op[0] == "!":
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
            if not condition_op:
                op = ConditionFunctions.EQ
            elif len(condition_op) == 1 and condition_op[0] == "!":
                op = ConditionFunctions.NEQ
            return FunctionCall(
                None,
                op,
                (Column(None, None, lhs[0]), Literal(None, rhs)),
            )

    def visit_nested_expr(self, node: Node, children: Sequence[Any]) -> Any:
        _, _, filter_expr, *_ = children
        return filter_expr

    def visit_function(
        self,
        node: Node,
        children: Tuple[
            dict[
                str,
                Union[
                    str,
                    Sequence[str],
                    Tuple[str, str, Union[str, Sequence[str]]],
                    SelectedExpression,
                ],
            ],
            Sequence[Union[str, Sequence[str]]],
        ],
    ) -> dict[
        str,
        Union[
            str,
            Sequence[str],
            Tuple[str, str, Union[str, Sequence[str]]],
            SelectedExpression,
        ],
    ]:
        targets, packed_groupbys = children
        target = targets[0]
        if packed_groupbys:
            group_by = packed_groupbys[0]
            target["groupby"] = group_by

        return target

    def visit_group_by(
        self,
        node: Node,
        children: Tuple[Any, Any, Any, Sequence[Sequence[str]]],
    ) -> Sequence[str]:
        *_, groupbys = children
        groupby = groupbys[0]
        if isinstance(groupby, str):
            groupby = [groupby]
        columns = [
            Column(
                alias=column_name,
                table_name=None,
                column_name=column_name,
            )
            for column_name in groupby
        ]
        return columns

    def visit_condition_op(self, node: Node, children: Sequence[Any]) -> str:
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
        children: Sequence[Union[Mapping[str, str], Sequence[Mapping[str, str]]]],
    ) -> Mapping[str, str]:
        target = children[0]
        if isinstance(children[0], list):
            target = children[0][0]
        assert isinstance(target, dict)
        return target

    def visit_variable(self, node: Node, children: Sequence[Any]) -> str:
        raise InvalidQueryException("Variables are not supported yet")

    def visit_nested_expression(
        self, node: Node, children: Tuple[Any, Any, dict[str, str | dict[str, str]]]
    ) -> dict[str, str | dict[str, str]]:
        return children[2]

    def visit_aggregate(
        self,
        node: Node,
        children: Tuple[
            str, Tuple[Any, Any, dict[str, Union[str, SelectedExpression]], Any, Any]
        ],
    ) -> dict[str, Union[str, SelectedExpression]]:
        aggregate_name, zero_or_one = children
        _, _, target, zero_or_more_others, *_ = zero_or_one
        if "mri" in target:
            metric_name = target["mri"]
        else:
            metric_name = target["public_name"]
        selected_aggregate_column = [
            SelectedExpression(
                f"{aggregate_name}({metric_name})",
                FunctionCall(
                    AGGREGATE_ALIAS,
                    aggregate_name,
                    (Column(None, None, "value"),),
                ),
            ),
        ]
        if "selected_aggregate" in target:
            target["selected_aggregate"] = (
                selected_aggregate_column + target["selected_aggregate"]
            )
        else:
            target["selected_aggregate"] = selected_aggregate_column
        return target

    def visit_curried_aggregate(
        self,
        node: Node,
        children: Tuple[
            str,
            Tuple[Any, Any, Sequence[Sequence[Union[str, int, float]]], Any, Any],
            Tuple[Any, Any, dict[str, Union[str, SelectedExpression]], Any, Any],
        ],
    ) -> dict[str, Union[str, SelectedExpression]]:
        aggregate_name, agg_params, zero_or_one = children
        _, _, target, _, *_ = zero_or_one
        _, _, agg_param_list, _, *_ = agg_params
        aggregate_params = agg_param_list[0] if agg_param_list else []

        if "mri" in target:
            metric_name = target["mri"]
        else:
            metric_name = target["public_name"]
        params_str = ", ".join(map(str, aggregate_params))
        selected_aggregate_column = [
            SelectedExpression(
                f"{aggregate_name}({params_str})({metric_name})",
                CurriedFunctionCall(
                    AGGREGATE_ALIAS,
                    FunctionCall(
                        None,
                        aggregate_name,
                        tuple(
                            Literal(alias=None, value=param)
                            for param in aggregate_params
                        ),
                    ),
                    (Column(None, None, "value"),),
                ),
            )
        ]
        if "selected_aggregate" in target:
            target["selected_aggregate"] = (
                selected_aggregate_column + target["selected_aggregate"]
            )
        else:
            target["selected_aggregate"] = selected_aggregate_column
        return target

    def visit_param(
        self, node: Node, children: Tuple[Union[str, int, float], Any]
    ) -> Union[str, int, float]:
        param, *_ = children
        return param

    def visit_param_expression(
        self, node: Node, children: Tuple[Union[str, int, float], Any]
    ) -> Union[str, int, float]:
        (param,) = children
        return param

    def visit_aggregate_list(
        self,
        node: Node,
        children: Tuple[list[Union[str, int, float]], Optional[Union[str, int, float]]],
    ) -> Sequence[str | int | float]:
        agg_params, param = children
        if param is not None:
            agg_params.append(param)
        assert isinstance(agg_params, list)
        return agg_params

    def visit_aggregate_name(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def visit_quoted_mri(self, node: Node, children: Sequence[Any]) -> dict[str, str]:
        assert isinstance(node.text, str)
        return {"mri": str(node.text[1:-1])}

    def visit_unquoted_mri(self, node: Node, children: Sequence[Any]) -> dict[str, str]:
        assert isinstance(node.text, str)
        return {"mri": str(node.text)}

    def visit_quoted_public_name(
        self, node: Node, children: Sequence[Any]
    ) -> dict[str, str]:
        assert isinstance(node.text, str)
        return {"public_name": str(node.text[1:-1])}

    def visit_unquoted_public_name(
        self, node: Node, children: Sequence[Any]
    ) -> dict[str, str]:
        assert isinstance(node.text, str)
        return {"public_name": str(node.text)}

    def visit_identifier(self, node: Node, children: Sequence[Any]) -> str:
        assert isinstance(node.text, str)
        return node.text

    def generic_visit(self, node: Node, children: Sequence[Any]) -> Any:
        """The generic visit method."""
        return children


def parse_mql_query_initial(
    body: str,
    mql_context: Mapping[str, Any],
) -> Tuple[Mapping[str, Any], Union[CompositeQuery[QueryEntity], LogicalQuery]]:
    """
    Parses the query body MQL generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """
    try:
        """
        Example of parsed tree for:
        'max(transaction.user{dist:["dist1", "dist2"]}) by transaction',

        {
            'public_name': 'transaction.user',
            'selected_aggregate': [SelectedExpression(name='sum(d:transactions/duration@millisecond)', expression=sum(value) AS `aggregate_value`)],
            'filters': [ IN(dist, tuple('dist1', 'dist2')) ],
            'groupby': [Column('transaction')]
        }
        """
        exp_tree = MQL_GRAMMAR.parse(body)
        parsed: dict[str, Any] = MQLVisitor().visit(exp_tree)
        selected_columns = assemble_selected_columns(
            parsed.get("selected_aggregate", None), parsed.get("groupby", None)
        )
        conditions = parsed.get("filters", None)
        if conditions:
            conditions = combine_and_conditions(conditions)
        query = LogicalQuery(
            from_clause=None,
            selected_columns=selected_columns,
            condition=conditions,
            groupby=parsed.get("groupby", None),
        )
    except Exception as e:
        raise e

    if "entity" not in mql_context:
        raise InvalidQueryException("No entity specified in MQL context")
    entity_name = mql_context["entity"]
    entity_key = EntityKey(entity_name)
    query.set_from_clause(
        QueryEntity(key=entity_key, schema=get_entity(entity_key).get_data_model())
    )

    mql_context_args = extract_mql_context_args(parsed, mql_context, entity_key)
    query.add_condition_to_ast(mql_context_args["filters"])
    query.set_ast_orderby(mql_context_args["order_by"])
    query.set_limit(mql_context_args["limit"])
    query.set_offset(mql_context_args["offset"])
    query.set_granularity(mql_context_args["granularity"])
    query.set_totals(mql_context_args["totals"])

    return parsed, query


def assemble_selected_columns(
    selected_aggregate: Optional[Sequence[SelectedExpression]],
    groupby: Optional[Sequence[Column]],
) -> Sequence[SelectedExpression]:
    selected_columns = selected_aggregate if selected_aggregate else []
    if groupby:
        groupby_selected_columns = [
            SelectedExpression(name=column.alias, expression=column)
            for column in groupby
        ]
        selected_columns.extend(groupby_selected_columns)
    return selected_columns


def extract_mql_context_args(
    parsed: Mapping[str, Any],
    mql_context: Mapping[str, Any],
    entity_key: EntityKey,
) -> Mapping[str, Any]:
    """
    Extracts all metadata from MQL context, creates the appropriate expressions for them,
    and returns them in a formatted dictionary.

    Example of serialized MQL context:
        "mql_context": {
            "entity": "generic_metrics_distributions"
            "start": "2023-01-02T03:04:05+00:00",
            "end": "2023-01-16T03:04:05+00:00",
            "rollup": {
                    "orderby": {"column_name": "timestamp", "direction": "ASC"},
                    "granularity": "3600",
                    "interval": "3600",
                    "with_totals": "",
            },
            "scope": {
                    "org_ids": ["1"],
                    "project_ids": ["11"],
                    "use_case_id": "transactions",
            },
            "limit": "",
            "offset": "0",
            "indexer_mappings": {
                "d:transactions/duration@millisecond": "123456", ...
            }
        }
    """
    mql_context_args: dict[str, Any] = {}
    filters = []
    if "indexer_mappings" not in mql_context:
        raise InvalidQueryException("No indexer mappings specified in MQL context.")

    filters.extend(extract_scope(parsed, mql_context))
    filters.extend(extract_start_end_time(parsed, mql_context, entity_key))
    order_by, granularity, totals = extract_rollup(parsed, mql_context)
    limit = extract_limit(mql_context)
    offset = extract_offset(mql_context)

    mql_context_args["filters"] = combine_and_conditions(filters)
    mql_context_args["order_by"] = order_by
    mql_context_args["granularity"] = granularity
    mql_context_args["totals"] = totals
    mql_context_args["limit"] = limit
    mql_context_args["offset"] = offset

    return mql_context_args


def extract_start_end_time(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any], entity_key: EntityKey
) -> list[FunctionCall]:
    filters = []
    if "start" not in mql_context or "end" not in mql_context:
        raise InvalidQueryException(
            "No start or end specified in MQL context indexer_mappings."
        )
    entity = get_entity(entity_key)
    required_timestamp_column = (
        entity.required_time_column if entity.required_time_column else "timestamp"
    )
    start = mql_context["start"]
    filters.append(
        binary_condition(
            ConditionFunctions.GTE,
            Column(alias=None, table_name=None, column_name=required_timestamp_column),
            FunctionCall(
                alias=None,
                function_name="toDateTime",
                parameters=(Literal(alias=None, value=start),),
            ),
        )
    )
    end = mql_context["end"]
    filters.append(
        binary_condition(
            ConditionFunctions.LT,
            Column(alias=None, table_name=None, column_name=required_timestamp_column),
            FunctionCall(
                alias=None,
                function_name="toDateTime",
                parameters=(Literal(alias=None, value=end),),
            ),
        )
    )
    return filters


def extract_scope(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> list[FunctionCall]:
    filters = []
    if "scope" not in mql_context:
        raise InvalidQueryException("No scope specified in MQL context.")
    scope = mql_context["scope"]
    filters.append(
        binary_condition(
            ConditionFunctions.IN,
            Column(alias=None, table_name=None, column_name="project_id"),
            FunctionCall(
                alias=None,
                function_name="tuple",
                parameters=tuple(
                    Literal(alias=None, value=int(project_id))
                    for project_id in scope["project_ids"]
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
                    for org_id in scope["org_ids"]
                ),
            ),
        )
    )
    filters.append(
        binary_condition(
            ConditionFunctions.EQ,
            Column(alias=None, table_name=None, column_name="use_case_id"),
            Literal(alias=None, value=scope["use_case_id"]),
        )
    )
    return filters


def extract_rollup(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> tuple[list[OrderBy], int, bool]:
    if "rollup" not in mql_context:
        raise InvalidQueryException("No rollup specified in MQL context.")

    # Extract orderby
    order_by = []
    if "orderby" in mql_context["rollup"]:
        for order_by_info in mql_context["rollup"]["orderby"]:
            direction = (
                OrderByDirection.ASC
                if order_by_info["direction"] == "ASC"
                else OrderByDirection.DESC
            )
            order_by.append(
                OrderBy(
                    direction,
                    Column(
                        alias=None,
                        table_name=None,
                        column_name=order_by_info["column_name"],
                    ),
                )
            )

    # Extract granularity
    # TODO: We eventually want to move the automatic granularity functionality in Sentry into here.
    if "granularity" not in mql_context["rollup"]:
        raise InvalidQueryException("No granularity specified in MQL context rollup.")
    granularity = int(mql_context["rollup"]["granularity"])

    # Extract with totals
    with_totals = mql_context["rollup"].get("with_totals") == "True"

    return order_by, granularity, with_totals


def extract_limit(mql_context: Mapping[str, Any]) -> Optional[int]:
    if (
        "limit" in mql_context
        and mql_context["limit"] != ""
        and mql_context["limit"].isdigit()
    ):
        limit = int(mql_context["limit"])
        if limit > MAX_LIMIT:
            raise ParsingException(
                "queries cannot have a limit higher than 10000", should_report=False
            )
        return int(mql_context["limit"])
    return 1000


def extract_offset(mql_context: Mapping[str, Any]) -> int:
    if (
        "limit" in mql_context
        and mql_context["offset"] != ""
        and mql_context["offset"].isdigit()
    ):
        return int(mql_context["offset"])
    return 0


CustomProcessors = Sequence[
    Callable[[Union[CompositeQuery[QueryEntity], LogicalQuery]], None]
]


def parse_mql_query(
    body: str,
    mql_context: Mapping[str, Any],
    dataset: Dataset,
    custom_processing: Optional[CustomProcessors] = None,
    settings: QuerySettings | None = None,
) -> Tuple[Union[CompositeQuery[QueryEntity], LogicalQuery], str]:
    with sentry_sdk.start_span(op="parser", description="parse_mql_query_initial"):
        parsed, query = parse_mql_query_initial(body, mql_context)

    with sentry_sdk.start_span(op="processor", description="resolve_indexer_mappings"):
        query = resolve_mappings(query, parsed, mql_context)

    if settings and settings.get_dry_run():
        explain_meta.set_original_ast(str(query))

    # NOTE (volo): The anonymizer that runs after this function call chokes on
    # OR and AND clauses with multiple parameters so we have to treeify them
    # before we run the anonymizer and the rest of the post processors
    with sentry_sdk.start_span(op="processor", description="treeify_conditions"):
        _post_process(query, [_treeify_or_and_conditions], settings)

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
