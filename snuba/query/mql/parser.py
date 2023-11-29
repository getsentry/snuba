from __future__ import annotations

import logging
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple, Union

import sentry_sdk
from parsimonious.nodes import Node, NodeVisitor
from snuba_sdk.conditions import OPERATOR_TO_FUNCTION, ConditionFunction, Op
from snuba_sdk.dsl.dsl import MQL_GRAMMAR

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import binary_condition, combine_and_conditions
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.parser.exceptions import ParsingException
from snuba.query.query_settings import QuerySettings
from snuba.query.snql.anonymize import format_snql_anonymized
from snuba.query.snql.parser import (
    POST_PROCESSORS,
    VALIDATORS,
    _post_process,
    _replace_time_condition,
    _treeify_or_and_conditions,
)
from snuba.state import explain_meta

logger = logging.getLogger("snuba.mql.parser")


class MQLVisitor(NodeVisitor):
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
        self, node: Node, children: Sequence[Any]
    ) -> Mapping[str, Any]:
        args, zero_or_more_others = children
        return args

    def visit_expr_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryException("Arithmetic function not supported yet")

    def visit_term(self, node: Node, children: Sequence[Any]) -> Mapping[str, Any]:
        term, zero_or_more_others = children
        if zero_or_more_others:
            raise InvalidQueryException("Arithmetic function not supported yet")
        return term

    def visit_term_op(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryException("Arithmetic function not supported yet")

    def visit_coefficient(self, node: Node, children: Any) -> Mapping[str, Any]:
        return children[0]

    def visit_number(self, node: Node, children: Sequence[Any]) -> float:
        return float(node.text)

    def visit_filter(self, node: Node, children: Sequence[Any]) -> Mapping[str, Any]:
        args, packed_filters, packed_groupbys, *_ = children
        assert isinstance(args, dict)
        if packed_filters:
            _, _, first, zero_or_more_others, *_ = packed_filters[0]
            new_filters = [first, *(v for _, _, _, v in zero_or_more_others)]
            if "filters" in args:
                args["filters"] = args["filters"] + new_filters
            else:
                args["filters"] = new_filters

        if packed_groupbys:
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            if "groupby" in args:
                args["groupby"] = args["groupby"] + group_by
            else:
                args["groupby"] = group_by

        return args

    def visit_condition(
        self, node: Node, children: Sequence[Any]
    ) -> Tuple[str, Any, Any]:
        condition_op, lhs, _, _, _, rhs = children
        op = Op.EQ
        if not condition_op and isinstance(rhs, list):
            op = Op.IN
        elif len(condition_op) == 1 and condition_op[0] == Op.NOT:
            if isinstance(rhs, str):
                op = Op.NEQ
            elif isinstance(rhs, list):
                op = Op.NOT_IN
        return (OPERATOR_TO_FUNCTION[op].value, lhs[0], rhs)

    def visit_function(self, node: Node, children: Sequence[Any]) -> Mapping[str, Any]:
        target, packed_groupbys = children
        if packed_groupbys:
            group_by = packed_groupbys[0]
            if not isinstance(group_by, list):
                group_by = [group_by]
            target["groupby"] = group_by

        return target

    def visit_group_by(self, node: Node, children: Sequence[Any]) -> Any:
        *_, groupby = children
        columns = groupby[0]
        return columns

    def visit_condition_op(self, node: Node, children: Sequence[Any]) -> Op:
        return Op(node.text)

    def visit_tag_key(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def visit_tag_value(
        self, node: Node, children: Sequence[Union[str, Sequence[str]]]
    ) -> str:
        tag_value = children[0]
        return tag_value

    def visit_unquoted_string(self, node: Node, children: Sequence[Any]) -> str:
        return str(node.text)

    def visit_quoted_string(self, node: Node, children: Sequence[Any]) -> str:
        return str(node.text[1:-1])

    def visit_string_tuple(self, node: Node, children: Sequence[Any]) -> Sequence[str]:
        _, _, first, zero_or_more_others, _, _ = children
        return [first[0], *(v[0] for _, _, _, v in zero_or_more_others)]

    def visit_group_by_name(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def visit_group_by_name_tuple(
        self, node: Node, children: Sequence[Any]
    ) -> Sequence[str]:
        _, _, first, zero_or_more_others, _, _ = children
        return [first, *(v for _, _, _, v in zero_or_more_others)]

    def visit_target(self, node: Node, children: Sequence[Any]) -> Mapping[str, Any]:
        target = children[0]
        if isinstance(children[0], list):
            target = children[0][0]
        return target

    def visit_variable(self, node: Node, children: Sequence[Any]) -> Any:
        raise InvalidQueryException("Variables are not supported yet")

    def visit_nested_expression(
        self, node: Node, children: Sequence[Any]
    ) -> Mapping[str, Any]:
        return children[2]

    def visit_aggregate(
        self, node: Node, children: Sequence[Any]
    ) -> SelectedExpression:
        aggregate_name, zero_or_one = children
        _, _, target, zero_or_more_others, *_ = zero_or_one

        if "mri" in target:
            metric_name = target["mri"]
        elif "public_name" in target:
            metric_name = target["public_name"]
        else:
            metric_name = ""
        target["aggregate"] = SelectedExpression(
            name=f"{aggregate_name}({metric_name})",
            expression=FunctionCall(
                alias=None,
                function_name=aggregate_name,
                parameters=[Column(alias=None, table_name=None, column_name="value")],
            ),
        )
        return target

    def visit_aggregate_name(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def visit_quoted_mri(
        self, node: Node, children: Sequence[Any]
    ) -> Mapping[str, str]:
        return {"mri": str(node.text[1:-1])}

    def visit_unquoted_mri(
        self, node: Node, children: Sequence[Any]
    ) -> Mapping[str, str]:
        return {"mri": str(node.text)}

    def visit_quoted_public_name(
        self, node: Node, children: Sequence[Any]
    ) -> Mapping[str, str]:
        return {"public_name": str(node.text[1:-1])}

    def visit_unquoted_public_name(
        self, node: Node, children: Sequence[Any]
    ) -> Mapping[str, str]:
        return {"public_name": str(node.text)}

    def visit_identifier(self, node: Node, children: Sequence[Any]) -> str:
        return node.text

    def generic_visit(self, node: Node, children: Sequence[Any]) -> Any:
        """The generic visit method."""
        return children


def parse_mql_query_initial(
    body: str,
    mql_context: Mapping[str, Any],
) -> Union[CompositeQuery[QueryEntity], LogicalQuery]:
    """
    Parses the query body MQL generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """
    try:
        exp_tree = MQL_GRAMMAR.parse(body)
        parsed = MQLVisitor().visit(exp_tree)
    except Exception as e:
        raise e

    if "entity" not in mql_context:
        raise InvalidQueryException("No entity specified in MQL context")
    entity_name = mql_context["entity"]
    entity_key = EntityKey(entity_name)
    args = {
        "from_clause": QueryEntity(
            key=entity_key, schema=get_entity(entity_key).get_data_model()
        ),
    }

    resolved_args = extract_args_from_mql_context(parsed, mql_context)
    selected_columns = extract_selected_columns(parsed, resolved_args, mql_context)

    args["selected_columns"] = selected_columns
    args["groupby"] = resolved_args["groupby"]
    args["condition"] = resolved_args["filters"]
    args["order_by"] = resolved_args["order_by"]
    args["limit"] = resolved_args["limit"]
    args["offset"] = resolved_args["offset"]
    args["granularity"] = resolved_args["granularity"]
    args["totals"] = resolved_args["totals"]

    query = LogicalQuery(**args)
    return query


def extract_selected_columns(
    parsed: Mapping[str, Any],
    resolved_args: Mapping[str, Any],
    mql_context: Mapping[str, Any],
) -> list[SelectedExpression]:
    selected_columns = []
    if "aggregate" in parsed:
        selected_columns.append(parsed["aggregate"])
    if "groupby" in resolved_args:
        columns = resolved_args["groupby"]
        for column in columns:
            assert isinstance(column, Column)
            selected_columns.append(
                SelectedExpression(name=column.alias, expression=column)
            )
    return selected_columns


def extract_args_from_mql_context(
    parsed: Mapping[str, Any],
    mql_context: Mapping[str, Any],
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
    resolved_args = {}
    filters = []
    groupbys = []
    if "indexer_mappings" not in mql_context:
        raise InvalidQueryException("No indexer mappings specified in MQL context.")

    filters.extend(extract_scope(parsed, mql_context))
    filters.extend(extract_start_end_time(parsed, mql_context))
    filters.extend(extract_metric_id(parsed, mql_context))
    filters.extend(extract_resolved_tag_filters(parsed, mql_context))

    groupbys.extend(extract_resolved_gropupby(parsed, mql_context))
    order_by, granularity, totals = extract_rollup(parsed, mql_context)
    limit = extract_limit(mql_context)
    offset = extract_offset(mql_context)

    resolved_args["filters"] = combine_and_conditions(filters)
    resolved_args["groupby"] = groupbys
    resolved_args["order_by"] = order_by
    resolved_args["granularity"] = granularity
    resolved_args["totals"] = totals
    resolved_args["limit"] = limit
    resolved_args["offset"] = offset

    return resolved_args


def extract_metric_id(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> list[FunctionCall]:
    if "mri" not in parsed and "public_name" in parsed:
        public_name = parsed["public_name"]
        mri = mql_context["indexer_mappings"][public_name]
    else:
        mri = parsed["mri"]

    if mri not in mql_context["indexer_mappings"]:
        raise InvalidQueryException(
            "No mri to metric_id mapping found in MQL context indexer_mappings."
        )
    metric_id = mql_context["indexer_mappings"][mri]
    return [
        (
            binary_condition(
                ConditionFunction.EQ.value,
                Column(alias=None, table_name=None, column_name="metric_id"),
                Literal(alias=None, value=metric_id),
            )
        )
    ]


def extract_resolved_tag_filters(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> list[FunctionCall]:
    # Extract resolved tag filters from mql context indexer_mappings
    filters = []
    if "filters" in parsed:
        for filter in parsed["filters"]:
            operator, lhs, rhs = filter
            if lhs in mql_context["indexer_mappings"]:
                resolved = mql_context["indexer_mappings"][lhs]
                lhs_column_name = f"tags_raw[{resolved}]"
            else:
                lhs_column_name = lhs

            if isinstance(rhs, str):
                filters.append(
                    binary_condition(
                        operator,
                        Column(
                            alias=lhs,
                            table_name=None,
                            column_name=lhs_column_name,
                        ),
                        Literal(alias=None, value=rhs),
                    )
                )
            else:
                filters.append(
                    binary_condition(
                        operator,
                        Column(
                            alias=lhs,
                            table_name=None,
                            column_name=lhs_column_name,
                        ),
                        FunctionCall(
                            alias=None,
                            function_name="tuple",
                            parameters=[
                                Literal(alias=None, value=item) for item in rhs
                            ],
                        ),
                    )
                )

    return filters


def extract_start_end_time(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> list[FunctionCall]:
    filters = []
    if "start" not in mql_context or "end" not in mql_context:
        raise InvalidQueryException(
            "No start or end specified in MQL context indexer_mappings."
        )
    start = mql_context["start"]
    filters.append(
        binary_condition(
            ConditionFunction.GTE.value,
            Column(alias=None, table_name=None, column_name="timestamp"),
            FunctionCall(
                alias=None,
                function_name="toDateTime",
                parameters=[Literal(alias=None, value=start)],
            ),
        )
    )
    end = mql_context["end"]
    filters.append(
        binary_condition(
            ConditionFunction.LT.value,
            Column(alias=None, table_name=None, column_name="timestamp"),
            FunctionCall(
                alias=None,
                function_name="toDateTime",
                parameters=[Literal(alias=None, value=end)],
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
            ConditionFunction.IN.value,
            Column(alias=None, table_name=None, column_name="project_id"),
            FunctionCall(
                alias=None,
                function_name="tuple",
                parameters=[
                    Literal(alias=None, value=int(project_id))
                    for project_id in scope["project_ids"]
                ],
            ),
        )
    )
    filters.append(
        binary_condition(
            ConditionFunction.IN.value,
            Column(alias=None, table_name=None, column_name="org_id"),
            FunctionCall(
                alias=None,
                function_name="tuple",
                parameters=[
                    Literal(alias=None, value=int(org_id))
                    for org_id in scope["org_ids"]
                ],
            ),
        )
    )
    filters.append(
        binary_condition(
            ConditionFunction.EQ.value,
            Column(alias=None, table_name=None, column_name="use_case_id"),
            Literal(alias=None, value=scope["use_case_id"]),
        )
    )
    return filters


def extract_resolved_gropupby(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> list[FunctionCall]:
    groupbys = []
    if "groupby" in parsed:
        for groupby_col_name in parsed["groupby"]:
            if groupby_col_name in mql_context["indexer_mappings"]:
                resolved = mql_context["indexer_mappings"][groupby_col_name]
                resolved_column_name = f"tags_raw[{resolved}]"
            else:
                resolved_column_name = groupby_col_name
            groupbys.append(
                Column(
                    alias=groupby_col_name,
                    table_name=None,
                    column_name=resolved_column_name,
                )
            )
    return groupbys


def extract_rollup(
    parsed: Mapping[str, Any], mql_context: Mapping[str, Any]
) -> tuple[list[FunctionCall], int, bool]:
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
    if "granularity" not in mql_context["rollup"]:
        raise InvalidQueryException("No granularity specified in MQL context rollup.")
    granularity = int(mql_context["rollup"]["granularity"])

    # Extract with totals
    with_totals = False
    if (
        "with_totals" in mql_context["rollup"]
        and mql_context["rollup"]["with_totals"] == "True"
    ):
        with_totals = True

    return order_by, granularity, with_totals


def extract_limit(mql_context: Mapping[str, Any]) -> Optional[int]:
    if (
        "limit" in mql_context
        and mql_context["limit"] != ""
        and mql_context["limit"].isdigit()
    ):
        limit = int(mql_context["limit"])
        if limit > 10000:
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
        query = parse_mql_query_initial(body, mql_context)

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
