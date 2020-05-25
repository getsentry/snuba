import logging
from typing import Any, MutableMapping, Optional

from snuba import state
from snuba.clickhouse.escaping import NEGATE_RE
from snuba.datasets.dataset import Dataset
from snuba.query.expressions import Column, Expression
from snuba.query.logical import OrderBy, OrderByDirection, Query
from snuba.query.parser.conditions import parse_conditions_to_expr
from snuba.query.parser.expressions import parse_aggregation, parse_expression
from snuba.util import is_function, to_list, tuplify

logger = logging.getLogger(__name__)


def build_query(body: MutableMapping[str, Any], dataset: Dataset) -> Query:
    """
    Parses the query body generating the AST and resolves the columns.
    This only takes into account the initial query body.
    Extensions are parsed by extension processors and are supposed to update the AST.
    """
    try:
        query = parse_query(body, dataset)
        resolve_columns(query, dataset)
        return query
    except Exception as e:
        # During the development there is no need to fail Snuba queries if the parser
        # has an issue, anyway the production query is ran based on the old query
        # representation.
        # Once we will be actually using the ast to build the Clickhouse query
        # this try/except block will disappear.
        enforce_validity = state.get_config("query_parsing_enforce_validity", 0)
        if enforce_validity:
            raise e
        else:
            logger.warning("Failed to parse query", exc_info=True)
            return Query(body, None)


def resolve_columns(query: Query, dataset: Dataset) -> None:
    def resolve_col(expr: Expression) -> Expression:
        if not isinstance(expr, Column):
            return expr

        resolved_col = dataset.get_column_resolver().resolve_column(expr.column_name)
        assert (
            resolved_col is not None
        ), f"Invalid column expression {expr.column_name} for dataset {type(dataset).__name__}."
        return Column(
            expr.alias,
            resolved_col.table_name,
            resolved_col.column_name,
            tuple(resolved_col.path),
        )

    query.transform_expressions(resolve_col)


def parse_query(body: MutableMapping[str, Any], dataset: Dataset) -> Query:
    aggregate_exprs = []
    for aggregation in body.get("aggregations", []):
        assert isinstance(aggregation, (list, tuple))
        aggregation_function = aggregation[0]
        column_expr = aggregation[1]
        column_expr = column_expr if column_expr else []
        alias = aggregation[2]
        alias = alias if alias else None

        aggregate_exprs.append(
            parse_aggregation(aggregation_function, column_expr, alias)
        )

    groupby_exprs = [
        parse_expression(tuplify(group_by))
        for group_by in to_list(body.get("groupby", []))
    ]
    select_exprs = [
        parse_expression(tuplify(select)) for select in body.get("selected_columns", [])
    ]

    selected_cols = groupby_exprs + aggregate_exprs + select_exprs

    arrayjoin = body.get("arrayjoin")
    if arrayjoin:
        array_join_expr: Optional[Expression] = parse_expression(body["arrayjoin"])
    else:
        array_join_expr = None

    where_expr = parse_conditions_to_expr(
        body.get("conditions", []), dataset, arrayjoin
    )
    having_expr = parse_conditions_to_expr(body.get("having", []), dataset, arrayjoin)

    orderby_exprs = []
    for orderby in to_list(body.get("orderby", [])):
        if isinstance(orderby, str):
            match = NEGATE_RE.match(orderby)
            assert match is not None, f"Invalid Order By clause {orderby}"
            direction, col = match.groups()
            orderby = col
        elif is_function(orderby):
            match = NEGATE_RE.match(orderby[0])
            assert match is not None, f"Invalid Order By clause {orderby}"
            direction, col = match.groups()
            orderby = [col] + orderby[1:]
        else:
            raise ValueError(f"Invalid Order By clause {orderby}")
        orderby_parsed = parse_expression(tuplify(orderby))
        orderby_exprs.append(
            OrderBy(
                OrderByDirection.DESC if direction == "-" else OrderByDirection.ASC,
                orderby_parsed,
            )
        )

    return Query(
        body,
        None,
        selected_columns=selected_cols,
        array_join=array_join_expr,
        condition=where_expr,
        groupby=groupby_exprs,
        having=having_expr,
        order_by=orderby_exprs,
    )
