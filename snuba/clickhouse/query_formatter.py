from typing import Sequence, Tuple, Union, NamedTuple, Mapping, Optional

from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.datasets.schemas import RelationalSource
from snuba.query.composite import CompositeQuery
from snuba.query.parsing import ParsingContext

FormattableQuery = Union[Query, CompositeQuery[RelationalSource]]


class FormattedQuery(NamedTuple):
    # TODO: This is going to be a little more complex for subqueries
    # and joins in order to have a useful tracing representation.
    clauses: Sequence[Tuple[str, str]]

    def get_sql(self, format: Optional[str] = None) -> str:
        query = " ".join([c for _, c in self.clauses])
        if format is not None:
            query = f"{query} FORMAT {format}"

        return query

    def get_mapping(self) -> Mapping[str, str]:
        return dict(self.clauses)


def format_query(query: FormattableQuery) -> FormattedQuery:
    """
    Formats a Query from the AST representation into an intermediate
    structure that can either serialized into a string (for clickhouse)
    or to a dictionary (for logging and tracing).

    This is the entry point for any type of query, whether simple or
    composite.

    Remark: The ideal way to structure this would be a visitor just
    like we doo for the AST expression but this is not possible
    without introducing either circular dependencies or dropping
    type checking since the visitor and the classes to visit cannot
    be in the same module as of now.
    """

    if isinstance(query, Query):
        return format_processable_query(query)
    else:
        # TODO: Support composite queries
        raise NotImplementedError("Query type not yet supported")


def format_processable_query(query: Query) -> FormattedQuery:
    ast_groupby = query.get_groupby_from_ast()
    ast_having = query.get_having_from_ast()

    # TODO: Move this into a validator much earlier in the query.
    if ast_having:
        assert ast_groupby, "found HAVING clause with no GROUP BY"

    parsing_context = ParsingContext()
    formatter = ClickhouseExpressionFormatter(parsing_context)

    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns_from_ast()
    ]
    select_clause = f"SELECT {', '.join(selected_cols)}"

    from_clause = f"FROM {query.get_from_clause().format_from()}"

    if query.get_final():
        from_clause = f"{from_clause} FINAL"

    ast_sample = query.get_sample()
    if ast_sample:
        from_clause = f"{from_clause} SAMPLE {ast_sample}"

    ast_arrayjoin = query.get_arrayjoin_from_ast()
    if ast_arrayjoin:
        formatted_array_join = ast_arrayjoin.accept(formatter)
        array_join_clause = f"ARRAY JOIN {formatted_array_join}"
    else:
        array_join_clause = ""

    ast_prewhere = query.get_prewhere_ast()
    if ast_prewhere:
        formatted_prewhere = ast_prewhere.accept(formatter)
        prewhere_clause = f"PREWHERE {formatted_prewhere}"
    else:
        prewhere_clause = ""

    ast_condition = query.get_condition_from_ast()
    if ast_condition:
        where_clause = f"WHERE {ast_condition.accept(formatter)}"
    else:
        where_clause = ""

    group_clause = ""
    if ast_groupby:
        # reformat to use aliases generated during the select clause formatting.
        groupby_expressions = [e.accept(formatter) for e in ast_groupby]
        group_clause = f"GROUP BY ({', '.join(groupby_expressions)})"
        if query.has_totals():
            group_clause = f"{group_clause} WITH TOTALS"

    if ast_having:
        having_clause = f"HAVING {ast_having.accept(formatter)}"
    else:
        having_clause = ""

    ast_orderby = query.get_orderby_from_ast()
    if ast_orderby:
        orderby = [
            f"{e.expression.accept(formatter)} {e.direction.value}" for e in ast_orderby
        ]
        order_clause = f"ORDER BY {', '.join(orderby)}"
    else:
        order_clause = ""

    ast_limitby = query.get_limitby()
    if ast_limitby is not None:
        limitby_clause = "LIMIT {} BY {}".format(*ast_limitby)
    else:
        limitby_clause = ""

    ast_limit = query.get_limit()
    if ast_limit is not None:
        limit_clause = f"LIMIT {ast_limit} OFFSET {query.get_offset()}"
    else:
        limit_clause = ""

    return FormattedQuery(
        [
            (clause, value)
            for clause, value in [
                ("select", select_clause),
                ("from", from_clause),
                ("array_join", array_join_clause),
                ("prewhere", prewhere_clause),
                ("where", where_clause),
                ("group", group_clause),
                ("having", having_clause),
                ("order", order_clause),
                ("limitby", limitby_clause),
                ("limit", limit_clause),
            ]
            if value
        ]
    )
