from abc import ABC
from dataclasses import dataclass, replace
from typing import Any, Mapping, Optional, Sequence, Tuple, Union

from snuba import settings as snuba_settings
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Table
from snuba.query.parsing import ParsingContext
from snuba.request.request_settings import RequestSettings


class FormattedNode(ABC):
    """
    After formatting all clauses of a query, we may serialize the
    query itself as a string or exporting it as a dictionary for
    tracing and debugging.
    This data structure is a node in a tree of formatted expressions
    that can be serialized as a string or as a dictionary.
    """

    def __str__(self) -> str:
        raise NotImplementedError

    def for_mapping(self) -> Union[str, Mapping[str, Any]]:
        raise NotImplementedError


@dataclass(frozen=True)
class StringNode(FormattedNode):
    value: str

    def __str__(self) -> str:
        return self.value

    def for_mapping(self) -> str:
        return self.value


@dataclass(frozen=True)
class OrderedNestedNode(FormattedNode):
    content: Sequence[Tuple[str, FormattedNode]]

    def __str__(self) -> str:
        return " ".join([str(c) for _, c in self.content])

    def for_mapping(self) -> Mapping[str, Any]:
        return dict(((k, v.for_mapping()) for k, v in self.content))


@dataclass(frozen=True)
class FormattedSubQuery(OrderedNestedNode):
    def __str__(self) -> str:
        return f"({super().__str__()})"


@dataclass(frozen=True)
class FormattedQuery(OrderedNestedNode):
    """
    Used to move around a query data structure after all clauses have
    been formatted but such that it can be still serialized
    differently for different usages (running the query or tracing).
    """

    def get_sql(self, format: Optional[str] = None) -> str:
        query = str(self)
        if format is not None:
            query = f"{query} FORMAT {format}"

        return query


FormattableQuery = Union[Query, CompositeQuery[Table]]


def format_query(query: Query, settings: RequestSettings) -> FormattedQuery:
    """
    Replaces the AstSqlQuery abstraction.

    TODO: Remove this method entirely and move the sampling logic
    into a query processor.
    """

    if settings.get_turbo() and not query.get_from_clause().sampling_rate:
        query.set_from_clause(
            replace(
                query.get_from_clause(), sampling_rate=snuba_settings.TURBO_SAMPLE_RATE
            )
        )

    return _format_query_impl(query)


def _format_query_impl(query: FormattableQuery) -> FormattedQuery:
    """
    Formats a Query from the AST representation into an intermediate
    structure that can either be serialized into a string (for clickhouse)
    or extracted as a dictionary (for logging and tracing).

    This is the entry point for any type of query, whether simple or
    composite.

    Remark: The ideal way to structure this would be a visitor just
    like we did for the AST expression but this is not possible
    without introducing either circular dependencies or dropping
    type checking since the visitor and the classes to visit cannot
    be in the same module as of now and in a visitor pattern they would
    have to be mutually dependent.
    """

    if isinstance(query, Query):
        return format_processable_query(query)
    else:
        # TODO: Support composite queries
        raise NotImplementedError("Query type not yet supported")


def format_processable_query(query: Query) -> FormattedQuery:
    parsing_context = ParsingContext()
    formatter = ClickhouseExpressionFormatter(parsing_context)

    selected_cols = [
        e.expression.accept(formatter) for e in query.get_selected_columns_from_ast()
    ]
    select_clause = f"SELECT {', '.join(selected_cols)}"

    query_from_clause = query.get_from_clause()
    from_clause = f"FROM {query_from_clause.table_name}"

    if query_from_clause.final:
        from_clause = f"{from_clause} FINAL"

    ast_sample = query_from_clause.sampling_rate
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
    ast_groupby = query.get_groupby_from_ast()
    if ast_groupby:
        # reformat to use aliases generated during the select clause formatting.
        groupby_expressions = [e.accept(formatter) for e in ast_groupby]
        group_clause = f"GROUP BY ({', '.join(groupby_expressions)})"
        if query.has_totals():
            group_clause = f"{group_clause} WITH TOTALS"

    ast_having = query.get_having_from_ast()
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
                ("select", StringNode(select_clause) if select_clause else None),
                ("from", StringNode(from_clause) if from_clause else None),
                (
                    "array_join",
                    StringNode(array_join_clause) if array_join_clause else None,
                ),
                ("prewhere", StringNode(prewhere_clause) if prewhere_clause else None),
                ("where", StringNode(where_clause) if where_clause else None),
                ("group", StringNode(group_clause) if group_clause else None),
                ("having", StringNode(having_clause) if having_clause else None),
                ("order", StringNode(order_clause) if order_clause else None),
                ("limitby", StringNode(limitby_clause) if limitby_clause else None),
                ("limit", StringNode(limit_clause) if limit_clause else None),
            ]
            if value
        ]
    )
