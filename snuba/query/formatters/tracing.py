from typing import Any, List, Mapping, Sequence, Union

from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.multi import MultiQuery
from snuba.query.data_source.simple import SimpleDataSource
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import StringifyVisitor

TExpression = Union[str, Mapping[str, Any], Sequence[Any]]


def _indent_str_list(str_list: List[str], levels: int) -> List[str]:
    indent = "  " * levels
    return [f"{indent}{s}" for s in str_list]


def format_query(
    query: Union[ProcessableQuery[SimpleDataSource], CompositeQuery[SimpleDataSource]]
) -> List[str]:
    """
    Formats a query as a list of strings with each element being a new line

    This representation is meant to be used for tracing/error tracking
    as the query would not be truncated when ingesting the event.
    """

    eformatter = StringifyVisitor(level=0, initial_indent=1)

    selects = ",\n".join(
        [
            f"{e.expression.accept(eformatter)} |> {e.name}"
            for e in query.get_selected_columns()
        ]
    )
    select_str = f"SELECT\n{selects}" if selects else ""

    from_strs = [
        "FROM",
        *_indent_str_list(
            TracingQueryFormatter().visit(query.get_from_clause()), levels=1
        ),
    ]

    groupbys = ",\n".join([e.accept(eformatter) for e in query.get_groupby()])
    groupby_str = f"GROUPBY\n{groupbys}" if groupbys else ""

    orderbys = ",\n".join(
        [
            f"{e.expression.accept(eformatter)} {e.direction.value}"
            for e in query.get_orderby()
        ]
    )
    orderby_str = f"ORDER_BY\n{orderbys}" if orderbys else ""

    str_list = [select_str, *from_strs, groupby_str, orderby_str]

    array_join = query.get_arrayjoin()
    if array_join:
        arrayjoins = ",\n".join([e.accept(eformatter) for e in array_join])
        str_list.append(f"ARRAYJOIN\n{arrayjoins}")
    condition = query.get_condition()
    if condition:
        str_list.append(f"WHERE\n{condition.accept(eformatter)}")
    having = query.get_having()
    if having:
        str_list.append(f"HAVING\n{having.accept(eformatter)}")
    limitby = query.get_limitby()
    if limitby:
        columns_accepted = [column.accept(eformatter) for column in limitby.columns]
        column_string = ",".join(columns_accepted)
        str_list.append(f"LIMIT {limitby.limit} BY {column_string}")
    limit = query.get_limit()
    if limit:
        str_list.append(f"  LIMIT {limit}")
    offset = query.get_offset()
    if offset:
        str_list.append(f"  OFFSET {offset}")
    # The StringifyVisitor formats expression with newlines but we
    # need them as a list of strings (to avoid truncation)
    # therefore as a last step we join our list of strings into one string,
    # and then split it again by newlines to go from this:

    # ['SELECT\n  c1,  ev.c AS `_snuba_c1`\n  f1,  f(\n    ev.c2\n  ) AS `_snuba_f1`',
    #  'FROM',
    #  "  ENTITY(events) AS `ev`",
    # ]

    # To this:

    # ['SELECT',
    #  '  c1,  ev.c AS `_snuba_c1`',
    #  '  f1,  f(',
    #  '    ev.c2',
    #  '  ) AS `_snuba_f1`',
    #  'FROM',
    #  "  ENTITY(events) AS `ev`",
    return "\n".join(str_list).rstrip().split("\n")


class TracingQueryFormatter(
    DataSourceVisitor[List[str], SimpleDataSource],
    JoinVisitor[List[str], SimpleDataSource],
):
    def _indent_str_list(self, str_list: List[str], levels: int) -> List[str]:
        indent = "  " * levels
        return [f"{indent}{s}" for s in str_list]

    def _visit_simple_source(self, data_source: SimpleDataSource) -> List[str]:
        # Entity and Table define their sampling rates with slightly different
        # terms and renaming it would introduce a lot of code changes down the line
        # so we use this dynamic workaround
        sample_val = getattr(
            data_source, "sample", getattr(data_source, "sampling_rate", None)
        )
        sample_str = f" SAMPLE {sample_val}" if sample_val is not None else ""
        return [f"{data_source.human_readable_id}{sample_str}"]

    def _visit_join(self, data_source: JoinClause[SimpleDataSource]) -> List[str]:
        return self.visit_join_clause(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[SimpleDataSource]
    ) -> List[str]:
        return format_query(data_source)

    def _visit_composite_query(
        self, data_source: CompositeQuery[SimpleDataSource]
    ) -> List[str]:
        return format_query(data_source)

    def visit_individual_node(
        self, node: IndividualNode[SimpleDataSource]
    ) -> List[str]:
        return [f"{self.visit(node.data_source)} AS `{node.alias}`"]

    def visit_join_clause(self, node: JoinClause[SimpleDataSource]) -> List[str]:
        # There is only one of these in the on clause (I think)
        on_list = [
            [
                f"{c.left.table_alias}.{c.left.column}",
                f"{c.right.table_alias}.{c.right.column}",
            ]
            for c in node.keys
        ][0]

        return [
            *_indent_str_list(node.left_node.accept(self), 1),
            f"{node.join_type.name.upper()} JOIN",
            *_indent_str_list(node.right_node.accept(self), 1),
            "ON",
            *_indent_str_list(
                on_list,
                1,
            ),
        ]

    def _visit_multi_query(
        self, data_source: MultiQuery[SimpleDataSource]
    ) -> List[str]:
        formatted_queries = []
        for q in data_source.queries:
            formatted_queries.extend(_indent_str_list(format_query(q), 1))
        return formatted_queries
