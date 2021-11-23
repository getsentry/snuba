from typing import Any, List, Mapping, Sequence, Union

from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import SimpleDataSource
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import AsCodeVisitor

TExpression = Union[str, Mapping[str, Any], Sequence[Any]]


def _indent_str_list(str_list: List[str], levels: int) -> List[str]:
    indent = "  " * levels
    return [f"{indent}{s}" for s in str_list]


def format_query(
    query: Union[ProcessableQuery[SimpleDataSource], CompositeQuery[SimpleDataSource]]
) -> str:
    """
    Formats a query as a list of strings with each element being a new line

    This representation is meant to be used for tracing/error tracking
    as the query would not be truncated when ingesting the event.
    """

    eformatter = AsCodeVisitor(level=0, initial_indent=1)


    selects = ",\n".join(
        f"SelectedExpression(repr({e.name}), {e.expression.accept(eformatter)}"
        for e in query.get_selected_columns()
    )
    select_str = f"({selects})"
    from_str = query.get_from_clause().accept(eformatter)

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
        str_list.append(f"ARRAYJOIN\n{array_join.accept(eformatter)}")
    condition = query.get_condition()
    if condition:
        str_list.append(f"WHERE\n{condition.accept(eformatter)}")
    having = query.get_having()
    if having:
        str_list.append(f"HAVING\n{having.accept(eformatter)}")
    limitby = query.get_limitby()
    if limitby:
        str_list.append(
            f"LIMIT {limitby.limit} BY {limitby.expression.accept(eformatter)}"
        )
    limit = query.get_limit()
    if limit:
        str_list.append(f"  LIMIT {limit}")
    offset = query.get_offset()
    if offset:
        str_list.append(f"  OFFSET {offset}")

    return f"""{query.__class__.__name__}(
        selected_columns={selected_columns_str},
        array_join={array_join_str},
        condition={condition_str},
        groupby={groupby_str},
        having={having_str},
        order_by={order_by_str},
        limitby={limitby_str},
        limit={limit_str},
        offset={offset_str},
        totals={totals_str},
        granularity={granularity_str},
        experiments={experiments_str},
    """

class TracingQueryFormatter(
    DataSourceVisitor[str, SimpleDataSource],
    JoinVisitor[str, SimpleDataSource],
):
    def _indent_str_list(self, str_list: List[str], levels: int) -> List[str]:
        indent = "  " * levels
        return [f"{indent}{s}" for s in str_list]

    def _visit_simple_source(self, data_source: SimpleDataSource) -> str:
        return repr(data_source)

    def _visit_join(self, data_source: JoinClause[SimpleDataSource]) -> str:
        return repr(data_source)

    def _visit_simple_query(
        self, data_source: ProcessableQuery[SimpleDataSource]
    ) -> str:
        return format_query(data_source)

    def _visit_composite_query(
        self, data_source: CompositeQuery[SimpleDataSource]
    ) -> str:
        return format_query(data_source)

    def visit_individual_node(
        self, node: IndividualNode[SimpleDataSource]
    ) -> str:
        return f"""IndividualNode({repr(node.alias)}, {self.visit(node.data_source)})"""

    def visit_join_clause(self, node: JoinClause[SimpleDataSource]) -> str]:
        # There is only one of these in the on clause (I think)
        return f"""JoinClause(
            left_node={node.left_node.accept(self)},
            right_node={node.right_node.accept(self)},
            keys=# TODO
        """


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
            *_indent_str_list(on_list, 1,),
        ]
