from typing import Any, List, Mapping, Sequence, Union

from snuba.query import ProcessableQuery
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.join import IndividualNode, JoinClause, JoinVisitor
from snuba.query.data_source.simple import SimpleDataSource
from snuba.query.data_source.visitor import DataSourceVisitor
from snuba.query.expressions import AsCodeVisitor

TExpression = Union[str, Mapping[str, Any], Sequence[Any]]


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
        f"SelectedExpression({repr({e.name})}, {e.expression.accept(eformatter)})"
        for e in query.get_selected_columns()
    )
    select_str = f"[{selects}]"
    from_clause = query.get_from_clause()
    from_str = CodeQueryFormatter().visit(from_clause)

    groupbys = ",\n".join([e.accept(eformatter) for e in query.get_groupby()])
    groupby_str = f"[{groupbys}]"

    orderbys = ",\n".join(
        [
            f"""OrderBy(
    direction=OrderByDirection.{e.direction.name},
    expression={e.expression.accept(eformatter)}
)
"""
            for e in query.get_orderby()
        ]
    )
    order_by_str = f"[{orderbys}]"
    array_join = query.get_arrayjoin()
    array_join_str = f"[{array_join.accept(eformatter)}]" if array_join else None

    condition = query.get_condition()
    condition_str = condition.accept(eformatter) if condition else None

    having = query.get_having()
    having_str = having.accept(eformatter) if having else None

    limitby = query.get_limitby()
    limitby_str = (
        f"""LimitBy(
    limit={limitby.limit},
    expression={limitby.expression.accept(eformatter)}
)
    """
        if limitby
        else None
    )

    res = f"""{query.__class__.__name__}(
        from_clause={from_str},
        selected_columns={select_str},
        array_join={array_join_str},
        condition={condition_str},
        groupby={groupby_str},
        having={having_str},
        order_by={order_by_str},
        limitby={limitby_str},
        limit={repr(query.get_limit())},
        offset={repr(query.get_offset())},
        totals={repr(query.has_totals())},
        granularity={repr(query.get_granularity())},
        experiments={repr(query.get_experiments())},
)
    """
    # HACK (Vlad): this code will only work in unit tests (but that's the only place
    # we need it. This saves a lot of time trying to format stuff.
    import black

    return str(black.format_str(res, mode=black.FileMode()))


class CodeQueryFormatter(
    DataSourceVisitor[str, SimpleDataSource], JoinVisitor[str, SimpleDataSource],
):
    def _indent_str_list(self, str_list: List[str], levels: int) -> List[str]:
        indent = "  " * levels
        return [f"{indent}{s}" for s in str_list]

    def _visit_simple_source(self, data_source: SimpleDataSource) -> str:
        return data_source.human_readable_id

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

    def visit_individual_node(self, node: IndividualNode[SimpleDataSource]) -> str:
        return f"""IndividualNode({repr(node.alias)}, {self.visit(node.data_source)})"""

    def visit_join_clause(self, node: JoinClause[SimpleDataSource]) -> str:
        # There is only one of these in the on clause (I think)
        join_conditions = ", ".join([repr(jc) for jc in node.keys])
        return f"""JoinClause(
    left_node={node.left_node.accept(self)},
    right_node={node.right_node.accept(self)},
    keys=({join_conditions}),
    join_type=JoinType.{node.join_type.name},
    join_modifier=JoinModifier.{node.join_modifier.name if node.join_modifier else 'None'}
)
"""
