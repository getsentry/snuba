from typing import (
    Any,
    Callable,
    Iterable,
    MutableMapping,
    NamedTuple,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query import (
    Limitby,
    OrderBy,
    OrderByDirection,
    Query as AbstractQuery,
    SelectedExpression,
)
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    binary_condition,
    combine_and_conditions,
    combine_or_conditions,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.data_source.join import (
    IndividualNode,
    JoinClause,
    JoinCondition,
    JoinConditionExpression,
    JoinType,
)
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)

from snuba.query.logical import Query as LogicalQuery
from snuba.query.matchers import (
    Any as AnyMatch,
    FunctionCall as FunctionCallMatch,
    Literal as LiteralMatch,
    Param,
    String as StringMatch,
)
from snuba.query.parser import (
    _validate_aliases,
    _parse_subscriptables,
    _apply_column_aliases,
    _expand_aliases,
    _deescape_aliases,
)
from snuba.query.snql.expression_visitor import (
    HighPriArithmetic,
    HighPriOperator,
    HighPriTuple,
    LowPriArithmetic,
    LowPriOperator,
    LowPriTuple,
    generic_visit,
    visit_arithmetic_term,
    visit_boolean_literal,
    visit_column_name,
    visit_function_name,
    visit_high_pri_arithmetic,
    visit_high_pri_op,
    visit_high_pri_tuple,
    visit_integer_literal,
    visit_low_pri_arithmetic,
    visit_low_pri_op,
    visit_low_pri_tuple,
    visit_numeric_literal,
    visit_parameter,
    visit_parameters_list,
    visit_quoted_literal,
)
from snuba.util import parse_datetime


snql_grammar = Grammar(
    r"""
    query_exp             = match_clause select_clause group_by_clause? where_clause? having_clause? order_by_clause? limit_by_clause? limit_clause? offset_clause? granularity_clause? totals_clause?

    match_clause          = space* "MATCH" space+ (relationships / entity_match / subquery) space*
    select_clause         = space* "SELECT" space+ select_list space*
    group_by_clause       = space* "BY" space+ group_list space*
    where_clause          = space* "WHERE" space+ or_expression space*
    having_clause         = space* "HAVING" space+ or_expression space*
    order_by_clause       = space* "ORDER BY" space+ order_list space*
    limit_by_clause       = space* "LIMIT" space+ integer_literal space* "BY" space* column_name space*
    limit_clause          = space* "LIMIT" space+ integer_literal space*
    offset_clause         = space* "OFFSET" space+ integer_literal space*
    granularity_clause    = space* "GRANULARITY" space+ integer_literal space*
    totals_clause         = space* "TOTALS" space+ boolean_literal space*

    sample_clause         = space* "SAMPLE" space* numeric_literal space*
    entity_match          = open_paren entity_alias colon space* entity_name sample_clause? close_paren
    relationship_link     = ~r"-\[" relationship_name ~r"\]->"
    relationship_match    = space* entity_match space* relationship_link space* entity_match space*
    relationships         = relationship_match (comma relationship_match)*
    subquery              = open_brace query_exp close_brace

    main_condition        = low_pri_arithmetic condition_op (function_call / column_name / quoted_literal / numeric_literal) space*
    condition             = main_condition / parenthesized_cdn
    condition_op          = "=" / "!=" / ">" / ">=" / "<" / "<="
    parenthesized_cdn     = space* open_paren or_expression close_paren space*

    and_expression        = space* condition space* (and_tuple)*
    or_expression         = space* and_expression space* (or_tuple)*
    and_tuple             = "AND" condition
    or_tuple              = "OR" and_expression

    select_list           = select_columns* (selected_expression)
    select_columns        = selected_expression space* comma space*
    selected_expression   = low_pri_arithmetic space*

    group_list            = group_columns* (low_pri_arithmetic)
    group_columns         = low_pri_arithmetic space* comma space*
    order_list            = order_columns* low_pri_arithmetic ("ASC"/"DESC")
    order_columns         = low_pri_arithmetic ("ASC"/"DESC") space* comma space*

    clause                = space* ~r"[-=><\w]+" space*

    low_pri_arithmetic    = space* high_pri_arithmetic space* (low_pri_tuple)*
    high_pri_arithmetic   = space* arithmetic_term space* (high_pri_tuple)*
    low_pri_tuple         = low_pri_op high_pri_arithmetic
    high_pri_tuple        = high_pri_op arithmetic_term

    arithmetic_term       = space* (function_call / numeric_literal / column_name / parenthesized_arithm) space*
    parenthesized_arithm  = open_paren low_pri_arithmetic close_paren

    low_pri_op            = "+" / "-"
    high_pri_op           = "/" / "*"
    param_expression      = low_pri_arithmetic / quoted_literal
    parameters_list       = parameter* (param_expression)
    parameter             = param_expression space* comma space*
    function_call         = function_name open_paren parameters_list? close_paren (open_paren parameters_list? close_paren)? (space* "AS" space* string_literal)?
    simple_term           = quoted_literal / numeric_literal / column_name
    literal               = ~r"[a-zA-Z0-9_\.:-]+"
    quoted_literal        = "'" string_literal "'"
    string_literal        = ~r"[a-zA-Z0-9_\.\+\*\/:-]*"
    numeric_literal       = ~r"-?[0-9]+(\.[0-9]+)?(e[\+\-][0-9]+)?"
    integer_literal       = ~r"-?[0-9]+"
    boolean_literal       = true_literal / false_literal
    true_literal          = ~r"TRUE"i
    false_literal         = ~r"FALSE"i
    column_name           = ~r"[a-zA-Z_][a-zA-Z0-9_\.]*"
    function_name         = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_alias          = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_name           = ~r"[A-Z][a-zA-Z]*"
    relationship_name     = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    open_paren            = "("
    close_paren           = ")"
    open_brace            = "{"
    close_brace           = "}"
    single_quote          = "'"
    colon                 = ":"
    space                 = ~r" |\n|\t"
    comma                 = ","

"""
)


class AndTuple(NamedTuple):
    op: str
    exp: Expression


class OrTuple(NamedTuple):
    op: str
    exp: Expression


class EntityTuple(NamedTuple):
    alias: str
    name: str
    sample_rate: Optional[float]


class RelationshipTuple(NamedTuple):
    lhs: EntityTuple
    relationship: str
    rhs: EntityTuple


class JoinGraph(NamedTuple):
    vertices: Set[EntityTuple]
    edges: Set[RelationshipTuple]

    def find_edges(self, vertex: EntityTuple) -> Sequence[RelationshipTuple]:
        return [e for e in self.edges if e.lhs == vertex]

    def find_root(self) -> EntityTuple:
        incoming = {v: 0 for v in self.vertices}
        for relationship in self.edges:
            incoming[relationship.rhs] += 1

        roots = set()
        for v, count in incoming.items():
            if count == 0:
                roots.add(v)

        if len(roots) > 1:
            raise Exception("graph is not connected")
        elif len(roots) < 1:
            raise Exception("why would you add a cycle")

        return roots.pop()


class ListNode(NamedTuple):
    entity: EntityTuple
    join_conditions: Optional[Sequence[JoinCondition]]


def build_graph(relationships: Sequence[RelationshipTuple]) -> JoinGraph:
    vertices = set()
    edges = set()
    for relationship in relationships:
        vertices.add(relationship.lhs)
        vertices.add(relationship.rhs)
        edges.add(relationship)

    return JoinGraph(vertices, edges)


def _flatten(
    root: EntityTuple, graph: JoinGraph, nodelist: List[ListNode]
) -> List[ListNode]:
    edges = graph.find_edges(root)
    if len(edges) == 0:
        return nodelist

    for edge in edges:
        join_keys = get_entity(EntityKey(root.name)).get_join_conditions(
            edge.relationship
        )
        join_conditions = []
        for join_key in join_keys:
            join_conditions.append(
                JoinCondition(
                    left=JoinConditionExpression(edge.lhs.alias, join_key[0]),
                    right=JoinConditionExpression(edge.rhs.alias, join_key[1]),
                )
            )
        nodelist.append(ListNode(edge.rhs, join_conditions,))

    for edge in edges:
        _flatten(edge.rhs, graph, nodelist)

    return nodelist


def build_join_clause(nodes: List[ListNode]) -> JoinClause[QueryEntity]:
    if len(nodes) < 2:
        raise Exception("something went wrong")

    node = nodes.pop()
    assert node.join_conditions is not None, "no conditions???"
    rhs = IndividualNode(
        node.entity.alias,
        QueryEntity(
            EntityKey(node.entity.name),
            get_entity(EntityKey(node.entity.name)).get_data_model(),
            node.entity.sample_rate,
        ),
    )
    lhs: Optional[Union[JoinClause[QueryEntity], IndividualNode[QueryEntity]]] = None
    if len(nodes) == 1:
        lhs_node = nodes.pop()
        lhs = IndividualNode(
            lhs_node.entity.alias,
            QueryEntity(
                EntityKey(lhs_node.entity.name),
                get_entity(EntityKey(lhs_node.entity.name)).get_data_model(),
                lhs_node.entity.sample_rate,
            ),
        )
    else:
        lhs = build_join_clause(nodes)

    return JoinClause(
        left_node=lhs,
        right_node=rhs,
        keys=node.join_conditions,
        join_type=JoinType.INNER,
    )


def graph_to_join_clause(graph: JoinGraph) -> JoinClause[QueryEntity]:
    root = graph.find_root()
    nodelist = _flatten(root, graph, [ListNode(root, None)])
    join_clause = build_join_clause(nodelist)

    return join_clause


class SnQLVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_query_exp(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Union[LogicalQuery, CompositeQuery[QueryEntity]]:
        args: MutableMapping[str, Any] = {
            "array_join": None,
        }
        (
            data_source,
            args["selected_columns"],
            args["groupby"],
            args["condition"],
            args["having"],
            args["order_by"],
            args["limitby"],
            args["limit"],
            offset,
            args["granularity"],
            args["totals"],
        ) = visited_children
        if not isinstance(offset, Node):
            args["offset"] = offset

        for k, v in args.items():
            if isinstance(v, Node):
                args[k] = None

        # Do some magic based on what the data source for this query is.
        if isinstance(data_source, (CompositeQuery, LogicalQuery, JoinClause)):
            args["from_clause"] = data_source
            return CompositeQuery(**args)

        args.update({"body": {}, "prewhere": None, "data_source": data_source})
        if isinstance(data_source, QueryEntity):
            # TODO: How sample rate gets stored needs to be addressed in a future PR
            args["sample"] = data_source.sample_rate

        return LogicalQuery(**args)

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return visit_function_name(node, visited_children)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return visit_column_name(node, visited_children)

    def visit_and_tuple(
        self, node: Node, visited_children: Tuple[Node, Expression]
    ) -> AndTuple:
        and_string, exp = visited_children
        return AndTuple(and_string.text, exp)

    def visit_or_tuple(
        self, node: Node, visited_children: Tuple[Node, Expression]
    ) -> OrTuple:
        or_string, exp = visited_children
        return OrTuple(or_string.text, exp)

    def visit_parenthesized_cdn(
        self, node: Node, visited_children: Tuple[Any, Any, Expression, Any, Any]
    ) -> Expression:
        _, _, condition, _, _ = visited_children
        return condition

    def visit_parenthesized_arithm(
        self, node: Node, visited_children: Tuple[Any, Expression, Any]
    ) -> Expression:
        _, arithm, _ = visited_children
        return arithm

    def visit_low_pri_tuple(
        self, node: Node, visited_children: Tuple[LowPriOperator, Expression]
    ) -> LowPriTuple:
        return visit_low_pri_tuple(node, visited_children)

    def visit_high_pri_tuple(
        self, node: Node, visited_children: Tuple[HighPriOperator, Expression]
    ) -> HighPriTuple:
        return visit_high_pri_tuple(node, visited_children)

    def visit_low_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> LowPriOperator:
        return visit_low_pri_op(node, visited_children)

    def visit_high_pri_op(
        self, node: Node, visited_children: Iterable[Any]
    ) -> HighPriOperator:
        return visit_high_pri_op(node, visited_children)

    def visit_arithmetic_term(
        self, node: Node, visited_children: Tuple[Any, Expression, Any]
    ) -> Expression:
        return visit_arithmetic_term(node, visited_children)

    def visit_low_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, LowPriArithmetic],
    ) -> Expression:
        return visit_low_pri_arithmetic(node, visited_children)

    def visit_high_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Any, HighPriArithmetic],
    ) -> Expression:
        return visit_high_pri_arithmetic(node, visited_children)

    def visit_numeric_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return visit_numeric_literal(node, visited_children)

    def visit_integer_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return visit_integer_literal(node, visited_children)

    def visit_boolean_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return visit_boolean_literal(node, visited_children)

    def visit_quoted_literal(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Literal:
        return visit_quoted_literal(node, visited_children)

    def __relationship_to_join_clause(
        self, relationship: RelationshipTuple
    ) -> JoinClause[QueryEntity]:
        lhs_entity = get_entity(EntityKey(relationship.lhs.name))
        lhs_query_entity = QueryEntity(
            EntityKey(relationship.lhs.name),
            lhs_entity.get_data_model(),
            relationship.lhs.sample_rate,
        )
        rhs_query_entity = QueryEntity(
            EntityKey(relationship.rhs.name),
            get_entity(EntityKey(relationship.rhs.name)).get_data_model(),
            relationship.rhs.sample_rate,
        )

        # This part of the process is a WIP. It will change once the relationships
        # are stored in the data model.
        join_keys = lhs_entity.get_join_conditions(relationship.relationship)
        join_conditions = []
        for join_key in join_keys:
            join_conditions.append(
                JoinCondition(
                    left=JoinConditionExpression(relationship.lhs.alias, join_key[0]),
                    right=JoinConditionExpression(relationship.rhs.alias, join_key[1]),
                )
            )

        join = JoinClause(
            left_node=IndividualNode(relationship.lhs.alias, lhs_query_entity),
            right_node=IndividualNode(relationship.rhs.alias, rhs_query_entity),
            keys=join_conditions,
            join_type=JoinType.INNER,
        )
        return join

    def visit_match_clause(
        self,
        node: Node,
        visited_children: Tuple[
            Any, Any, Any, Union[EntityTuple, Sequence[RelationshipTuple]], Any
        ],
    ) -> Union[
        Optional[CompositeQuery[QueryEntity]],
        Optional[LogicalQuery],
        Optional[QueryEntity],
        Optional[JoinClause[QueryEntity]],
    ]:
        _, _, _, match, _ = visited_children
        if isinstance(match, EntityTuple):
            key = EntityKey(match.name)
            query_entity = QueryEntity(
                key, get_entity(key).get_data_model(), match.sample_rate
            )
            return query_entity
        elif isinstance(match, (CompositeQuery, LogicalQuery)):
            return match
        elif isinstance(match, RelationshipTuple):
            return self.__relationship_to_join_clause(match)
        if isinstance(match, list) and all(
            isinstance(m, RelationshipTuple) for m in match
        ):
            graph = build_graph(match)
            join_clause = graph_to_join_clause(graph)
            return join_clause

        return None

    def visit_relationships(
        self, node: Node, visited_children: Tuple[RelationshipTuple, Any],
    ) -> Sequence[RelationshipTuple]:
        relationships = [visited_children[0]]
        if isinstance(visited_children[1], Node):
            return relationships

        for child in visited_children[1]:
            if isinstance(child, RelationshipTuple):
                relationships.append(child)
            elif isinstance(child, list):
                relationships.append(child[1])

        return relationships

    def visit_relationship_match(
        self,
        node: Node,
        visited_children: Tuple[Any, EntityTuple, Any, Node, Any, EntityTuple, Any],
    ) -> RelationshipTuple:
        _, lhs, _, relationship, _, rhs, _ = visited_children
        return RelationshipTuple(lhs, relationship, rhs)

    def visit_relationship_link(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> str:
        _, relationship, _ = visited_children
        return str(relationship.text)

    def visit_entity_match(
        self,
        node: Node,
        visited_children: Tuple[Any, str, Any, Any, str, Optional[float], Any],
    ) -> EntityTuple:
        _, alias, _, _, name, sample, _ = visited_children
        if isinstance(sample, Node):
            sample = None
        return EntityTuple(alias, name.lower(), sample)

    def visit_entity_alias(self, node: Node, visited_children: Tuple[Any]) -> str:
        return str(node.text)

    def visit_entity_name(self, node: Node, visited_children: Tuple[Any]) -> str:
        return str(node.text)

    def visit_subquery(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Union[LogicalQuery, CompositeQuery[QueryEntity]]:
        _, query, _ = visited_children
        assert isinstance(query, (CompositeQuery, LogicalQuery))  # mypy
        return query

    def visit_where_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Expression, Any]
    ) -> Expression:
        _, _, _, conditions, _ = visited_children
        return conditions

    def visit_having_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Expression, Any]
    ) -> Expression:
        _, _, conditions, _ = visited_children
        return conditions

    def visit_and_expression(
        self, node: Node, visited_children: Tuple[Any, Expression, Any, Expression]
    ) -> Expression:
        _, left_condition, _, and_condition = visited_children
        args = [left_condition]
        # in the case of one Condition
        # and_condition will be an empty Node
        if isinstance(and_condition, Node):
            return left_condition
        if isinstance(and_condition, (AndTuple, OrTuple)):
            _, exp = and_condition
            return combine_and_conditions([left_condition, exp])
        elif isinstance(and_condition, list):
            for elem in and_condition:
                _, exp = elem
                args.append(exp)
        return combine_and_conditions(args)

    def visit_or_expression(
        self, node: Node, visited_children: Tuple[Any, Expression, Any, Expression]
    ) -> Expression:
        _, left_condition, _, or_condition = visited_children
        args = [left_condition]
        # in the case of one Condition
        # or_condition will be an empty Node
        if isinstance(or_condition, Node):
            return left_condition
        if isinstance(or_condition, (AndTuple, OrTuple)):
            _, exp = or_condition
            return combine_or_conditions([left_condition, exp])
        elif isinstance(or_condition, list):
            for elem in or_condition:
                _, exp = elem
                args.append(exp)
        return combine_or_conditions(args)

    def visit_main_condition(
        self, node: Node, visited_children: Tuple[Expression, str, Expression, Any]
    ) -> Expression:
        exp, op, literal, _ = visited_children
        return binary_condition(None, op, exp, literal)

    def visit_condition_op(self, node: Node, visited_children: Iterable[Any]) -> str:
        return OPERATOR_TO_FUNCTION[node.text]

    def visit_order_by_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Sequence[OrderBy], Any]
    ) -> Sequence[OrderBy]:
        _, _, _, order_columns, _ = visited_children
        return order_columns

    def visit_order_list(
        self, node: Node, visited_children: Tuple[OrderBy, Expression, Node]
    ) -> Sequence[OrderBy]:
        left_order_list, right_order, order = visited_children
        ret: List[OrderBy] = []

        # in the case of one OrderBy
        # left_order_list will be an empty node
        if not isinstance(left_order_list, Node):
            if not isinstance(left_order_list, (list, tuple)):
                ret.append(left_order_list)
            else:
                for p in left_order_list:
                    ret.append(p)

        direction = (
            OrderByDirection.ASC if order.text == "ASC" else OrderByDirection.DESC
        )
        ret.append(OrderBy(direction, right_order))

        return ret

    def visit_order_columns(
        self, node: Node, visited_children: Tuple[Expression, Node, Any, Any, Any]
    ) -> OrderBy:
        column, order, _, _, _ = visited_children

        direction = (
            OrderByDirection.ASC if order.text == "ASC" else OrderByDirection.DESC
        )
        return OrderBy(direction, column)

    def visit_sample_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal, Any]
    ) -> float:
        _, _, _, sample, _ = visited_children
        assert isinstance(sample.value, float)  # mypy
        return sample.value

    def visit_granularity_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal, Any]
    ) -> float:
        _, _, _, granularity, _ = visited_children
        assert isinstance(granularity.value, int)  # mypy
        return granularity.value

    def visit_totals_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal, Any]
    ) -> float:
        _, _, _, totals, _ = visited_children
        assert isinstance(totals.value, bool)  # mypy
        return totals.value

    def visit_limit_by_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Literal, Any, Any, Any, Column, Any],
    ) -> Limitby:
        _, _, _, limit, _, _, _, column, _ = visited_children
        assert isinstance(limit.value, int)  # mypy
        return (limit.value, column.column_name)

    def visit_limit_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal, Any]
    ) -> int:
        _, _, _, limit, _ = visited_children
        assert isinstance(limit.value, int)  # mypy
        return limit.value

    def visit_offset_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal, Any]
    ) -> int:
        _, _, _, offset, _ = visited_children
        assert isinstance(offset.value, int)  # mypy
        return offset.value

    def visit_group_by_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Sequence[Expression], Any],
    ) -> Sequence[Expression]:
        _, _, _, group_columns, _ = visited_children
        return group_columns

    def visit_group_columns(
        self, node: Node, visited_children: Tuple[Expression, Any, Any, Any]
    ) -> Expression:
        columns, _, _, _ = visited_children
        return columns

    def visit_group_list(
        self, node: Node, visited_children: Tuple[Expression, Expression]
    ) -> Sequence[Expression]:
        left_group_list, right_group = visited_children
        ret: List[Expression] = []

        # in the case of one GroupBy / By
        # left_group_list will be an empty node
        if not isinstance(left_group_list, Node):
            if not isinstance(left_group_list, (list, tuple)):
                ret.append(left_group_list)
            else:
                for p in left_group_list:
                    ret.append(p)

        ret.append(right_group)
        return ret

    def visit_select_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Sequence[SelectedExpression], Any],
    ) -> Sequence[SelectedExpression]:
        _, _, _, selected_columns, _ = visited_children
        return selected_columns

    def visit_selected_expression(
        self, node: Node, visited_children: Tuple[Expression, Any]
    ) -> SelectedExpression:
        exp, _ = visited_children
        return SelectedExpression(node.text.strip(), exp)

    def visit_select_columns(
        self, node: Node, visited_children: Tuple[SelectedExpression, Any, Any, Any]
    ) -> SelectedExpression:
        columns, _, _, _, = visited_children
        return columns

    def visit_select_list(
        self,
        node: Node,
        visited_children: Tuple[SelectedExpression, SelectedExpression],
    ) -> Sequence[SelectedExpression]:
        column_list, right_column = visited_children
        ret: List[SelectedExpression] = []

        # in the case of one Collect
        # column_list will be an empty node
        if not isinstance(column_list, Node):
            if not isinstance(column_list, (list, tuple)):
                ret.append(column_list)
            else:
                for p in column_list:
                    ret.append(p)

        ret.append(right_column)
        return ret

    def visit_parameter(
        self, node: Node, visited_children: Tuple[Expression, Any, Any, Any]
    ) -> Expression:
        return visit_parameter(node, visited_children)

    def visit_parameters_list(
        self,
        node: Node,
        visited_children: Tuple[Union[Expression, List[Expression]], Expression],
    ) -> List[Expression]:
        return visit_parameters_list(node, visited_children)

    def visit_function_call(
        self,
        node: Node,
        visited_children: Tuple[
            str,
            Any,
            List[Expression],
            Any,
            Union[Node, List[Expression]],
            Union[Node, List[Any]],
        ],
    ) -> Expression:
        name, _, params1, _, params2, alias = visited_children
        if isinstance(alias, Node) or len(alias) == 0:
            alias = None
        else:
            _, _, _, alias = alias
            alias = alias.text

        param_list1 = tuple(params1)
        if isinstance(params2, Node) and params2.text == "":
            # params2.text == "" means empty node.
            return FunctionCall(alias, name, param_list1)

        internal_f = FunctionCall(None, name, param_list1)
        _, param_list2, _ = params2
        if isinstance(param_list2, (list, tuple)) and len(param_list2) > 0:
            param_list2 = tuple(param_list2)
        else:
            # This happens when the second parameter list is empty. Somehow
            # it does not turn into an empty list.
            param_list2 = ()
        return CurriedFunctionCall(alias, internal_f, param_list2)

    def generic_visit(self, node: Node, visited_children: Any) -> Any:
        return generic_visit(node, visited_children)


def parse_snql_query_initial(
    body: str,
) -> Union[CompositeQuery[QueryEntity], LogicalQuery]:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """
    exp_tree = snql_grammar.parse(body)
    query = SnQLVisitor().visit(exp_tree)
    assert isinstance(query, (CompositeQuery, LogicalQuery))  # mypy
    return query


DATETIME_MATCH = FunctionCallMatch(
    StringMatch("toDateTime"), (Param("date_string", LiteralMatch(AnyMatch(str))),)
)


def query_transform(
    query: AbstractQuery, transformer: Callable[[AbstractQuery], None],
) -> None:
    transformer(query)
    if isinstance(query, CompositeQuery):
        sub_query = query.get_from_clause()
        if isinstance(sub_query, AbstractQuery):
            query_transform(sub_query, transformer)


def _parse_datetime_literals(query: AbstractQuery) -> None:
    def parse(exp: Expression) -> Expression:
        result = DATETIME_MATCH.match(exp)
        if result is not None:
            date_string = result.expression("date_string")
            assert isinstance(date_string, Literal)  # mypy
            assert isinstance(date_string.value, str)  # mypy
            return Literal(exp.alias, parse_datetime(date_string.value))

        return exp

    query.transform_expressions(parse)


def parse_snql_query(
    body: str, dataset: Dataset
) -> Union[CompositeQuery[QueryEntity], LogicalQuery]:
    query = parse_snql_query_initial(body)

    # These are the post processing phases
    query_transform(query, _parse_datetime_literals)

    # TODO: Update these functions to work with snql queries in a separate PR
    _validate_aliases(query)  # -> needs to recurse through sub queries
    _parse_subscriptables(query)  # -> This should be part of the grammar
    _apply_column_aliases(query)  # -> needs to recurse through sub queries
    _expand_aliases(query)  # -> needs to recurse through sub queries
    _deescape_aliases(
        query
    )  # -> This should not be needed at all, assuming SnQL can properly accept escaped/unicode strings
    return query
