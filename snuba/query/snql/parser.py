from typing import (
    Any,
    Iterable,
    NamedTuple,
    List,
    Sequence,
    Tuple,
    Union,
)

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    binary_condition,
    combine_and_conditions,
    combine_or_conditions,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.logical import Query
from snuba.query.parser import (
    _apply_column_aliases,
    _deescape_aliases,
    _expand_aliases,
    _mangle_aliases,
    _parse_subscriptables,
    _validate_aliases,
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
    visit_column_name,
    visit_function_name,
    visit_high_pri_arithmetic,
    visit_high_pri_op,
    visit_high_pri_tuple,
    visit_low_pri_arithmetic,
    visit_low_pri_op,
    visit_low_pri_tuple,
    visit_numeric_literal,
    visit_parameter,
    visit_parameters_list,
    visit_quoted_literal,
)

snql_grammar = Grammar(
    r"""
    query_exp             = match_clause where_clause? collect_clause? group_by_clause? having_clause? order_by_clause? limit_clause?

    match_clause          = space* "MATCH" space* entity_match space*
    where_clause          = space* "WHERE" or_expression space*
    collect_clause        = space* "COLLECT" collect_list space*
    group_by_clause       = space* "BY" group_list space*
    having_clause         = space* "HAVING" or_expression space*
    order_by_clause       = space* "ORDER BY" order_list space*
    limit_clause          = space* "LIMIT" space* integer_literal space*

    entity_match          = open_paren entity_alias colon space* entity_name close_paren

    main_condition        = low_pri_arithmetic space* condition_op space* (function_call / column_name / quoted_literal / numeric_literal) space*
    condition             = main_condition / parenthesized_cdn
    condition_op          = "=" / "!=" / ">" / ">=" / "<" / "<=" / "IN"
    parenthesized_cdn     = space* open_paren or_expression close_paren space*

    and_expression        = space* condition space* (and_tuple)*
    or_expression         = space* and_expression space* (or_tuple)*
    and_tuple             = "AND" condition
    or_tuple              = "OR" and_expression

    collect_list          = collect_columns* (selected_expression)
    collect_columns       = selected_expression space* comma space*
    selected_expression   = low_pri_arithmetic space*

    group_list            = group_columns* (selected_expression)
    group_columns         = selected_expression space* comma space*
    order_list            = order_columns* low_pri_arithmetic ("ASC"/"DESC")
    order_columns         = low_pri_arithmetic ("ASC"/"DESC") space* comma space*

    clause                = space* ~r"[-=><\w]+" space*

    low_pri_arithmetic    = space* high_pri_arithmetic space* (low_pri_tuple)*
    high_pri_arithmetic   = space* arithmetic_term space* (high_pri_tuple)*
    low_pri_tuple         = low_pri_op high_pri_arithmetic
    high_pri_tuple        = high_pri_op arithmetic_term

    arithmetic_term       = space* (function_call / numeric_literal / subscriptable / column_name / parenthesized_arithm) space*
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
    subscriptable         = column_name open_square column_name close_square
    column_name           = ~r"[a-zA-Z_][a-zA-Z0-9_\.]*"
    function_name         = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_alias          = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_name           = ~r"[a-zA-Z]+"
    open_paren            = "("
    close_paren           = ")"
    open_square           = "["
    close_square          = "]"
    space                 = " "
    comma                 = ","
    colon                 = ":"

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


class SnQLVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_query_exp(self, node: Node, visited_children: Iterable[Any]) -> Query:
        match, where, collect, groupby, having, orderby, limit = visited_children
        # check for empty clauses
        if isinstance(groupby, Node):
            groupby = None
        if isinstance(orderby, Node):
            orderby = None
        if isinstance(having, Node):
            having = None
        if isinstance(limit, Node):
            limit = None

        selected_columns = collect
        _groupby = None
        if groupby:
            selected_columns += groupby
            _groupby = [g.expression for g in groupby]

        return Query(
            body={},
            from_clause=match,
            selected_columns=selected_columns,
            array_join=None,
            condition=where,
            prewhere=None,
            groupby=_groupby,
            having=having,
            order_by=orderby,
            limit=limit,
        )

    def visit_match_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, EntityTuple, Any],
    ) -> QueryEntity:
        _, _, _, match, _ = visited_children
        key = EntityKey(match.name)
        query_entity = QueryEntity(key, get_entity(key).get_data_model())
        return query_entity

    def visit_entity_match(
        self, node: Node, visited_children: Tuple[Any, str, Any, Any, str, Any],
    ) -> EntityTuple:
        _, alias, _, _, name, _ = visited_children
        return EntityTuple(alias, name)

    def visit_entity_alias(self, node: Node, visited_children: Tuple[Any]) -> str:
        return str(node.text)

    def visit_entity_name(self, node: Node, visited_children: Tuple[Any]) -> str:
        return str(node.text)

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return visit_function_name(node, visited_children)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return visit_column_name(node, visited_children)

    def visit_subscriptable(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Column:
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
        return visit_numeric_literal(node, visited_children)

    def visit_quoted_literal(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Literal:
        return visit_quoted_literal(node, visited_children)

    def visit_where_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Expression, Any]
    ) -> Expression:
        _, _, conditions, _ = visited_children
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
        self,
        node: Node,
        visited_children: Tuple[Expression, Any, str, Any, Expression, Any],
    ) -> Expression:
        exp, _, op, _, literal, _ = visited_children
        return binary_condition(None, op, exp, literal)

    def visit_condition_op(self, node: Node, visited_children: Iterable[Any]) -> str:
        return OPERATOR_TO_FUNCTION[node.text]

    def visit_order_by_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Sequence[OrderBy], Any]
    ) -> Sequence[OrderBy]:
        _, _, order_columns, _ = visited_children
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

    def visit_limit_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal, Any]
    ) -> int:
        _, _, _, limit, _ = visited_children
        assert isinstance(limit.value, int)  # mypy
        return limit.value

    def visit_group_by_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Sequence[SelectedExpression], Any],
    ) -> Sequence[SelectedExpression]:
        _, _, group_columns, _ = visited_children
        return group_columns

    def visit_group_columns(
        self, node: Node, visited_children: Tuple[SelectedExpression, Any, Any, Any]
    ) -> SelectedExpression:
        columns, _, _, _ = visited_children
        return columns

    def visit_group_list(
        self,
        node: Node,
        visited_children: Tuple[SelectedExpression, SelectedExpression],
    ) -> Sequence[SelectedExpression]:
        left_group_list, right_group = visited_children
        ret: List[SelectedExpression] = []

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

    def visit_collect_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Sequence[SelectedExpression], Any],
    ) -> Sequence[SelectedExpression]:
        _, _, selected_columns, _ = visited_children
        return selected_columns

    def visit_selected_expression(
        self, node: Node, visited_children: Tuple[Expression, Any]
    ) -> SelectedExpression:
        exp, _ = visited_children
        alias = exp.alias or node.text.strip()
        return SelectedExpression(alias, exp)

    def visit_collect_columns(
        self, node: Node, visited_children: Tuple[SelectedExpression, Any, Any, Any]
    ) -> SelectedExpression:
        columns, _, _, _, = visited_children
        return columns

    def visit_collect_list(
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


def parse_snql_query_initial(body: str) -> Query:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """
    exp_tree = snql_grammar.parse(body)
    parsed = SnQLVisitor().visit(exp_tree)
    assert isinstance(parsed, Query)  # mypy
    return parsed


def parse_snql_query(body: str, dataset: Dataset) -> Query:
    query = parse_snql_query_initial(body)
    _validate_aliases(query)
    _parse_subscriptables(query)
    _apply_column_aliases(query)
    _expand_aliases(query)
    _deescape_aliases(query)
    _mangle_aliases(query)
    return query
