from typing import (
    Any,
    Iterable,
    MutableMapping,
    NamedTuple,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from parsimonious.exceptions import ParseError
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
from snuba.query import Limitby, OrderBy, OrderByDirection, SelectedExpression
from snuba.query.logical import Query
from snuba.query.matchers import (
    Any as AnyMatch,
    FunctionCall as FunctionCallMatch,
    Literal as LiteralMatch,
    Param,
    String as StringMatch,
)
from snuba.query.parser import (
    _validate_empty_table_names,
    _validate_aliases,
    _parse_subscriptables,
    _apply_column_aliases,
    _expand_aliases,
    _deescape_aliases,
    _validate_arrayjoin,
)
from snuba.query.parser.validation import validate_query
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
    query_exp             = match_clause select_clause group_by_clause? where_clause? having_clause? order_by_clause? limit_by_clause? limit_clause? offset_clause? sample_clause?

    match_clause          = space* "MATCH" space* (relationship_match+ / entity_match / subquery) space*
    select_clause         = space* "SELECT" select_list space*
    group_by_clause       = space* "BY" group_list space*
    where_clause          = space* "WHERE" or_expression space*
    having_clause         = space* "HAVING" or_expression space*
    order_by_clause       = space* "ORDER BY" order_list space*
    limit_by_clause       = space* "LIMIT" space* integer_literal space* "BY" space* column_name space*
    limit_clause          = space* "LIMIT" space* integer_literal space*
    offset_clause         = space* "OFFSET" space* integer_literal space*
    sample_clause         = space* "SAMPLE" space* numeric_literal space*

    entity_match          = open_paren entity_alias colon space* entity_name sample_clause? close_paren
    relationship_link     = ~r"-\[" relationship_name ~r"\]->"
    relationship_match    = space* entity_match space* relationship_link space* entity_match space* comma*
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
    space                 = " "
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


class SnQLVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_query_exp(self, node: Node, visited_children: Iterable[Any]) -> Query:
        args: MutableMapping[str, Any] = {
            "body": {},
            "array_join": None,
            "prewhere": None,
        }
        (
            args["data_source"],
            args["selected_columns"],
            args["groupby"],
            args["condition"],
            args["having"],
            args["order_by"],
            args["limitby"],
            args["limit"],
            offset,
            args["sample"],
        ) = visited_children
        if not isinstance(offset, Node):
            args["offset"] = offset

        for k, v in args.items():
            if isinstance(v, Node):
                args[k] = None

        if args["data_source"].sample_rate is not None:
            args["sample"] = args["data_source"].sample_rate

        return Query(**args)

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

    def visit_match_clause(
        self,
        node: Node,
        visited_children: Tuple[
            Any, Any, Any, Union[EntityTuple, Sequence[RelationshipTuple]], Any
        ],
    ) -> Optional[QueryEntity]:
        _, _, _, match, _ = visited_children
        if isinstance(match, EntityTuple):
            key = EntityKey(match.name.lower())
            query_entity = QueryEntity(
                key, get_entity(key).get_data_model(), match.sample_rate
            )
            return query_entity
        elif isinstance(match, RelationshipTuple):
            # TODO: Change this once we have a proper data source for JOINs
            key = EntityKey(match.lhs.name.lower())
            query_entity = QueryEntity(key, get_entity(key).get_data_model())
            return query_entity
        if isinstance(match, list) and all(
            isinstance(m, RelationshipTuple) for m in match
        ):
            # TODO: Change this once we have a proper data source for JOINs
            key = EntityKey(match[0].lhs.name.lower())
            query_entity = QueryEntity(key, get_entity(key).get_data_model())
            return query_entity
        elif isinstance(match, Query):
            # TODO: Also need to be able to handle queries this as a data source
            return match.get_from_clause()

        return None

    def visit_relationship_match(
        self,
        node: Node,
        visited_children: Tuple[
            Any, EntityTuple, Any, Node, Any, EntityTuple, Any, Any
        ],
    ) -> RelationshipTuple:
        _, lhs, _, relationship, _, rhs, _, _ = visited_children
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
        return EntityTuple(alias, name, sample)

    def visit_entity_alias(self, node: Node, visited_children: Tuple[Any]) -> str:
        return str(node.text)

    def visit_entity_name(self, node: Node, visited_children: Tuple[Any]) -> str:
        return str(node.text)

    def visit_subquery(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Query:
        _, query, _ = visited_children
        assert isinstance(query, Query)  # mypy
        return query

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
        self, node: Node, visited_children: Tuple[Expression, str, Expression, Any]
    ) -> Expression:
        exp, op, literal, _ = visited_children
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

    def visit_sample_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal, Any]
    ) -> float:
        _, _, _, sample, _ = visited_children
        assert isinstance(sample.value, float)  # mypy
        return sample.value

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
        self, node: Node, visited_children: Tuple[Any, Any, Sequence[Expression], Any]
    ) -> Sequence[Expression]:
        _, _, group_columns, _ = visited_children
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
        visited_children: Tuple[Any, Any, Sequence[SelectedExpression], Any],
    ) -> Sequence[SelectedExpression]:
        _, _, selected_columns, _ = visited_children
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
        internal_f = FunctionCall(alias, name, param_list1)
        if isinstance(params2, Node) and params2.text == "":
            # params2.text == "" means empty node.
            return internal_f

        _, param_list2, _ = params2
        if isinstance(param_list2, (list, tuple)) and len(param_list2) > 0:
            param_list2 = tuple(param_list2)
        else:
            # This happens when the second parameter list is empty. Somehow
            # it does not turn into an empty list.
            param_list2 = ()
        return CurriedFunctionCall(None, internal_f, param_list2)

    def generic_visit(self, node: Node, visited_children: Any) -> Any:
        return generic_visit(node, visited_children)


def parse_snql_query_initial(body: str) -> Query:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """
    exp_tree = snql_grammar.parse(body)
    return SnQLVisitor().visit(exp_tree)


DATETIME_MATCH = FunctionCallMatch(
    StringMatch("toDateTime"), (Param("date_string", LiteralMatch(AnyMatch(str))),)
)


def _parse_datetime_literals(query: Query) -> None:
    def parse(exp: Expression) -> Expression:
        result = DATETIME_MATCH.match(exp)
        if result is not None:
            date_string = result.expression("date_string")
            assert isinstance(date_string, Literal)  # mypy
            assert isinstance(date_string.value, str)  # mypy
            return Literal(exp.alias, parse_datetime(date_string.value))

        return exp

    query.transform_expressions(parse)


def parse_snql_query(body: str, dataset: Dataset) -> Query:
    query = parse_snql_query_initial(body)
    entity = get_entity(query.get_from_clause().key)
    # These are the post processing phases
    _parse_datetime_literals(query)
    _validate_empty_table_names(query)
    _validate_aliases(query)
    _parse_subscriptables(query)
    _apply_column_aliases(query)
    _expand_aliases(query)
    # WARNING: These steps above assume table resolution did not happen
    # yet. If it is put earlier than here (unlikely), we need to adapt them.
    _deescape_aliases(query)
    _validate_arrayjoin(query)
    validate_query(query, entity)

    return query
