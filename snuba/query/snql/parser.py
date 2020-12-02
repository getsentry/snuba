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
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.matchers import (
    Any as AnyMatch,
    AnyExpression,
    AnyOptionalString,
    Column as ColumnMatch,
    FunctionCall as FunctionCallMatch,
    Literal as LiteralMatch,
    Param,
    Or,
    String as StringMatch,
)
from snuba.query import (
    LimitBy,
    OrderBy,
    OrderByDirection,
    SelectedExpression,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.parser import (
    _apply_column_aliases,
    _deescape_aliases,
    _expand_aliases,
    _mangle_aliases,
    _parse_subscriptables,
    _validate_aliases,
)
from snuba.query.schema import POSITIVE_OPERATORS
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
from snuba.util import parse_datetime

snql_grammar = Grammar(
    r"""
    query_exp             = match_clause select_clause group_by_clause? where_clause? having_clause? order_by_clause? limit_by_clause? limit_clause? offset_clause? granularity_clause? totals_clause? space*

    match_clause          = space* "MATCH" space+ entity_match
    select_clause         = space+ "SELECT" space+ select_list
    group_by_clause       = space+ "BY" space+ group_list
    where_clause          = space+ "WHERE" space+ or_expression
    having_clause         = space+ "HAVING" space+ or_expression
    order_by_clause       = space+ "ORDER BY" space+ order_list
    limit_by_clause       = space+ "LIMIT" space+ integer_literal space+ "BY" space+ column_name
    limit_clause          = space+ "LIMIT" space+ integer_literal
    offset_clause         = space+ "OFFSET" space+ integer_literal
    granularity_clause    = space+ "GRANULARITY" space+ integer_literal
    totals_clause         = space+ "TOTALS" space+ boolean_literal

    entity_match          = open_paren entity_alias colon space* entity_name sample_clause? space* close_paren
    sample_clause         = space+ "SAMPLE" space+ numeric_literal

    and_expression        = space* condition and_tuple*
    or_expression         = space* and_expression or_tuple*
    and_tuple             = space+ "AND" condition
    or_tuple              = space+ "OR" and_expression

    condition             = main_condition / parenthesized_cdn
    main_condition        = low_pri_arithmetic space* condition_op space* (function_call / column_name / quoted_literal / numeric_literal)
    condition_op          = "!=" / ">=" / ">" / "<=" / "<" / "=" / "IN" / "LIKE"
    parenthesized_cdn     = space* open_paren or_expression close_paren

    select_list          = select_columns* (selected_expression)
    select_columns       = selected_expression space* comma
    selected_expression  = space* low_pri_arithmetic

    group_list            = group_columns* (selected_expression)
    group_columns         = selected_expression space* comma
    order_list            = order_columns* low_pri_arithmetic space+ ("ASC"/"DESC")
    order_columns         = low_pri_arithmetic space+ ("ASC"/"DESC") space* comma space*

    low_pri_arithmetic    = space* high_pri_arithmetic (space* low_pri_tuple)*
    high_pri_arithmetic   = space* arithmetic_term (space* high_pri_tuple)*
    low_pri_tuple         = low_pri_op space* high_pri_arithmetic
    high_pri_tuple        = high_pri_op space* arithmetic_term

    arithmetic_term       = space* (function_call / numeric_literal / subscriptable / column_name / parenthesized_arithm)
    parenthesized_arithm  = open_paren low_pri_arithmetic close_paren

    low_pri_op            = "+" / "-"
    high_pri_op           = "/" / "*"
    param_expression      = low_pri_arithmetic / quoted_literal
    parameters_list       = parameter* (param_expression)
    parameter             = param_expression space* comma space*
    function_call         = function_name open_paren parameters_list? close_paren (open_paren parameters_list? close_paren)? (space* "AS" space* string_literal)?
    simple_term           = quoted_literal / numeric_literal / column_name
    literal               = ~r"[a-zA-Z0-9_\.:-]+"
    quoted_literal        = ~r"((?<!\\)')((?!(?<!\\)').)*.?'"
    string_literal        = ~r"[a-zA-Z0-9_\.\+\*\/:\-]*"
    numeric_literal       = ~r"-?[0-9]+(\.[0-9]+)?(e[\+\-][0-9]+)?"
    integer_literal       = ~r"-?[0-9]+"
    boolean_literal       = true_literal / false_literal
    true_literal          = ~r"TRUE"i
    false_literal         = ~r"FALSE"i
    subscriptable         = column_name open_square column_name close_square
    column_name           = ~r"[a-zA-Z_][a-zA-Z0-9_\.]*"
    function_name         = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_alias          = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_name           = ~r"[a-zA-Z_]+"
    open_paren            = "("
    close_paren           = ")"
    open_square           = "["
    close_square          = "]"
    space                 = ~r"\s"
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
    sample_rate: Optional[float]


class SnQLVisitor(NodeVisitor):
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    def visit_query_exp(
        self, node: Node, visited_children: Iterable[Any]
    ) -> LogicalQuery:
        args: MutableMapping[str, Any] = {}
        (
            data_source,
            args["selected_columns"],
            args["groupby"],
            args["condition"],
            args["having"],
            args["order_by"],
            args["limitby"],
            args["limit"],
            args["offset"],
            args["granularity"],
            args["totals"],
            _,
        ) = visited_children

        keys = list(args.keys())
        for k in keys:
            if isinstance(args[k], Node):
                del args[k]

        args.update({"body": {}, "prewhere": None, "from_clause": data_source})
        if isinstance(data_source, QueryEntity):
            # TODO: How sample rate gets stored needs to be addressed in a future PR
            args["sample"] = data_source.sample

        if "groupby" in args:
            if "selected_columns" not in args:
                args["selected_columns"] = []
            args["selected_columns"] += args["groupby"]
            args["groupby"] = map(lambda gb: gb.expression, args["groupby"])

        return LogicalQuery(**args)

    def visit_match_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, EntityTuple],
    ) -> QueryEntity:
        _, _, _, match = visited_children
        key = EntityKey(match.name)
        query_entity = QueryEntity(
            key, get_entity(key).get_data_model(), match.sample_rate
        )
        return query_entity

    def visit_entity_match(
        self,
        node: Node,
        visited_children: Tuple[Any, str, Any, Any, str, Optional[float], Any, Any],
    ) -> EntityTuple:
        _, alias, _, _, name, sample, _, _ = visited_children
        if isinstance(sample, Node):
            sample = None
        return EntityTuple(alias, name.lower(), sample)

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
        self, node: Node, visited_children: Tuple[Any, Node, Expression]
    ) -> AndTuple:
        _, and_string, exp = visited_children
        return AndTuple(and_string.text, exp)

    def visit_or_tuple(
        self, node: Node, visited_children: Tuple[Any, Node, Expression]
    ) -> OrTuple:
        _, or_string, exp = visited_children
        return OrTuple(or_string.text, exp)

    def visit_parenthesized_cdn(
        self, node: Node, visited_children: Tuple[Any, Any, Expression, Any]
    ) -> Expression:
        _, _, condition, _ = visited_children
        return condition

    def visit_parenthesized_arithm(
        self, node: Node, visited_children: Tuple[Any, Expression, Any]
    ) -> Expression:
        _, arithm, _ = visited_children
        return arithm

    def visit_low_pri_tuple(
        self, node: Node, visited_children: Tuple[LowPriOperator, Any, Expression]
    ) -> LowPriTuple:
        return visit_low_pri_tuple(node, visited_children)

    def visit_high_pri_tuple(
        self, node: Node, visited_children: Tuple[HighPriOperator, Any, Expression]
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
        self, node: Node, visited_children: Tuple[Any, Expression]
    ) -> Expression:
        return visit_arithmetic_term(node, visited_children)

    def visit_low_pri_arithmetic(
        self, node: Node, visited_children: Tuple[Any, Expression, LowPriArithmetic],
    ) -> Expression:
        return visit_low_pri_arithmetic(node, visited_children)

    def visit_high_pri_arithmetic(
        self, node: Node, visited_children: Tuple[Any, Expression, HighPriArithmetic],
    ) -> Expression:
        return visit_high_pri_arithmetic(node, visited_children)

    def visit_numeric_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return visit_numeric_literal(node, visited_children)

    def visit_integer_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return Literal(None, int(node.text))

    def visit_boolean_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        if node.text.lower() == "true":
            return Literal(None, True)

        return Literal(None, False)

    def visit_quoted_literal(
        self, node: Node, visited_children: Tuple[Node]
    ) -> Literal:

        return visit_quoted_literal(node, visited_children)

    def visit_where_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Expression]
    ) -> Expression:
        _, _, _, conditions = visited_children
        return conditions

    def visit_having_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Expression]
    ) -> Expression:
        _, _, conditions = visited_children
        return conditions

    def visit_and_expression(
        self, node: Node, visited_children: Tuple[Any, Expression, Node],
    ) -> Expression:
        _, left_condition, and_condition = visited_children
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
                if isinstance(elem, Node):
                    continue
                elif isinstance(elem, (AndTuple, OrTuple)):
                    args.append(elem.exp)
        return combine_and_conditions(args)

    def visit_or_expression(
        self, node: Node, visited_children: Tuple[Any, Expression, Node]
    ) -> Expression:
        _, left_condition, or_condition = visited_children
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
                if isinstance(elem, Node):
                    continue
                elif isinstance(elem, (AndTuple, OrTuple)):
                    args.append(elem.exp)
        return combine_or_conditions(args)

    def visit_main_condition(
        self,
        node: Node,
        visited_children: Tuple[Expression, Any, str, Any, Expression],
    ) -> Expression:
        exp, _, op, _, literal = visited_children
        return binary_condition(op, exp, literal)

    def visit_condition_op(self, node: Node, visited_children: Iterable[Any]) -> str:
        return OPERATOR_TO_FUNCTION[node.text]

    def visit_order_by_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Sequence[OrderBy]]
    ) -> Sequence[OrderBy]:
        _, _, _, order_columns = visited_children
        return order_columns

    def visit_order_list(
        self, node: Node, visited_children: Tuple[OrderBy, Expression, Any, Node]
    ) -> Sequence[OrderBy]:
        left_order_list, right_order, _, order = visited_children
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
        self, node: Node, visited_children: Tuple[Expression, Any, Node, Any, Any, Any]
    ) -> OrderBy:
        column, _, order, _, _, _ = visited_children

        direction = (
            OrderByDirection.ASC if order.text == "ASC" else OrderByDirection.DESC
        )
        return OrderBy(direction, column)

    def visit_sample_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal]
    ) -> float:
        _, _, _, sample = visited_children
        assert isinstance(sample.value, float)  # mypy
        return sample.value

    def visit_granularity_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal]
    ) -> float:
        _, _, _, granularity = visited_children
        assert isinstance(granularity.value, int)  # mypy
        return granularity.value

    def visit_totals_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal]
    ) -> float:
        _, _, _, totals = visited_children
        assert isinstance(totals.value, bool)  # mypy
        return totals.value

    def visit_limit_by_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Literal, Any, Any, Any, Column],
    ) -> LimitBy:
        _, _, _, limit, _, _, _, column = visited_children
        assert isinstance(limit.value, int)  # mypy
        return LimitBy(limit.value, column)

    def visit_limit_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal]
    ) -> int:
        _, _, _, limit = visited_children
        assert isinstance(limit.value, int)  # mypy
        return limit.value

    def visit_offset_clause(
        self, node: Node, visited_children: Tuple[Any, Any, Any, Literal]
    ) -> int:
        _, _, _, offset = visited_children
        assert isinstance(offset.value, int)  # mypy
        return offset.value

    def visit_group_by_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Sequence[SelectedExpression]],
    ) -> Sequence[SelectedExpression]:
        _, _, _, group_columns = visited_children
        return group_columns

    def visit_group_columns(
        self, node: Node, visited_children: Tuple[SelectedExpression, Any, Any]
    ) -> SelectedExpression:
        columns, _, _ = visited_children
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

    def visit_select_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Sequence[SelectedExpression]],
    ) -> Sequence[SelectedExpression]:
        _, _, _, selected_columns = visited_children
        return selected_columns

    def visit_selected_expression(
        self, node: Node, visited_children: Tuple[Any, Expression]
    ) -> SelectedExpression:
        _, exp = visited_children
        alias = exp.alias or node.text.strip()
        return SelectedExpression(alias, exp)

    def visit_select_columns(
        self, node: Node, visited_children: Tuple[SelectedExpression, Any, Any]
    ) -> SelectedExpression:
        columns, _, _ = visited_children
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


def parse_snql_query_initial(body: str) -> LogicalQuery:
    """
    Parses the query body generating the AST. This only takes into
    account the initial query body. Extensions are parsed by extension
    processors and are supposed to update the AST.
    """
    exp_tree = snql_grammar.parse(body)
    parsed = SnQLVisitor().visit(exp_tree)
    assert isinstance(parsed, LogicalQuery)  # mypy
    return parsed


DATETIME_MATCH = FunctionCallMatch(
    StringMatch("toDateTime"), (Param("date_string", LiteralMatch(AnyMatch(str))),)
)


def _parse_datetime_literals(query: LogicalQuery) -> None:
    def parse(exp: Expression) -> Expression:
        result = DATETIME_MATCH.match(exp)
        if result is not None:
            date_string = result.expression("date_string")
            assert isinstance(date_string, Literal)  # mypy
            assert isinstance(date_string.value, str)  # mypy
            return Literal(exp.alias, parse_datetime(date_string.value))

        return exp

    query.transform_expressions(parse)


ARRAY_JOIN_MATCH = FunctionCallMatch(
    StringMatch("arrayJoinExplode"),
    (
        Param("column", ColumnMatch(AnyOptionalString(), AnyMatch(str))),
        Param("op", Or([LiteralMatch(StringMatch(op)) for op in OPERATOR_TO_FUNCTION])),
        Param("value", AnyExpression()),
    ),
)


def _array_join_transformation(query: LogicalQuery) -> None:
    def parse(exp: Expression) -> Expression:
        result = ARRAY_JOIN_MATCH.match(exp)
        if result:
            column = result.expression("column")
            assert isinstance(column, Column)

            op_literal = result.expression("op")
            assert isinstance(op_literal, Literal)
            op = str(op_literal.value)

            value = result.expression("value")

            function_name = "arrayExists" if op in POSITIVE_OPERATORS else "arrayAll"

            return FunctionCall(
                None,
                function_name,
                (
                    Lambda(
                        None,
                        ("x",),
                        FunctionCall(
                            None,
                            "assumeNotNull",
                            (
                                FunctionCall(
                                    None,
                                    OPERATOR_TO_FUNCTION[op],
                                    (Argument(None, "x"), value,),
                                ),
                            ),
                        ),
                    ),
                    column,
                ),
            )

        return exp

    query.transform_expressions(parse)


def parse_snql_query(body: str, dataset: Dataset) -> LogicalQuery:
    query = parse_snql_query_initial(body)

    # These are the post processing phases
    _parse_datetime_literals(query)
    _array_join_transformation(query)

    # TODO: Update these functions to work with snql queries in a separate PR
    _validate_aliases(query)  # -> needs to recurse through sub queries
    _parse_subscriptables(query)  # -> This should be part of the grammar
    _apply_column_aliases(query)  # -> needs to recurse through sub queries
    _expand_aliases(query)  # -> needs to recurse through sub queries
    _deescape_aliases(
        query
    )  # -> This should not be needed at all, assuming SnQL can properly accept escaped/unicode strings
    _mangle_aliases(query)  # needs to recurse through sub queries
    return query
