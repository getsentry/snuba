from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta
from functools import partial
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    MutableMapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import sentry_sdk
from parsimonious.exceptions import IncompleteParseError
from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from snuba import state
from snuba.clickhouse.columns import Array
from snuba.clickhouse.query_dsl.accessors import get_time_range_expressions
from snuba.datasets.dataset import Dataset
from snuba.datasets.entities.entity_data_model import EntityColumnSet
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset_name
from snuba.query import (
    LimitBy,
    OrderBy,
    OrderByDirection,
    Select,
    SelectDistinct,
    SelectedExpression,
)
from snuba.query.composite import CompositeQuery
from snuba.query.conditions import (
    FUNCTION_TO_OPERATOR,
    OPERATOR_TO_FUNCTION,
    ConditionFunctions,
    binary_condition,
    build_match,
    combine_and_conditions,
    combine_or_conditions,
    get_first_level_and_conditions,
    is_condition,
    unary_condition,
)
from snuba.query.data_source.join import IndividualNode, JoinClause
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidExpressionException, InvalidQueryException
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import AnyExpression, AnyOptionalString
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or, Param
from snuba.query.matchers import String as StringMatch
from snuba.query.parser import (
    apply_column_aliases,
    expand_aliases,
    parse_subscriptables,
    validate_aliases,
)
from snuba.query.parser.exceptions import ParsingException
from snuba.query.parser.validation import validate_query
from snuba.query.query_settings import QuerySettings
from snuba.query.schema import POSITIVE_OPERATORS
from snuba.query.snql.anonymize import format_snql_anonymized
from snuba.query.snql.discover_entity_selection import select_discover_entity
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
from snuba.query.snql.joins import RelationshipTuple, build_join_clause
from snuba.state import explain_meta
from snuba.util import parse_datetime

logger = logging.getLogger("snuba.snql.parser")

snql_grammar = Grammar(
    r"""
    query_exp             = match_clause (distinct_clause / select_clause) group_by_clause? arrayjoin_clause? where_clause? having_clause? order_by_clause? limit_by_clause? limit_clause? offset_clause? granularity_clause? totals_clause? space*

    match_clause          = space* "MATCH" space+ (relationships / subquery / entity_single )
    distinct_clause       = space+ "DISTINCT" space+ select_list
    select_clause         = space+ "SELECT" space+ select_list
    group_by_clause       = space+ "BY" space+ group_list
    arrayjoin_clause      = space+ "ARRAY JOIN" space+ arrayjoin_entity arrayjoin_optional
    where_clause          = space+ "WHERE" space+ or_expression
    having_clause         = space+ "HAVING" space+ or_expression
    order_by_clause       = space+ "ORDER BY" space+ order_list
    limit_by_clause       = space+ "LIMIT" space+ integer_literal space+ "BY" space+ column_name limit_by_columns
    limit_clause          = space+ "LIMIT" space+ integer_literal
    offset_clause         = space+ "OFFSET" space+ integer_literal
    granularity_clause    = space+ "GRANULARITY" space+ integer_literal
    totals_clause         = space+ "TOTALS" space+ boolean_literal

    entity_single         = open_paren space* entity_name sample_clause? space* close_paren
    entity_match          = open_paren entity_alias colon space* entity_name sample_clause? space* close_paren
    relationship_link     = ~r"-\[" relationship_name ~r"\]->"
    relationship_match    = space* entity_match space* relationship_link space* entity_match
    relationships         = relationship_match (comma relationship_match)*
    subquery              = open_brace query_exp close_brace
    sample_clause         = space+ "SAMPLE" space+ numeric_literal

    and_expression        = space* condition and_tuple*
    or_expression         = space* and_expression or_tuple*
    and_tuple             = space+ "AND" condition
    or_tuple              = space+ "OR" and_expression

    condition             = unary_condition / main_condition / parenthesized_cdn
    unary_condition       = low_pri_arithmetic space+ unary_op
    main_condition        = low_pri_arithmetic space* condition_op space* (function_call / simple_term)
    condition_op          = "!=" / ">=" / ">" / "<=" / "<" / "=" / "NOT IN" / "NOT LIKE" / "IN" / "LIKE"
    unary_op              = "IS NULL" / "IS NOT NULL"
    parenthesized_cdn     = space* open_paren or_expression close_paren

    select_list          = select_columns* (selected_expression)
    select_columns       = selected_expression space* comma
    selected_expression  = space* (aliased_tag_column / aliased_subscriptable / aliased_column_name / low_pri_arithmetic)

    arrayjoin_entity     = tag_column / subscriptable / simple_term
    arrayjoin_optional   = (space* comma space* arrayjoin_entity)*

    group_list            = group_columns* (selected_expression)
    group_columns         = selected_expression space* comma
    order_list            = order_columns* low_pri_arithmetic space+ ("ASC"/"DESC")
    order_columns         = low_pri_arithmetic space+ ("ASC"/"DESC") space* comma space*
    limit_by_columns      = (space* comma space* column_name)*

    low_pri_arithmetic    = space* high_pri_arithmetic (space* low_pri_tuple)*
    high_pri_arithmetic   = space* arithmetic_term (space* high_pri_tuple)*
    low_pri_tuple         = low_pri_op space* high_pri_arithmetic
    high_pri_tuple        = high_pri_op space* arithmetic_term

    arithmetic_term       = space* (function_call / tag_column / subscriptable / simple_term / parenthesized_arithm)
    parenthesized_arithm  = open_paren low_pri_arithmetic close_paren

    low_pri_op            = "+" / "-"
    high_pri_op           = "/" / "*"
    param_expression      = low_pri_arithmetic / quoted_literal / identifier
    parameters_list       = parameter* (param_expression)
    parameter             = (lambda / param_expression) space* comma space*
    function_call         = function_name open_paren parameters_list? close_paren (open_paren parameters_list? close_paren)? (space+ "AS" space+ (quoted_alias_literal / alias_literal))?
    lambda                = open_paren space* identifier (comma space* identifier)* space* close_paren space* arrow space* function_call

    aliased_tag_column    = tag_column space+ "AS" space+ (quoted_alias_literal / alias_literal)
    aliased_subscriptable = subscriptable space+ "AS" space+ (quoted_alias_literal / alias_literal)
    aliased_column_name   = column_name space+ "AS" space+ (quoted_alias_literal / alias_literal)

    simple_term           = quoted_literal / numeric_literal / null_literal / boolean_literal / column_name
    quoted_literal        = ~r"(?<!\\)'(?:(?<!\\)(?:\\{2})*\\'|[^'])*(?<!\\)(?:\\{2})*'"
    string_literal        = ~r"[a-zA-Z0-9_\.\+\*\/:\-]*"
    alias_literal         = ~r"[a-zA-Z0-9_\.\+\*\/:\-\[\]]*"
    quoted_alias_literal  = backtick ~r"[a-zA-Z0-9_\.\+\*\/:\-\[\]\(\)\@]*" backtick
    numeric_literal       = ~r"-?[0-9]+(\.[0-9]+)?(e[\+\-][0-9]+)?"
    integer_literal       = ~r"-?[0-9]+"
    boolean_literal       = true_literal / false_literal
    true_literal          = ~r"TRUE"i
    false_literal         = ~r"FALSE"i
    null_literal          = ~r"NULL"i
    subscriptable         = column_name open_square (column_name/tag_name) close_square
    column_name           = ~r"[a-zA-Z_][a-zA-Z0-9_\.:@/]*"
    tag_column            = "tags" open_square tag_name close_square
    tag_name              = ~r"[^\[\]]*"
    identifier            = backtick ~r"[a-zA-Z_][a-zA-Z0-9_]*" backtick
    function_name         = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_alias          = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    entity_name           = ~r"[a-zA-Z_]+"
    relationship_name     = ~r"[a-zA-Z_][a-zA-Z0-9_]*"
    arrow                 = "->"
    open_brace            = "{"
    close_brace           = "}"
    open_paren            = "("
    close_paren           = ")"
    open_square           = "["
    close_square          = "]"
    space                 = ~r"\s"
    comma                 = ","
    colon                 = ":"
    backtick              = "`"

"""
)


class AndTuple(NamedTuple):
    op: str
    exp: Expression


class OrTuple(NamedTuple):
    op: str
    exp: Expression


class SnQLVisitor(NodeVisitor):  # type: ignore
    """
    Builds Snuba AST expressions from the Parsimonious parse tree.
    """

    @staticmethod
    def __extract_alias_from_match(alias: Union[Node, List[Node]]) -> str:
        extracted_alias: str
        if isinstance(alias, list):
            # Validate that we are parsing an expression that is
            # 'quoted_aliased_literal' -> "backtick alias backtick"
            if not (
                len(alias) == 3
                and alias[0].expr_name == alias[2].expr_name == "backtick"
            ):
                raise ParsingException(f"Error parsing quoted alias: {alias}")
            extracted_alias = alias[1].text
        else:
            extracted_alias = alias.text
        return extracted_alias

    def visit_query_exp(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Union[LogicalQuery, CompositeQuery[QueryEntity]]:
        args: MutableMapping[str, Any] = {}
        (
            data_source,
            args["selected_columns"],
            args["groupby"],
            args["array_join"],
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

        if "groupby" in args:
            if "selected_columns" not in args:
                args["selected_columns"] = args["groupby"]
            else:
                args["selected_columns"] = args["groupby"] + args["selected_columns"]

            args["groupby"] = [gb.expression for gb in args["groupby"]]

        if isinstance(data_source, (CompositeQuery, LogicalQuery, JoinClause)):
            args["from_clause"] = data_source
            return CompositeQuery(**args)

        args.update({"prewhere": None, "from_clause": data_source})
        if isinstance(data_source, QueryEntity):
            # TODO: How sample rate gets stored needs to be addressed in a future PR
            args["sample"] = data_source.sample

        return LogicalQuery(**args)

    def visit_match_clause(
        self,
        node: Node,
        visited_children: Tuple[
            Any,
            Any,
            Any,
            Union[
                QueryEntity,
                CompositeQuery[QueryEntity],
                LogicalQuery,
                RelationshipTuple,
                Sequence[RelationshipTuple],
            ],
        ],
    ) -> Union[
        CompositeQuery[QueryEntity],
        LogicalQuery,
        QueryEntity,
        JoinClause[QueryEntity],
    ]:
        _, _, _, match = visited_children
        if isinstance(match, (CompositeQuery, LogicalQuery)):
            return match
        elif isinstance(match, RelationshipTuple):
            join_clause = build_join_clause([match])
            return join_clause
        if isinstance(match, list) and all(
            isinstance(m, RelationshipTuple) for m in match
        ):
            join_clause = build_join_clause(match)
            return join_clause

        assert isinstance(match, QueryEntity)  # mypy
        return match

    def visit_entity_single(
        self,
        node: Node,
        visited_children: Tuple[
            Any, Any, EntityKey, Union[Optional[float], Node], Any, Any
        ],
    ) -> QueryEntity:
        _, _, name, sample, _, _ = visited_children
        if isinstance(sample, Node):
            sample = None

        return QueryEntity(name, get_entity(name).get_data_model(), sample)

    def visit_entity_match(
        self,
        node: Node,
        visited_children: Tuple[
            Any, str, Any, Any, EntityKey, Union[Optional[float], Node], Any, Any
        ],
    ) -> IndividualNode[QueryEntity]:
        _, alias, _, _, name, sample, _, _ = visited_children
        if isinstance(sample, Node):
            sample = None

        return IndividualNode(
            alias, QueryEntity(name, get_entity(name).get_data_model(), sample)
        )

    def visit_entity_alias(self, node: Node, visited_children: Tuple[Any]) -> str:
        return str(node.text)

    def visit_entity_name(self, node: Node, visited_children: Tuple[Any]) -> EntityKey:
        try:
            return EntityKey(node.text)
        except Exception:
            raise ParsingException(f"{node.text} is not a valid entity name")

    def visit_relationships(
        self,
        node: Node,
        visited_children: Tuple[RelationshipTuple, Any],
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
        visited_children: Tuple[
            Any,
            IndividualNode[QueryEntity],
            Any,
            Node,
            Any,
            IndividualNode[QueryEntity],
        ],
    ) -> RelationshipTuple:
        _, lhs, _, relationship, _, rhs = visited_children
        assert isinstance(lhs.data_source, QueryEntity)
        assert isinstance(rhs.data_source, QueryEntity)
        lhs_entity = get_entity(lhs.data_source.key)
        data = lhs_entity.get_join_relationship(relationship)
        if data is None:
            raise ParsingException(
                f"{lhs.data_source.key.value} does not have a join relationship -[{relationship}]->"
            )
        elif data.rhs_entity != rhs.data_source.key:
            raise ParsingException(
                f"-[{relationship}]-> cannot be used to join {lhs.data_source.key.value} to {rhs.data_source.key.value}"
            )

        return RelationshipTuple(lhs, relationship, rhs, data)

    def visit_relationship_link(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> str:
        _, relationship, _ = visited_children
        return str(relationship.text)

    def visit_subquery(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Union[LogicalQuery, CompositeQuery[QueryEntity]]:
        _, query, _ = visited_children
        assert isinstance(query, (CompositeQuery, LogicalQuery))  # mypy
        return query

    def visit_function_name(self, node: Node, visited_children: Iterable[Any]) -> str:
        return visit_function_name(node, visited_children)

    def visit_column_name(self, node: Node, visited_children: Iterable[Any]) -> Column:
        return visit_column_name(node, visited_children)

    def visit_subscriptable(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Column:
        return visit_column_name(node, visited_children)

    def visit_tag_column(self, node: Node, visited_children: Iterable[Any]) -> Column:
        x = visit_column_name(node, visited_children)
        return x

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
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, LowPriArithmetic],
    ) -> Expression:
        return visit_low_pri_arithmetic(node, visited_children)

    def visit_high_pri_arithmetic(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, HighPriArithmetic],
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

    def visit_null_literal(
        self, node: Node, visited_children: Iterable[Any]
    ) -> Literal:
        return Literal(None, None)

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
        self, node: Node, visited_children: Tuple[Any, Any, Any, Expression]
    ) -> Expression:
        _, _, _, conditions = visited_children
        return conditions

    def visit_and_expression(
        self,
        node: Node,
        visited_children: Tuple[Any, Expression, Node],
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

    def visit_unary_condition(
        self, node: Node, visited_children: Tuple[Expression, Any, str]
    ) -> Expression:
        exp, _, op = visited_children
        return unary_condition(op, exp)

    def visit_unary_op(self, node: Node, visited_children: Iterable[Any]) -> str:
        return OPERATOR_TO_FUNCTION[node.text]

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
        visited_children: Tuple[
            Any, Any, Any, Literal, Any, Any, Any, Column, Optional[Sequence[Column]]
        ],
    ) -> LimitBy:
        _, _, _, limit, _, _, _, column_one, columns_rest = visited_children
        assert isinstance(limit.value, int)  # mypy
        columns = [column_one]
        if columns_rest is not None:
            columns.extend(columns_rest)
        return LimitBy(limit.value, columns)

    def visit_limit_by_columns(
        self, node: Node, visited_children: Sequence[Tuple[Any, Any, Any, Column]]
    ) -> Sequence[Column]:
        columns: List[Column] = []
        for column_visit in visited_children:
            _, _, _, column_inst = column_visit
            columns.append(column_inst)
        return columns

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
    ) -> Select:
        _, _, _, selected_columns = visited_children
        return Select(selected_columns)

    def visit_distinct_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Sequence[SelectedExpression]],
    ) -> SelectDistinct:
        _, _, _, selected_columns = visited_children
        return SelectDistinct(selected_columns)

    def visit_selected_expression(
        self,
        node: Node,
        visited_children: Tuple[Any, Union[SelectedExpression, Expression]],
    ) -> SelectedExpression:
        _, exp = visited_children
        if isinstance(exp, SelectedExpression):
            return exp

        alias = exp.alias or node.text.strip()
        if not isinstance(exp, Column) and exp.alias is None:
            exp = replace(exp, alias=alias)
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

    def visit_arrayjoin_clause(
        self,
        node: Node,
        visited_children: Tuple[Any, Any, Any, Expression, Optional[List[Expression]]],
    ) -> Sequence[Expression]:
        _, _, _, join_first, join_rest = visited_children
        exprs = [join_first]

        if join_rest is not None:
            exprs.extend(join_rest)

        return exprs

    def visit_arrayjoin_optional(
        self,
        node: Node,
        visited_children: List[Tuple[Any, Any, Any, Expression]],
    ) -> List[Expression]:
        exprs: List[Expression] = list()
        if visited_children is not None:
            for child in visited_children:
                _, _, _, exp = child
                exprs.append(exp)
        return exprs

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
            alias = self.__extract_alias_from_match(alias)

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

    def visit_aliased_tag_column(
        self,
        node: Node,
        visited_children: Tuple[Column, Any, Any, Any, Node],
    ) -> SelectedExpression:
        column, _, _, _, alias = visited_children
        return SelectedExpression(self.__extract_alias_from_match(alias), column)

    def visit_aliased_subscriptable(
        self,
        node: Node,
        visited_children: Tuple[Column, Any, Any, Any, Node],
    ) -> SelectedExpression:
        column, _, _, _, alias = visited_children
        return SelectedExpression(self.__extract_alias_from_match(alias), column)

    def visit_aliased_column_name(
        self,
        node: Node,
        visited_children: Tuple[Column, Any, Any, Any, Node],
    ) -> SelectedExpression:
        column, _, _, _, alias = visited_children
        return SelectedExpression(self.__extract_alias_from_match(alias), column)

    def visit_identifier(
        self, node: Node, visited_children: Tuple[Any, Node, Any]
    ) -> Argument:
        return Argument(None, visited_children[1].text)

    def visit_lambda(
        self,
        node: Node,
        visited_children: Tuple[
            Any,
            Any,
            Argument,
            Union[Node, List[Node | Argument]],
            Any,
            Any,
            Any,
            Any,
            Any,
            Expression,
        ],
    ) -> Lambda:
        first_identifier = visited_children[2]
        other_identifiers = visited_children[3]
        functionCall = visited_children[-1]
        parameters = [first_identifier.name]
        if isinstance(other_identifiers, list):
            for other in other_identifiers:
                if isinstance(other, Argument):
                    parameters.append(other.name)
                elif isinstance(other, list):
                    parameters.extend(
                        [o.name for o in other if isinstance(o, Argument)]
                    )

        return Lambda(None, tuple(parameters), functionCall)

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
    try:
        exp_tree = snql_grammar.parse(body)
        parsed = SnQLVisitor().visit(exp_tree)
    except ParsingException as e:
        logger.warning(f"Invalid SnQL query ({e}): {body}")
        raise e
    except IncompleteParseError as e:
        lines = body.split("\n")
        if e.line() > len(lines):
            line = body
        else:
            line = lines[e.line() - 1]

        idx = e.column()
        prefix = line[max(0, idx - 3) : idx]
        suffix = line[idx : (idx + 10)]
        raise ParsingException(
            f"Parsing error on line {e.line()} at '{prefix}{suffix}'"
        )
    except Exception as e:
        message = str(e)
        if "\n" in message:
            message, _ = message.split("\n", 1)
        raise ParsingException(message)

    assert isinstance(parsed, (CompositeQuery, LogicalQuery))  # mypy

    # Add these defaults here to avoid them getting applied to subqueries
    limit = parsed.get_limit()
    if limit is None:
        parsed.set_limit(1000)
    elif limit > 10000:
        raise ParsingException(
            "queries cannot have a limit higher than 10000", should_report=False
        )

    if parsed.get_offset() is None:
        parsed.set_offset(0)

    return parsed


def _qualify_columns(query: Union[CompositeQuery[QueryEntity], LogicalQuery]) -> None:
    """
    All columns in a join query should be qualified with the entity alias, e.g. e.event_id
    Take those aliases and put them in the table name. This has to be done in a post
    process since we need to have all the aliases from the join clause.
    """

    from_clause = query.get_from_clause()
    if not isinstance(from_clause, JoinClause):
        return  # We don't qualify columns that have a single source

    aliases = set(from_clause.get_alias_node_map().keys())

    def transform(exp: Expression) -> Expression:
        if not isinstance(exp, Column):
            return exp

        parts = exp.column_name.split(".", 1)
        if len(parts) != 2 or parts[0] not in aliases:
            raise ParsingException(
                f"column {exp.column_name} must be qualified in a join query"
            )

        return Column(exp.alias, parts[0], parts[1])

    query.transform_expressions(transform)


DATETIME_MATCH = FunctionCallMatch(
    StringMatch("toDateTime"), (Param("date_string", LiteralMatch(AnyMatch(str))),)
)


def _parse_datetime_literals(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery]
) -> None:
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
    Param("function_name", Or([StringMatch("arrayExists"), StringMatch("arrayAll")])),
    (
        Param("column", ColumnMatch(AnyOptionalString(), AnyMatch(str))),
        Param("op", Or([LiteralMatch(StringMatch(op)) for op in OPERATOR_TO_FUNCTION])),
        Param("value", AnyExpression()),
    ),
)


def _array_join_transformation(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery]
) -> None:
    def parse(exp: Expression) -> Expression:
        result = ARRAY_JOIN_MATCH.match(exp)
        if result:
            function_name = result.string("function_name")
            column = result.expression("column")
            assert isinstance(column, Column)
            op_literal = result.expression("op")
            assert isinstance(op_literal, Literal)
            op = str(op_literal.value)
            value = result.expression("value")

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
                                    (
                                        Argument(None, "x"),
                                        value,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    column,
                ),
            )

        return exp

    query.transform_expressions(parse)


def _transform_array_condition(array_columns: Set[str], exp: Expression) -> Expression:
    if not is_condition(exp) or not isinstance(exp, FunctionCall):
        return exp
    elif len(exp.parameters) < 2:
        return exp

    lhs = exp.parameters[0]
    if not isinstance(lhs, Column):
        return exp

    aliased_name = (
        f"{lhs.table_name + '.' if lhs.table_name is not None else ''}{lhs.column_name}"
    )
    if aliased_name not in array_columns:
        return exp

    function_name = (
        "arrayExists"
        if FUNCTION_TO_OPERATOR[exp.function_name] in POSITIVE_OPERATORS
        else "arrayAll"
    )

    # This is an expression like:
    # arrayExists(x -> assumeNotNull(notLike(x, rhs)), lhs)
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
                            exp.function_name,
                            (Argument(None, "x"), exp.parameters[1]),
                        ),
                    ),
                ),
            ),
            lhs,
        ),
    )


def _unpack_array_conditions(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    schema: EntityColumnSet,
    entity_alias: Optional[str] = None,
) -> None:
    array_columns: Set[str] = set()
    array_join_arguments = query.get_arrayjoin()
    array_join_columns = set()
    if array_join_arguments is not None:
        for array_join_col in array_join_arguments:
            assert isinstance(array_join_col, Column)
            array_join_columns.add(
                f"{array_join_col.table_name + '.' if array_join_col.table_name else ''}{array_join_col.column_name}"
            )

    entity_alias = f"{entity_alias}." if entity_alias is not None else ""
    for column in schema:
        if isinstance(column.type, Array):
            aliased_base_name = f"{entity_alias}{column.base_name}"
            aliased_flattened = f"{entity_alias}{column.flattened}"
            if (
                aliased_base_name in array_join_columns
                or aliased_flattened in array_join_columns
            ):
                continue

            array_columns.add(aliased_base_name)
            array_columns.add(aliased_flattened)

    condition = query.get_condition()
    if condition:
        query.set_ast_condition(
            condition.transform(partial(_transform_array_condition, array_columns))
        )


def _array_column_conditions(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery]
) -> None:
    """
    Find conditions on array columns, and if those columns are not in the array join,
    convert them to the appropriate higher order function.
    """
    from_clause = query.get_from_clause()
    if isinstance(from_clause, (CompositeQuery, LogicalQuery)):
        # It's difficult to know if a subquery is returning an array,
        # so we can't easily detect a condition on that.
        return

    if isinstance(from_clause, QueryEntity):
        _unpack_array_conditions(query, from_clause.schema)
    elif isinstance(from_clause, JoinClause):
        alias_map = from_clause.get_alias_node_map()
        for alias, node in alias_map.items():
            assert isinstance(node.data_source, QueryEntity)  # mypy
            _unpack_array_conditions(query, node.data_source.schema, alias)


def _mangle_query_aliases(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
) -> None:
    """
    If a query has a subquery, the inner query will get its aliases mangled. This is
    a problem because the outer query is using the inner aliases, not the inner
    selected expression values.

    So, we mangle the outer query column names to match the inner query aliases as well.
    There's no way around this since the inner queries are not executed separately from
    the outer queries in Clickhouse, so we only receive one set of results.
    """

    alias_prefix = "_snuba_"

    def mangle_aliases(exp: Expression) -> Expression:
        alias = exp.alias
        if alias is not None:
            return replace(exp, alias=f"{alias_prefix}{alias}")

        return exp

    def mangle_column_value(exp: Expression) -> Expression:
        if not isinstance(exp, Column):
            return exp

        return replace(exp, column_name=f"{alias_prefix}{exp.column_name}")

    query.transform_expressions(mangle_aliases, skip_array_join=True)

    # Check if this query has a subquery. If it does, we need to mangle the column name as well
    # and keep track of what we mangled by updating the mappings in memory.
    if isinstance(query.get_from_clause(), LogicalQuery):
        query.transform_expressions(mangle_column_value)


def validate_identifiers_in_lambda(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery]
) -> None:
    """
    Check to make sure that any identifiers referenced in a lambda were defined in that lambda
    or in an outer lambda.
    """
    identifiers: Set[str] = set()
    unseen_identifiers: Set[str] = set()

    def validate_lambda(exp: Lambda) -> None:
        for p in exp.parameters:
            identifiers.add(p)
            unseen_identifiers.discard(p)

        for inner_exp in exp.transformation:
            if isinstance(inner_exp, Argument) and inner_exp.name not in identifiers:
                unseen_identifiers.add(inner_exp.name)
            elif isinstance(inner_exp, Lambda):
                validate_lambda(inner_exp)

        for p in exp.parameters:
            identifiers.discard(p)

    for exp in query.get_all_expressions():
        if isinstance(exp, Lambda):
            validate_lambda(exp)

    if len(unseen_identifiers) > 0:
        ident_str = ",".join(f"`{u}`" for u in unseen_identifiers)
        raise InvalidExpressionException(f"identifier(s) {ident_str} not defined")


def _replace_time_condition(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery]
) -> None:
    condition = query.get_condition()
    top_level = (
        get_first_level_and_conditions(condition) if condition is not None else []
    )
    max_days, date_align = state.get_configs(
        [("max_days", None), ("date_align_seconds", 1)]
    )
    assert isinstance(date_align, int)
    if max_days is not None:
        max_days = int(max_days)

    if isinstance(query, LogicalQuery):
        new_top_level = _align_max_days_date_align(
            query.get_from_clause().key, top_level, max_days, date_align
        )
        query.set_ast_condition(combine_and_conditions(new_top_level))
    else:
        from_clause = query.get_from_clause()
        if not isinstance(from_clause, JoinClause):
            return

        alias_map = from_clause.get_alias_node_map()
        for alias, node in alias_map.items():
            assert isinstance(node.data_source, QueryEntity)  # mypy
            new_top_level = _align_max_days_date_align(
                node.data_source.key, top_level, max_days, date_align, alias
            )
            top_level = new_top_level
            query.set_ast_condition(combine_and_conditions(new_top_level))


def _align_max_days_date_align(
    key: EntityKey,
    old_top_level: Sequence[Expression],
    max_days: Optional[int],
    date_align: int,
    alias: Optional[str] = None,
) -> Sequence[Expression]:
    entity = get_entity(key)
    if not entity.required_time_column:
        return old_top_level

    # If there is an = or IN condition on time, we don't need to do any of this
    match = build_match(
        col=entity.required_time_column,
        ops=[ConditionFunctions.EQ],
        param_type=datetime,
        alias=alias,
    )
    if any(match.match(cond) for cond in old_top_level):
        return old_top_level

    lower, upper = get_time_range_expressions(
        old_top_level, entity.required_time_column, alias
    )
    if not lower:
        raise ParsingException(
            f"Missing >= condition with a datetime literal on column {entity.required_time_column} for entity {key.value}. "
            f"Example: {entity.required_time_column} >= toDateTime('2023-05-16 00:00')"
        )
    elif not upper:
        raise ParsingException(
            f"Missing < condition with a datetime literal on column {entity.required_time_column} for entity {key.value}. "
            f"Example: {entity.required_time_column} < toDateTime('2023-05-16 00:00')"
        )

    from_date, from_exp = lower
    to_date, to_exp = upper

    from_date = from_date - timedelta(
        seconds=(from_date - from_date.min).seconds % date_align
    )
    to_date = to_date - timedelta(seconds=(to_date - to_date.min).seconds % date_align)
    if from_date > to_date:
        raise ParsingException(
            f"invalid time conditions on entity {key.value}", should_report=False
        )

    if max_days is not None and (to_date - from_date).days > max_days:
        from_date = to_date - timedelta(days=max_days)

    def replace_cond(exp: Expression) -> Expression:
        if not isinstance(exp, FunctionCall):
            return exp
        elif exp == from_exp:
            return replace(
                exp,
                parameters=(from_exp.parameters[0], Literal(None, from_date)),
            )
        elif exp == to_exp:
            return replace(
                exp, parameters=(to_exp.parameters[0], Literal(None, to_date))
            )

        return exp

    return list(map(replace_cond, old_top_level))


def validate_entities_with_query(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery]
) -> None:
    if isinstance(query, LogicalQuery):
        entity = get_entity(query.get_from_clause().key)
        try:
            for v in entity.get_validators():
                v.validate(query)
        except InvalidQueryException as e:
            raise ParsingException(
                f"validation failed for entity {query.get_from_clause().key.value}: {e}",
                should_report=e.should_report,
            )
    else:
        from_clause = query.get_from_clause()
        if isinstance(from_clause, JoinClause):
            alias_map = from_clause.get_alias_node_map()
            for alias, node in alias_map.items():
                assert isinstance(node.data_source, QueryEntity)  # mypy
                entity = get_entity(node.data_source.key)
                try:
                    for v in entity.get_validators():
                        v.validate(query, alias)
                except InvalidQueryException as e:
                    raise ParsingException(
                        f"validation failed for entity {node.data_source.key.value}: {e}",
                        should_report=e.should_report,
                    )


def _select_entity_for_dataset(
    dataset: Dataset,
) -> Callable[[Union[CompositeQuery[QueryEntity], LogicalQuery]], None]:
    def selector(query: Union[CompositeQuery[QueryEntity], LogicalQuery]) -> None:
        # If you are doing a JOIN, then you have to specify the entity
        if isinstance(query, CompositeQuery):
            return

        if get_dataset_name(dataset) == "discover":
            query_entity = query.get_from_clause()
            # The legacy -> snql parser will mark queries with no entity specified as the "discover" entity
            # so only do this selection in that case. If someone wants the "discover" entity specifically
            # then their query will have to only use fields from that entity.
            if query_entity.key == EntityKey.DISCOVER:
                selected_entity_key = select_discover_entity(query)
                selected_entity = get_entity(selected_entity_key)
                query_entity = QueryEntity(
                    selected_entity_key, selected_entity.get_data_model()
                )
                query.set_from_clause(query_entity)

                # XXX: This exists only to ensure that the generated SQL matches legacy queries.
                def replace_time_condition_aliases(exp: Expression) -> Expression:
                    if (
                        isinstance(exp, FunctionCall)
                        and len(exp.parameters) == 2
                        and isinstance(exp.parameters[0], Column)
                        and exp.parameters[0].alias == "_snuba_timestamp"
                    ):
                        return FunctionCall(
                            exp.alias,
                            exp.function_name,
                            (
                                Column(
                                    f"_snuba_{selected_entity.required_time_column}",
                                    exp.parameters[0].table_name,
                                    exp.parameters[0].column_name,
                                ),
                                exp.parameters[1],
                            ),
                        )

                    return exp

                condition = query.get_condition()
                if condition is not None:
                    query.set_ast_condition(
                        condition.transform(replace_time_condition_aliases)
                    )

    return selector


def _post_process(
    query: Union[CompositeQuery[QueryEntity], LogicalQuery],
    funcs: Sequence[Callable[[Union[CompositeQuery[QueryEntity], LogicalQuery]], None]],
    settings: QuerySettings | None = None,
) -> None:
    for func in funcs:
        # custom processors can be partials instead of functions but partials don't
        # have the __name__ attribute set automatically (and we don't set it manually)
        description = getattr(func, "__name__", "custom")

        with sentry_sdk.start_span(op="processor", description=description):
            if settings and settings.get_dry_run():
                with explain_meta.with_query_differ("snql_parsing", description, query):
                    func(query)
            else:
                func(query)

    if isinstance(query, CompositeQuery):
        from_clause = query.get_from_clause()
        if isinstance(from_clause, (LogicalQuery, CompositeQuery)):
            _post_process(from_clause, funcs, settings)
            query.set_from_clause(from_clause)


POST_PROCESSORS = [
    _parse_datetime_literals,
    validate_aliases,
    parse_subscriptables,  # -> This should be part of the grammar
    apply_column_aliases,
    expand_aliases,
    _mangle_query_aliases,
    _array_join_transformation,
    _qualify_columns,
    _array_column_conditions,
]

VALIDATORS = [
    validate_identifiers_in_lambda,
    validate_query,
    validate_entities_with_query,
]


CustomProcessors = Sequence[
    Callable[[Union[CompositeQuery[QueryEntity], LogicalQuery]], None]
]


def parse_snql_query(
    body: str,
    dataset: Dataset,
    custom_processing: Optional[CustomProcessors] = None,
    settings: QuerySettings | None = None,
) -> Tuple[Union[CompositeQuery[QueryEntity], LogicalQuery], str]:
    with sentry_sdk.start_span(op="parser", description="parse_snql_query_initial"):
        query = parse_snql_query_initial(body)

    if settings and settings.get_dry_run():
        explain_meta.set_original_ast(str(query))

    with sentry_sdk.start_span(op="parser", description="anonymize_snql_query"):
        snql_anonymized = format_snql_anonymized(query).get_sql()

    with sentry_sdk.start_span(op="processor", description="post_processors"):
        _post_process(
            query,
            POST_PROCESSORS,
            settings,
        )

    # Custom processing to tweak the AST before validation
    with sentry_sdk.start_span(op="processor", description="custom_processing"):
        if custom_processing is not None:
            _post_process(query, custom_processing, settings)

    # Time based processing
    with sentry_sdk.start_span(op="processor", description="time_based_processing"):
        _post_process(query, [_replace_time_condition], settings)

    # XXX: Select the entity to be used for the query. This step is temporary. Eventually
    # entity selection will be moved to Sentry and specified for all SnQL queries.
    _post_process(query, [_select_entity_for_dataset(dataset)], settings)

    # Validating
    with sentry_sdk.start_span(op="validate", description="expression_validators"):
        _post_process(query, VALIDATORS)
    return query, snql_anonymized
