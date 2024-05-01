from typing import Callable, Sequence

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query import LimitBy, OrderBy, SelectedExpression
from snuba.query.conditions import (
    get_first_level_and_conditions,
    get_first_level_or_conditions,
)
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.matchers import AnyExpression as AnyExpressionMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Pattern
from snuba.query.matchers import String as StringMatch
from snuba.query.matchers import SubscriptableReference as SubscriptableReferenceMatch

MapFn = Callable[[Expression, ExpressionVisitor[str]], str]


and_cond_match = FunctionCallMatch(
    StringMatch("and"),
)


def and_cond_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    conditions = get_first_level_and_conditions(exp)
    parameters = ", ".join([arg.accept(visitor) for arg in conditions])
    return f"and_cond({parameters})"


or_cond_match = FunctionCallMatch(
    StringMatch("or"),
)


def or_cond_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    conditions = get_first_level_or_conditions(exp)
    parameters = ", ".join([arg.accept(visitor) for arg in conditions])
    return f"or_cond({parameters})"


tags_raw_match = SubscriptableReferenceMatch(
    column_name=StringMatch("tags_raw"),
)


def tags_raw_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, SubscriptableReference)
    return f"tags_raw['{exp.key.value}']"


literals_tuple_match = FunctionCallMatch(
    StringMatch("tuple"),
    all_parameters=LiteralMatch(),
)


def literals_tuple_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    parameters = ", ".join([arg.accept(visitor) for arg in exp.parameters])
    return f"literals_tuple({repr(exp.alias)}, [{parameters}])"


literals_array_match = FunctionCallMatch(
    StringMatch("array"),
    all_parameters=LiteralMatch(),
)


def literals_array_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    parameters = ", ".join([arg.accept(visitor) for arg in exp.parameters])
    return f"literals_array({repr(exp.alias)}, [{parameters}])"


array_element_match = FunctionCallMatch(
    StringMatch("arrayElement"),
)


def array_element_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    return f"arrayElement({repr(exp.alias)}, {exp.parameters[0].accept(visitor)}, {exp.parameters[1].accept(visitor)})"


tuple_element_match = FunctionCallMatch(
    StringMatch("tupleElement"),
)


def tuple_element_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    return f"tupleElement({repr(exp.alias)}, {exp.parameters[0].accept(visitor)}, {exp.parameters[1].accept(visitor)})"


array_join_match = FunctionCallMatch(
    StringMatch("arrayJoin"),
)


def array_join_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    return f"arrayJoin({repr(exp.alias)}, {exp.parameters[0].accept(visitor)})"


def binary_function_match(fn_name: str) -> Pattern[Expression]:
    return FunctionCallMatch(
        StringMatch(fn_name),
        parameters=(AnyExpressionMatch(), AnyExpressionMatch()),
    )


def binary_function_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    if exp.function_name in ["plus", "minus", "multiply", "divide"]:
        alias = f", {repr(exp.alias)}" if exp.alias else ""
        return f"{exp.function_name}({exp.parameters[0].accept(visitor)}, {exp.parameters[1].accept(visitor)}{alias})"
    elif exp.function_name in ["greaterOrEquals", "less", "equals"]:
        return f"{exp.function_name}({exp.parameters[0].accept(visitor)}, {exp.parameters[1].accept(visitor)})"
    elif exp.function_name == "in":
        return f"in_cond({exp.parameters[0].accept(visitor)}, {exp.parameters[1].accept(visitor)})"

    return exp.accept(visitor)


class DSLMapperVisitor(ExpressionVisitor[str]):
    def __init__(self) -> None:
        self._mappers: dict[Pattern[Expression], MapFn] = {
            and_cond_match: and_cond_repr,
            or_cond_match: or_cond_repr,
            tags_raw_match: tags_raw_repr,
            literals_tuple_match: literals_tuple_repr,
            literals_array_match: literals_array_repr,
            array_element_match: array_element_repr,
            tuple_element_match: tuple_element_repr,
            array_join_match: array_join_repr,
            binary_function_match("plus"): binary_function_repr,
            binary_function_match("minus"): binary_function_repr,
            binary_function_match("multiply"): binary_function_repr,
            binary_function_match("divide"): binary_function_repr,
            binary_function_match("equals"): binary_function_repr,
            binary_function_match("greaterOrEquals"): binary_function_repr,
            binary_function_match("less"): binary_function_repr,
            binary_function_match("in"): binary_function_repr,
        }

    def ast_repr(self, exp: Expression) -> str | None:
        for matcher, mapper in self._mappers.items():
            if matcher.match(exp):
                return mapper(exp, self)

        return None

    def _str(self, val: str | None) -> str:
        return f'"{val}", ' if val else "None, "

    def _q(self, val: str) -> str:
        return f'"{val}"'

    def visit_literal(self, exp: Literal) -> str:
        res = self.ast_repr(exp)
        al = f", {repr(exp.alias)}" if exp.alias else ""
        # We usually `from datetime import datetime`
        val = repr(exp.value).replace("datetime.", "")
        return res or f"literal({val}{al})"

    def visit_column(self, exp: Column) -> str:
        if res := self.ast_repr(exp):
            return res

        tn = f", {repr(exp.table_name)}" if exp.table_name else ""
        al = f", {repr(exp.alias)}" if exp.alias else ""
        if al and not tn:
            tn = ", None"
        return f"column({repr(exp.column_name)}{tn}{al})"

    def visit_subscriptable_reference(self, exp: SubscriptableReference) -> str:
        if res := self.ast_repr(exp):
            return res

        return f"SubscriptableReference({repr(exp.alias)}, {exp.column.accept(self)}, {exp.key.accept(self)})"

    def visit_function_call(self, exp: FunctionCall) -> str:
        if res := self.ast_repr(exp):
            return res
        parameters = ""
        if exp.parameters:
            parameters = ", ".join([arg.accept(self) for arg in exp.parameters])
            parameters += ","

        return f"f.{exp.function_name}({parameters}alias={repr(exp.alias)})"

    def visit_curried_function_call(self, exp: CurriedFunctionCall) -> str:
        if res := self.ast_repr(exp):
            return res
        internal_function = exp.internal_function.accept(self)
        parameters = ""
        if exp.parameters:
            raw_parameters = ", ".join([arg.accept(self) for arg in exp.parameters])
            if len(exp.parameters) == 1:
                raw_parameters += ","
            parameters = f", ({raw_parameters})"
        return (
            f"CurriedFunctionCall({repr(exp.alias)}, {internal_function}{parameters})"
        )

    def visit_argument(self, exp: Argument) -> str:
        return repr(exp)

    def visit_lambda(self, exp: Lambda) -> str:
        return repr(exp)

    def visit_selected_expression(self, exp: SelectedExpression) -> str:
        return f"SelectedExpression({repr(exp.name)}, {exp.expression.accept(self)})"

    def visit_orderby(self, exp: OrderBy) -> str:
        return f"OrderBy(OrderByDirection.{exp.direction.value}, {exp.expression.accept(self)})"

    def visit_limitby(self, exp: LimitBy) -> str:
        columns = ", ".join([col.accept(self) for col in exp.columns])
        return f"LimitBy({exp.limit}, [{columns}])"


def ast_repr(
    exp: (
        Expression
        | LimitBy
        | Sequence[Expression | SelectedExpression | OrderBy]
        | None
    ),
    visitor: DSLMapperVisitor,
) -> str:
    if not exp:
        return "None"
    elif isinstance(exp, Expression):
        return exp.accept(visitor)
    elif isinstance(exp, LimitBy):
        return visitor.visit_limitby(exp)

    strings = []
    for e in exp:
        if isinstance(e, SelectedExpression):
            strings.append(visitor.visit_selected_expression(e))
        elif isinstance(e, OrderBy):
            strings.append(visitor.visit_orderby(e))
        else:
            strings.append(e.accept(visitor))

    return f"[{', '.join(strings)}]"


def query_repr(query: LogicalQuery | ClickhouseQuery) -> str:
    visitor = DSLMapperVisitor()
    selected = ast_repr(query.get_selected_columns(), visitor)
    arrayjoin = ast_repr(query.get_arrayjoin(), visitor)
    condition = ast_repr(query.get_condition(), visitor)
    groupby = ast_repr(query.get_groupby(), visitor)

    return f"""Query(
        from_clause=from_clause,
        selected_columns={selected},
        array_join={arrayjoin},
        condition={condition},
        groupby={groupby},
        having={ast_repr(query.get_having(), visitor)},
        order_by={ast_repr(query.get_orderby(), visitor)},
        limitby={ast_repr(query.get_limitby(), visitor)},
        limit={repr(query.get_limit())},
        offset={repr(query.get_offset())},
        totals={repr(query.has_totals())},
        granularity={repr(query.get_granularity())},
    )"""
