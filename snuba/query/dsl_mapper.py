from typing import Callable, Sequence

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query import LimitBy, OrderBy, SelectedExpression
from snuba.query.composite import CompositeQuery
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import (
    Argument,
    Column,
    CurriedFunctionCall,
    DangerousRawSQL,
    Expression,
    ExpressionVisitor,
    FunctionCall,
    JsonPath,
    Lambda,
    Literal,
    SubscriptableReference,
)
from snuba.query.logical import Query as LogicalQuery
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Pattern
from snuba.query.matchers import String as StringMatch
from snuba.query.matchers import SubscriptableReference as SubscriptableReferenceMatch

MapFn = Callable[[Expression, ExpressionVisitor[str]], str]


and_cond_match = FunctionCallMatch(
    StringMatch("and"),
)


def and_cond_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    parameters = ", ".join([arg.accept(visitor) for arg in exp.parameters])
    return f"and_cond({parameters})"


or_cond_match = FunctionCallMatch(
    StringMatch("or"),
)


def or_cond_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    parameters = ", ".join([arg.accept(visitor) for arg in exp.parameters])
    return f"or_cond({parameters})"


in_cond_match = FunctionCallMatch(
    StringMatch("in"),
)


def in_cond_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, FunctionCall)
    parameters = ", ".join([arg.accept(visitor) for arg in exp.parameters])
    return f"in_cond({parameters})"


tags_raw_match = SubscriptableReferenceMatch(
    column_name=StringMatch("tags_raw"),
)


def tags_raw_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, SubscriptableReference)
    return f"tags_raw['{exp.key.value}']"


tags_match = SubscriptableReferenceMatch(
    column_name=StringMatch("tags"),
)


def tags_repr(exp: Expression, visitor: ExpressionVisitor[str]) -> str:
    assert isinstance(exp, SubscriptableReference)
    return f"tags['{exp.key.value}']"


class DSLMapperVisitor(ExpressionVisitor[str]):
    """
    This visitor is meant to take an AST object, and output a string representation of that object,
    that is valid Python code using the DSL classes defined in snuba.query.dsl.

    E.g. Literal(None, 1) -> "literal(1)"

    These code snippets can then be pasted into tests to replace the raw AST objects, and make the
    tests easier to read and edit.

    In order for the strings to work, there are a couple assumptions being made. Notably that the
    dsl.Functions class is being imported, and that NestedColumn objects are created for tags and
    tags_raw.

    ```python
    from snuba.query.dsl import (
        Functions as f,
        NestedColumn,
    )

    tags = NestedColumn("tags")
    tags_raw = NestedColumn("tags_raw")
    ```

    There are helper functions wrapping this visitor:
    `ast_repr` takes an Expression, LimitBy or SelectedExpression, or a sequence of any of those,
    and returns the correct string representation.
    `query_repr` takes a LogicalQuery or ClickhouseQuery object, and returns a string representation
    of the entire query, converting each clause to a dsl string.

    """

    def __init__(self) -> None:
        self._mappers: dict[Pattern[Expression], MapFn] = {
            and_cond_match: and_cond_repr,
            or_cond_match: or_cond_repr,
            in_cond_match: in_cond_repr,
            tags_raw_match: tags_raw_repr,
            tags_match: tags_repr,
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

        alias = f"alias={repr(exp.alias)}" if exp.alias else ""
        alias = f", {alias}" if (parameters and alias) else alias
        return f"f.{exp.function_name}({parameters}{alias})"

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
        return f"CurriedFunctionCall({repr(exp.alias)}, {internal_function}{parameters})"

    def visit_argument(self, exp: Argument) -> str:
        return repr(exp)

    def visit_lambda(self, exp: Lambda) -> str:
        return repr(exp)

    def visit_dangerous_raw_sql(self, exp: DangerousRawSQL) -> str:
        alias_str = f", {repr(exp.alias)}" if exp.alias else ", None"
        return f"DangerousRawSQL({alias_str}, {repr(exp.sql)})"

    def visit_json_path(self, exp: JsonPath) -> str:
        alias_str = repr(exp.alias)
        base_str = exp.base.accept(self)
        type_str = f", {repr(exp.return_type)}" if exp.return_type else ""
        return f"JsonPath({alias_str}, {base_str}, {repr(exp.path)}{type_str})"

    def visit_selected_expression(self, exp: SelectedExpression) -> str:
        return f"SelectedExpression({repr(exp.name)}, {exp.expression.accept(self)})"

    def visit_orderby(self, exp: OrderBy) -> str:
        return f"OrderBy(OrderByDirection.{exp.direction.value}, {exp.expression.accept(self)})"

    def visit_limitby(self, exp: LimitBy) -> str:
        columns = ", ".join([col.accept(self) for col in exp.columns])
        return f"LimitBy({exp.limit}, [{columns}])"


def ast_repr(
    exp: Expression | LimitBy | Sequence[Expression | SelectedExpression | OrderBy] | None,
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


def query_repr(query: LogicalQuery | ClickhouseQuery | CompositeQuery[Entity]) -> str:
    visitor = DSLMapperVisitor()
    selected = ast_repr(query.get_selected_columns(), visitor)
    arrayjoin = ast_repr(query.get_arrayjoin(), visitor)
    condition = ast_repr(query.get_condition(), visitor)
    groupby = ast_repr(query.get_groupby(), visitor)

    qfrom = query.get_from_clause()
    if isinstance(qfrom, Entity):
        key = qfrom.key
        from_clause = f"Entity({key},get_entity({key}).get_data_model())"
    else:
        from_clause = "from_clause"

    return f"""Query(
        from_clause={from_clause},
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
