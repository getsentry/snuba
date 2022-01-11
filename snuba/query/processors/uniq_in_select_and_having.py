from typing import List, Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Column, FunctionCall, NoopVisitor
from snuba.request.request_settings import RequestSettings
from snuba.utils.serializable_exception import SerializableException


class MismatchedAggregationException(SerializableException):
    pass


class _FunctionNameFindingVisitor(NoopVisitor):
    def __init__(self, function_name_to_find: str):
        self.function_name_to_find = function_name_to_find
        self.found_functions: List[FunctionCall] = []

    def visit_function_call(self, exp: FunctionCall) -> None:
        for param in exp.parameters:
            param.accept(self)
        if exp.function_name == self.function_name_to_find:
            self.found_functions.append(exp)
        return None


class _ExpressionOrAliasMatcher(NoopVisitor):
    def __init__(self, expressions_to_match: Sequence[FunctionCall]):
        self.expressions_to_match = expressions_to_match
        self.found_expressions = [False] * len(expressions_to_match)

    def visit_column(self, exp: Column) -> None:
        for i, exp_to_match in enumerate(self.expressions_to_match):
            if exp_to_match.alias is not None and exp_to_match.alias == exp.column_name:
                self.found_expressions[i] = True

    def visit_function_call(self, exp: FunctionCall) -> None:
        for param in exp.parameters:
            param.accept(self)
        for i, exp_to_match in enumerate(self.expressions_to_match):
            if exp_to_match.function_name == exp.function_name and exp_to_match == exp:
                self.found_expressions[i] = True


class UniqInSelectAndHavingProcessor(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        having_clause = query.get_having()
        if not having_clause:
            return None
        selected_columns = query.get_selected_columns()
        visitor = _FunctionNameFindingVisitor("uniq")
        having_clause.accept(visitor)
        if visitor.found_functions:
            matcher = _ExpressionOrAliasMatcher(visitor.found_functions)
            for col in selected_columns:
                col.expression.accept(matcher)
            if not all(matcher.found_expressions):
                raise MismatchedAggregationException(
                    f"Aggregation is in HAVING clause but not SELECT: {visitor.found_functions}"
                )
