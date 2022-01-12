"""This is a hacky query processor put in for the purpose of
mitigating clickhouse crashes. When a query is sent to clickhouse
which has a `uniq` in the `HAVING` clause but not in the `SELECT`
clause, the query node will crash. This may be fixed with future versions
of clickhouse but we have not upgraded yet. This exists as a check so that we
return an exception to the user but don't crash the clickhouse query node
"""

import logging
from dataclasses import fields
from typing import Any, Dict, List, Sequence, cast

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Expression, FunctionCall, NoopVisitor
from snuba.request.request_settings import RequestSettings
from snuba.state import get_config


class MismatchedAggregationException(InvalidQueryException):
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


def _expressions_equal_ignore_aliases(exp1: Expression, exp2: Expression) -> bool:
    if type(exp1) != type(exp2):
        return False
    fields_equal = True
    for field in fields(exp1):
        if field.name == "alias":
            continue
        field_val1 = getattr(exp1, field.name)
        field_val2 = getattr(exp2, field.name)
        if isinstance(field_val1, Expression):
            fields_equal &= _expressions_equal_ignore_aliases(field_val1, field_val2)
        else:
            fields_equal &= field_val1 == field_val2
        if not fields_equal:
            return False
    return fields_equal


class _ExpressionOrAliasMatcher(NoopVisitor):
    def __init__(self, expressions_to_match: Sequence[FunctionCall]):
        self.expressions_to_match = expressions_to_match
        self.found_expressions = [False] * len(expressions_to_match)

    def visit_function_call(self, exp: FunctionCall) -> None:
        for param in exp.parameters:
            param.accept(self)
        for i, exp_to_match in enumerate(self.expressions_to_match):
            if (
                exp_to_match.function_name == exp.function_name
                and _expressions_equal_ignore_aliases(exp_to_match, exp)
            ):
                self.found_expressions[i] = True


class UniqInSelectAndHavingProcessor(QueryProcessor):
    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        having_clause = query.get_having()
        if not having_clause:
            return None
        selected_columns = query.get_selected_columns()
        uniq_finder = _FunctionNameFindingVisitor("uniq")
        having_clause.accept(uniq_finder)
        if uniq_finder.found_functions:
            matcher = _ExpressionOrAliasMatcher(uniq_finder.found_functions)
            for col in selected_columns:
                col.expression.accept(matcher)
            if not all(matcher.found_expressions):
                should_throw = get_config("throw_on_uniq_select_and_having", False)
                error = MismatchedAggregationException(
                    "Aggregation is in HAVING clause but not SELECT", query=str(query)
                )
                if should_throw:
                    raise error
                else:
                    logging.warning(
                        "Aggregation is in HAVING clause but not SELECT",
                        exc_info=True,
                        extra=cast(Dict[str, Any], error.to_dict()),
                    )
