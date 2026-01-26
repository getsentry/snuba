"""This is a hacky query processor put in for the purpose of
mitigating clickhouse crashes. When a query is sent to clickhouse
which has a `uniq` in the `HAVING` clause but not in the `SELECT`
clause, the query node will crash. This may be fixed with future versions
of clickhouse but we have not upgraded yet. This exists as a check so that we
return an exception to the user but don't crash the clickhouse query node
"""

import logging
from typing import Any, Dict, Sequence, cast

from snuba.clickhouse.query import Query
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Expression, FunctionCall, NoopVisitor
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Param, String
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import get_config


class MismatchedAggregationException(InvalidQueryException):
    pass


class _ExpressionOrAliasMatcher(NoopVisitor):
    def __init__(self, expressions_to_match: Sequence[Expression]):
        self.expressions_to_match = expressions_to_match
        self.found_expressions = [False] * len(expressions_to_match)

    def visit_function_call(self, exp: FunctionCall) -> None:
        for param in exp.parameters:
            param.accept(self)
        for i, exp_to_match in enumerate(self.expressions_to_match):
            if isinstance(exp_to_match, FunctionCall) and exp_to_match.functional_eq(exp):
                self.found_expressions[i] = True


class UniqInSelectAndHavingProcessor(ClickhouseQueryProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        having_clause = query.get_having()
        if not having_clause:
            return None
        selected_columns = query.get_selected_columns()
        uniq_matcher = Param("function", FunctionCallMatch(String("uniq")))
        found_functions = []
        for exp in having_clause:
            match = uniq_matcher.match(exp)
            if match is not None:
                found_functions.append(match.expression("function"))
        if found_functions is not None:
            matcher = _ExpressionOrAliasMatcher(found_functions)
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
