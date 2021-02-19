from snuba.query.processors import QueryProcessor
from snuba.query.logical import Query
from snuba.query.conditions import (
    ConditionFunctions,
    get_first_level_and_conditions,
)
from snuba.query.expressions import FunctionCall, Literal
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import String as StringMatch
from snuba.query.matchers import Or, Param
from snuba.request.request_settings import RequestSettings
from snuba.state import get_configs
from snuba.state.rate_limit import PROJECT_RATE_LIMIT_NAME, RateLimitParameters


class ProjectRateLimiterProcessor(QueryProcessor):
    """
    If there isn't already a rate limiter on a project, search the top level
    conditions for project IDs using the given project column name and add a
    rate limiter for them.
    """

    def __init__(self, project_column: str) -> None:
        self.project_column = project_column

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # If the settings don't already have a project rate limit, add one
        existing = request_settings.get_rate_limit_params()
        for ex in existing:
            if ex.rate_limit_name == PROJECT_RATE_LIMIT_NAME:
                return

        condition = query.get_condition_from_ast()
        if not condition:
            return

        top_level = get_first_level_and_conditions(condition)
        column_match = ColumnMatch(None, StringMatch(self.project_column))
        match = Or(
            [
                FunctionCallMatch(
                    StringMatch(ConditionFunctions.EQ),
                    (column_match, Param("single", LiteralMatch(AnyMatch(int)))),
                ),
                FunctionCallMatch(
                    StringMatch(ConditionFunctions.IN),
                    (
                        column_match,
                        Param(
                            "multiple",
                            FunctionCallMatch(
                                Or([StringMatch("array"), StringMatch("tuple")]),
                                all_parameters=LiteralMatch(AnyMatch(int)),
                            ),
                        ),
                    ),
                ),
            ]
        )

        project_id = 0  # TODO rate limit on every project in the list?
        for cond in top_level:
            result = match.match(cond)
            if result is not None:
                if result.contains("single"):
                    exp = result.expression("single")
                    if isinstance(exp, Literal) and isinstance(exp.value, int):
                        project_id = exp.value
                        break
                elif result.contains("multiple"):
                    exp = result.expression("multiple")
                    assert isinstance(exp, FunctionCall)
                    found = False
                    for p in exp.parameters:
                        if isinstance(p, Literal) and isinstance(p.value, int):
                            project_id = p.value
                            found = True
                            break

                    if found:
                        break

        prl, pcl = get_configs(
            [("project_per_second_limit", 1000), ("project_concurrent_limit", 1000)]
        )

        # Specific projects can have their rate limits overridden
        (per_second, concurr) = get_configs(
            [
                ("project_per_second_limit_{}".format(project_id), prl),
                ("project_concurrent_limit_{}".format(project_id), pcl),
            ]
        )

        rate_limit = RateLimitParameters(
            rate_limit_name=PROJECT_RATE_LIMIT_NAME,
            bucket=str(project_id),
            per_second_limit=per_second,
            concurrent_limit=concurr,
        )

        request_settings.add_rate_limit(rate_limit)
