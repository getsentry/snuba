from datetime import datetime
from typing import Optional, Set, Tuple, cast

from snuba.clickhouse.query import Query
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    ConditionFunctions,
    get_first_level_and_conditions,
    is_in_condition_pattern,
)
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import (
    Any,
    AnyExpression,
    Column,
    FunctionCall,
    Literal,
    Or,
    Param,
    String,
)


def get_project_ids_in_query_ast(
    query: Query, project_column: str
) -> Optional[Set[int]]:
    """
    Finds the project ids this query is filtering according to the AST
    query representation.

    It works like get_project_ids_in_query with the exception that
    boolean functions are supported here.
    """

    def get_project_ids_in_condition(condition: Expression) -> Optional[Set[int]]:
        """
        Extract project ids from an expression. Returns None if no project
        if condition is found. It returns an empty set of conflicting project_id
        conditions are found.
        """
        match = FunctionCall(
            String(ConditionFunctions.EQ),
            (
                Column(column_name=String(project_column)),
                Literal(value=Param("project_id", Any(int))),
            ),
        ).match(condition)
        if match is not None:
            return {match.integer("project_id")}

        match = is_in_condition_pattern(
            Column(column_name=String(project_column))
        ).match(condition)
        if match is not None:
            projects = match.expression("tuple")
            assert isinstance(projects, FunctionCallExpr)
            return {
                lit.value
                for lit in projects.parameters
                if isinstance(lit, LiteralExpr) and isinstance(lit.value, int)
            }

        match = FunctionCall(
            Param(
                "operator",
                Or([String(BooleanFunctions.AND), String(BooleanFunctions.OR)]),
            ),
            (Param("lhs", AnyExpression()), Param("rhs", AnyExpression())),
        ).match(condition)
        if match is not None:
            lhs_projects = get_project_ids_in_condition(match.expression("lhs"))
            rhs_projects = get_project_ids_in_condition(match.expression("rhs"))
            if lhs_projects is None:
                return rhs_projects
            elif rhs_projects is None:
                return lhs_projects
            else:
                return (
                    lhs_projects & rhs_projects
                    if match.string("operator") == BooleanFunctions.AND
                    else lhs_projects | rhs_projects
                )

        return None

    condition = query.get_condition_from_ast()
    return get_project_ids_in_condition(condition) if condition is not None else None


def get_time_range(
    query: Query, timestamp_field: str
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Finds the minimal time range for this query. Which means, it finds
    the >= timestamp condition with the highest datetime literal and
    the < timestamp condition with the smallest and returns the interval
    in the form of a tuple of Literals. It only looks into first level
    AND conditions since, if the timestamp is nested in an OR we cannot
    say anything on how that compares to the other timestamp conditions.
    """

    condition_clause = query.get_condition_from_ast()
    if not condition_clause:
        return (None, None)

    max_lower_bound = None
    min_upper_bound = None
    for c in get_first_level_and_conditions(condition_clause):
        match = FunctionCall(
            Param(
                "operator",
                Or(
                    [
                        String(OPERATOR_TO_FUNCTION[">="]),
                        String(OPERATOR_TO_FUNCTION["<"]),
                    ]
                ),
            ),
            (
                Column(None, String(timestamp_field)),
                Literal(Param("timestamp", Any(datetime))),
            ),
        ).match(c)

        if match is not None:
            timestamp = cast(datetime, match.scalar("timestamp"))
            if match.string("operator") == OPERATOR_TO_FUNCTION[">="]:
                if not max_lower_bound or timestamp > max_lower_bound:
                    max_lower_bound = timestamp
            else:
                if not min_upper_bound or timestamp < min_upper_bound:
                    min_upper_bound = timestamp

    return (max_lower_bound, min_upper_bound)
