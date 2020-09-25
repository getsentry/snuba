from datetime import datetime
from functools import reduce
from typing import List, Optional, Sequence, Set, Tuple, cast

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
from snuba.query.types import Condition
from snuba.util import is_condition


def get_project_ids_in_query(query: Query, project_column: str) -> Optional[Set[int]]:
    """
    Finds the project ids this query is filtering according to the legacy query
    representation.

    It returns a set of referenced project_ids if relevant conditions are found or
    None if no project_id condition is found in the query. Empty set means that multiple
    conflicting project id conditions were found.

    This function looks into first level AND conditions and second level OR conditions
    but it cannot support project ids used as function parameters.
    Specific limitations:
    - If a project_id is a parameter of a function that returns the project_id itself
      it is not supported. It would be very hard to support every function without a
      whitelist/blacklist of allowed functions in Snuba queries.
    - boolean functions are not supported. So we do not unpack and/or/not conditions
      expressed as functions. We will be able to do that with the AST.
    - does not exclude projects referenced in NOT conditions.

    We are going to try to lift as many of these limitations as possible. So, please,
    do not rely on them for the correctness of your code.
    """

    def find_project_id_sets(conditions: Sequence[Condition],) -> Sequence[Set[int]]:
        """
        Scans a potentially nested sequence of conditions.
        For each simple condition adds to the output the set of project ids referenced
        by the condition.
        For each nested condition, it assumes it is a union of simple conditions
        (which is the only supported valid case by the Query object) and adds the union of the
        referenced project ids to the output.
        """
        project_id_sets: List[Set[int]] = list()
        for c in conditions:
            if is_condition(c):
                # This is a simple condition. Can extract the project ids directly.
                # Supports these kinds of conditions
                # ["col", "=", 1]
                # ["col", "IN", [1,2,3]]
                # ["col", "IN", (1,2,3)]
                if c[0] == project_column:
                    if c[1] == "=" and isinstance(c[2], int):
                        project_id_sets.append({c[2]})
                    elif c[1] == "IN" and all(
                        isinstance(project, int) for project in c[2]
                    ):
                        project_id_sets.append(set(c[2]))

            elif all(is_condition(second_level) for second_level in c):
                # This is supposed to be a union of simple conditions. Need to union
                # the sets of project ids.
                sets_to_unite = find_project_id_sets(c)
                if sets_to_unite:
                    project_id_sets.append(reduce(lambda x, y: x | y, sets_to_unite))
            else:
                raise ValueError(f"Invalid condition {conditions}")

        return project_id_sets

    all_project_id_sets = find_project_id_sets(query.get_conditions() or [])

    if not all_project_id_sets:
        return None
    return reduce(lambda x, y: x & y, all_project_id_sets)


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
