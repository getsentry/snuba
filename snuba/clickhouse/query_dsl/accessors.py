from datetime import datetime
from typing import Optional, Sequence, Set, Tuple, Union, cast

from snuba.query import ProcessableQuery
from snuba.query import Query as AbstractQuery
from snuba.query.conditions import (
    OPERATOR_TO_FUNCTION,
    BooleanFunctions,
    ConditionFunctions,
    get_first_level_and_conditions,
    is_in_condition_pattern,
)
from snuba.query.data_source.simple import Entity, SimpleDataSource, Table
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


def get_object_ids_in_condition(condition: Expression, object_column: str) -> Set[int]:
    """
    Extract project ids from an expression. Returns None if no project
    if condition is found. It returns an empty set of conflicting project_id
    conditions are found.
    """
    match = FunctionCall(
        String(ConditionFunctions.EQ),
        (
            Column(column_name=String(object_column)),
            Literal(value=Param("object_id", Any(int))),
        ),
    ).match(condition)
    if match is not None:
        return {match.integer("object_id")}

    match = is_in_condition_pattern(Column(column_name=String(object_column))).match(
        condition
    )
    if match is not None:
        objects = match.expression("sequence")
        assert isinstance(objects, FunctionCallExpr)
        return {
            lit.value
            for lit in objects.parameters
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
        lhs_objects = get_object_ids_in_condition(
            match.expression("lhs"), object_column
        )
        rhs_objects = get_object_ids_in_condition(
            match.expression("rhs"), object_column
        )
        if lhs_objects is None:
            return rhs_objects
        elif rhs_objects is None:
            return lhs_objects
        else:
            return (
                lhs_objects & rhs_objects
                if match.string("operator") == BooleanFunctions.AND
                else lhs_objects | rhs_objects
            )

    return set()


def get_object_ids_in_query_ast(query: AbstractQuery, object_column: str) -> Set[int]:
    """
    Finds the object ids (e.g. project ids) this query is filtering according to the AST
    query representation.

    It works like get_project_ids_in_query with the exception that
    boolean functions are supported here.
    """

    condition = query.get_condition()
    if not condition:
        return set()
    this_query_object_ids = get_object_ids_in_condition(condition, object_column)

    from_clause = query.get_from_clause()
    if isinstance(from_clause, SimpleDataSource):
        return this_query_object_ids

    elif isinstance(from_clause, AbstractQuery):
        subquery_project_ids = get_object_ids_in_query_ast(from_clause, object_column)
        return subquery_project_ids.union(this_query_object_ids)
    return set()


def get_time_range_expressions(
    conditions: Sequence[Expression],
    timestamp_field: str,
    table_name: Optional[str] = None,
) -> Tuple[
    Optional[Tuple[datetime, FunctionCallExpr]],
    Optional[Tuple[datetime, FunctionCallExpr]],
]:
    max_lower_bound: Optional[Tuple[datetime, FunctionCallExpr]] = None
    min_upper_bound: Optional[Tuple[datetime, FunctionCallExpr]] = None
    table_match = String(table_name) if table_name else None
    for c in conditions:
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
                Column(table_match, String(timestamp_field)),
                Literal(Param("timestamp", Any(datetime))),
            ),
        ).match(c)

        if match is not None:
            timestamp = cast(datetime, match.scalar("timestamp"))
            assert isinstance(c, FunctionCallExpr)
            if match.string("operator") == OPERATOR_TO_FUNCTION[">="]:
                if not max_lower_bound or timestamp > max_lower_bound[0]:
                    max_lower_bound = (timestamp, c)
            else:
                if not min_upper_bound or timestamp < min_upper_bound[0]:
                    min_upper_bound = (timestamp, c)

    return (max_lower_bound, min_upper_bound)


def get_time_range(
    query: Union[ProcessableQuery[Table], ProcessableQuery[Entity]],
    timestamp_field: str,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Finds the minimal time range for this query. Which means, it finds
    the >= timestamp condition with the highest datetime literal and
    the < timestamp condition with the smallest and returns the interval
    in the form of a tuple of Literals. It only looks into first level
    AND conditions since, if the timestamp is nested in an OR we cannot
    say anything on how that compares to the other timestamp conditions.
    """

    condition_clause = query.get_condition()
    if not condition_clause:
        return (None, None)

    lower, upper = get_time_range_expressions(
        get_first_level_and_conditions(condition_clause), timestamp_field
    )
    lower_bound = lower[0] if lower else None
    upper_bound = upper[0] if upper else None
    return lower_bound, upper_bound


def get_time_range_estimate(
    query: ProcessableQuery[Table],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Best guess to find the time range for the query.
    We pick the first column that is compared with a datetime Literal.
    """
    pattern = FunctionCall(
        Or([String(ConditionFunctions.GT), String(ConditionFunctions.GTE)]),
        (Column(None, Param("col_name", Any(str))), Literal(Any(datetime))),
    )

    from_date, to_date = None, None
    condition = query.get_condition()
    if condition is None:
        return None, None
    for exp in condition:
        result = pattern.match(exp)
        if result is not None:
            from_date, to_date = get_time_range(query, result.string("col_name"))
            break

    return from_date, to_date
