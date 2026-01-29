from snuba.clickhouse.query import Query
from snuba.query.conditions import ConditionFunctions, build_match
from snuba.query.expressions import Expression, Literal, SubscriptableReference
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class BooleanTagConditionProcessor(ClickhouseQueryProcessor):
    """
    Converts boolean integer literals (0 and 1) to string literals ("0" and "1")
    when used in tag conditions. This provides a defensive fix for queries that
    incorrectly use integers in tag comparisons (e.g., `tags[stack.in_app] = 1`).

    The root cause is typically that Sentry treats boolean fields like `stack.in_app`
    as tags instead of proper columns, converting boolean values to integers (0 or 1).
    Snuba's TagConditionValidator enforces string-only tag values, so we convert
    these integers to strings to avoid validation errors.

    This processor should run before TagConditionValidator.
    """

    def __init__(self, column_name: str) -> None:
        self.column_name = column_name
        # Build matchers for tag conditions with various operators
        self.condition_matchers = [
            build_match(
                subscriptable=column_name,
                ops=[op],
            )
            for op in [
                ConditionFunctions.EQ,
                ConditionFunctions.NEQ,
                ConditionFunctions.LTE,
                ConditionFunctions.GTE,
                ConditionFunctions.LT,
                ConditionFunctions.GT,
                ConditionFunctions.LIKE,
                ConditionFunctions.NOT_LIKE,
            ]
        ]
        self.array_condition_matchers = [
            build_match(
                subscriptable=column_name,
                array_ops=[op],
            )
            for op in [ConditionFunctions.IN, ConditionFunctions.NOT_IN]
        ]

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def process_condition(exp: Expression) -> Expression:
            # Check if this is a tag condition
            for matcher in self.condition_matchers:
                result = matcher.match(exp)
                if result:
                    column = result.expression("column")
                    if isinstance(column, SubscriptableReference):
                        rhs = result.expression("rhs")
                        if isinstance(rhs, Literal) and isinstance(rhs.value, int):
                            # Convert boolean integers (0 or 1) to strings
                            if rhs.value in (0, 1):
                                return exp.transform(
                                    lambda e: Literal(e.alias, str(e.value))
                                    if isinstance(e, Literal)
                                    and isinstance(e.value, int)
                                    and e.value in (0, 1)
                                    else e
                                )
                    return exp

            # Check array conditions (IN, NOT IN)
            for matcher in self.array_condition_matchers:
                result = matcher.match(exp)
                if result:
                    column = result.expression("column")
                    if isinstance(column, SubscriptableReference):
                        # Transform the expression to convert any 0 or 1 literals to strings
                        return exp.transform(
                            lambda e: Literal(e.alias, str(e.value))
                            if isinstance(e, Literal)
                            and isinstance(e.value, int)
                            and e.value in (0, 1)
                            else e
                        )
            return exp

        condition = query.get_condition()
        if condition is not None:
            query.set_ast_condition(condition.transform(process_condition))
