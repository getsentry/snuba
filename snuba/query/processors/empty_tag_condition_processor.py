import random

from snuba import settings
from snuba.query.conditions import ConditionFunctions, condition_pattern
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    SubscriptableReference,
)
from snuba.query.logical import Query
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import Param, String
from snuba.query.matchers import SubscriptableReference as SubscriptableReferencePattern
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings

CONDITION_PATTERN = condition_pattern(
    {ConditionFunctions.EQ, ConditionFunctions.NEQ},
    FunctionCallPattern(
        String("ifNull"),
        (
            Param("lhs", SubscriptableReferencePattern(String("tags"))),
            LiteralPattern(String("")),
        ),
    ),
    LiteralPattern(String("")),
    commutative=False,
)


class EmptyTagConditionProcessor(QueryProcessor):
    """
    If queries have conditions of the form `ifNull(tags[key], '') =/!= ''` we can simplify this using
    the `has` function.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_condition(exp: Expression) -> Expression:
            result = CONDITION_PATTERN.match(exp)
            if result is not None:
                assert isinstance(exp, FunctionCall)
                lhs = result.expression("lhs")
                assert isinstance(lhs, SubscriptableReference)
                replacement = FunctionCall(
                    exp.alias,
                    "has",
                    (Column(None, lhs.column.table_name, "tags.key"), lhs.key),
                )

                if exp.function_name == ConditionFunctions.EQ:
                    replacement = FunctionCall(exp.alias, "not", (replacement,))

                if settings.TESTING or random.random() < 0.5:
                    query.add_experiment("empty-string-tag-condition", "true")
                    return replacement
                else:
                    query.add_experiment("empty-string-tag-condition", "false")

            return exp

        condition = query.get_condition()
        if condition is not None:
            query.set_ast_condition(condition.transform(process_condition))
