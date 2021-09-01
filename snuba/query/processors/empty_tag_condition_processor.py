import random

from snuba import settings
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_MAPPING_PARAM,
    KEY_MAPPING_PARAM,
    TABLE_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.conditions import ConditionFunctions, condition_pattern
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.matchers import FunctionCall as FunctionCallPattern
from snuba.query.matchers import Literal as LiteralPattern
from snuba.query.matchers import String
from snuba.request.request_settings import RequestSettings

CONDITION_PATTERN = condition_pattern(
    {ConditionFunctions.EQ, ConditionFunctions.NEQ},
    FunctionCallPattern(
        String("ifNull"), (mapping_pattern, LiteralPattern(String(""))),
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
                key_column = result.optional_string(KEY_COL_MAPPING_PARAM)
                if key_column == "tags.key":
                    rhs = result.optional_string(KEY_MAPPING_PARAM)
                    table_name = result.optional_string(TABLE_MAPPING_PARAM)
                    replacement = FunctionCall(
                        exp.alias,
                        "has",
                        (Column(None, table_name, "tags.key"), Literal(None, rhs)),
                    )

                    assert isinstance(exp, FunctionCall)
                    if exp.function_name == ConditionFunctions.EQ:
                        replacement = FunctionCall(exp.alias, "not", (replacement,))

                    return replacement

            return exp

        condition = query.get_condition()
        if condition is not None:
            query.set_ast_condition(condition.transform(process_condition))
