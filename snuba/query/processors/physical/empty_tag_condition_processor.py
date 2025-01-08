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
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings

CONDITION_PATTERN = condition_pattern(
    {ConditionFunctions.EQ, ConditionFunctions.NEQ},
    FunctionCallPattern(
        String("ifNull"),
        (mapping_pattern, LiteralPattern(String(""))),
    ),
    LiteralPattern(String("")),
    commutative=False,
)


class EmptyTagConditionProcessor(ClickhouseQueryProcessor):
    """
    If queries have conditions of the form `ifNull(tags[key], '') =/!= ''` we can simplify this using
    the `has` function.
    """

    def __init__(self, column_name: str) -> None:
        self.column_name = column_name

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def process_condition(exp: Expression) -> Expression:
            result = CONDITION_PATTERN.match(exp)
            if result is not None:
                key_column = result.optional_string(KEY_COL_MAPPING_PARAM)
                if key_column == self.column_name:
                    rhs = result.optional_string(KEY_MAPPING_PARAM)
                    table_name = result.optional_string(TABLE_MAPPING_PARAM)
                    replacement = FunctionCall(
                        exp.alias,
                        "has",
                        (
                            Column(None, table_name, self.column_name),
                            Literal(None, rhs),
                        ),
                    )

                    assert isinstance(exp, FunctionCall)
                    if exp.function_name == ConditionFunctions.EQ:
                        replacement = FunctionCall(exp.alias, "not", (replacement,))

                    return replacement

            return exp

        condition = query.get_condition()
        if condition is not None:
            query.set_ast_condition(condition.transform(process_condition))
