from typing import Tuple

from snuba.query.expressions import Expression
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_MAPPING_PARAM,
    KEY_MAPPING_PARAM,
    VALUE_COL_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.matchers import (
    FunctionCall,
    String,
    Or,
    Param,
    Literal,
    Any,
    AnyExpression,
)
from snuba.query.conditions import (
    is_binary_condition,
    is_unary_condition,
    set_condition_pattern,
    ConditionFunctions,
)
from snuba.request.request_settings import RequestSettings


class TagsHashMapOptimizer(QueryProcessor):
    def __init__(self, column_name: str, hash_map_name: str) -> None:
        self.__column_name = column_name
        self.__hash_map_name = hash_map_name

        left_hand_side = Or(
            [
                mapping_pattern,
                FunctionCall(
                    alias=None,
                    function_name=String("ifNull"),
                    parameters=(mapping_pattern, Literal(None, String(""))),
                ),
            ]
        )
        self.__optimizable_pattern = Or(
            [
                FunctionCall(
                    alias=None,
                    function_name=String("equals"),
                    parameters=(
                        left_hand_side,
                        Param("right_hand_side", AnyExpression()),
                    ),
                ),
                FunctionCall(
                    alias=None,
                    function_name=String("in"),
                    parameters=(
                        left_hand_side,
                        Param(
                            "right_hand_side",
                            FunctionCall(
                                alias=None,
                                function_name=String("tuple"),
                                parameters=None,
                            ),
                        ),
                    ),
                ),
            ]
        )

    def __classify_conditions(self, condition: Expression) -> Tuple[bool, bool]:
        for c in condition:
            if not (is_binary_condition(c) or is_unary_condition(c)):
                continue

            match = self.__optimizable_pattern.match(c)
            if match is not None:
                ## check list right hand side

        # is it an optimizable condition ?
        # does it contain tags ? -> non optimizable
        # else ignore

    def __replace_with_hash_map(self, condition: Expression) -> Expression:
        pass

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        condition = query.get_condition_from_ast()
        optimizable = False
        if condition is not None:
            non_optimizable, optimizable = self.__classify_conditions(condition)
            if non_optimizable:
                return
        having_cond = query.get_having_from_ast()
        if having_cond is not None:
            non_optimizable, optimizable = self.__classify_conditions(having_cond)
            if non_optimizable:
                return

        if not optimizable:
            return

        if condition is not None:
            query.set_ast_condition(condition.transform(self.__replace_with_hash_map))
        if having_cond is not None:
            query.set_ast_having(having_cond.transform(self.__replace_with_hash_map))
