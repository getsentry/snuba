from enum import Enum
from typing import Optional, Sequence

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_MAPPING_PARAM,
    KEY_MAPPING_PARAM,
    TABLE_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    get_first_level_and_conditions,
    get_first_level_or_conditions,
)
from snuba.query.expressions import Column, Expression
from snuba.query.expressions import FunctionCall as FunctionExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import Any, FunctionCall, Literal, Or, Param, String
from snuba.request.request_settings import RequestSettings


class ConditionClass(Enum):
    IRRELEVANT = 1
    OPTIMIZABLE = 2
    NOT_OPTIMIZABLE = 3


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
                        Param("right_hand_side", Literal(None, Any(str))),
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

    def __classify_combined_conditions(self, condition: Expression) -> ConditionClass:
        if not isinstance(condition, FunctionExpr):
            return ConditionClass.IRRELEVANT
        if condition.function_name == BooleanFunctions.AND:
            for c in get_first_level_and_conditions(condition):
                return self.__classify_combined_conditions(c)
        elif condition.function_name == BooleanFunctions.OR:
            for c in get_first_level_or_conditions(condition):
                return self.__classify_combined_conditions(c)
        else:
            return self.__classify_condition(condition)

    def __classify_condition(self, condition: Expression) -> ConditionClass:
        # Expects this to be an individual condition
        match = self.__optimizable_pattern.match(condition)
        if (
            match is not None
            and match.string(KEY_COL_MAPPING_PARAM) == f"{self.__column_name}.key"
        ):
            rhs = match.expression("right_hand_side")
            if isinstance(rhs, LiteralExpr):
                if rhs.value == "":
                    return ConditionClass.NOT_OPTIMIZABLE
            elif isinstance(rhs, FunctionExpr):
                for p in rhs.parameters:
                    if not isinstance(rhs, LiteralExpr) or rhs.value == "":
                        return ConditionClass.NOT_OPTIMIZABLE
            return ConditionClass.OPTIMIZABLE
        elif match is None:
            for exp in condition:
                if isinstance(exp, Column) and exp.column_name == self.__column_name:
                    return ConditionClass.NOT_OPTIMIZABLE
            return ConditionClass.IRRELEVANT
        else:
            return ConditionClass.IRRELEVANT

    def __build_replaced(
        self,
        alias: Optional[str],
        table_name: Optional[str],
        function: str,
        parameters: Sequence[LiteralExpr],
    ) -> Expression:
        literals = [
            Column(
                alias=None, table_name=table_name, column_name=self.__hash_map_name,
            ),
            *[
                FunctionExpr(
                    alias=None,
                    function_name="cityHash64",
                    parameters=(LiteralExpr(None, param.value),),
                )
                for param in parameters
            ],
        ]
        return FunctionExpr(
            alias=alias,
            function_name=ConditionFunctions.EQ,
            parameters=(
                FunctionExpr(
                    alias=None, function_name=function, parameters=tuple(literals),
                ),
                LiteralExpr(None, 1),
            ),
        )

    def __replace_with_hash_map(self, condition: Expression) -> Expression:
        match = self.__optimizable_pattern.match(condition)
        if (
            match is None
            or match.string(KEY_COL_MAPPING_PARAM) != f"{self.__column_name}.key"
        ):
            return condition
        rhs = match.expression("right_hand_side")
        if isinstance(rhs, LiteralExpr):
            return self.__build_replaced(
                condition.alias,
                match.optional_string(TABLE_MAPPING_PARAM),
                "has",
                [LiteralExpr(None, f"{match.string(KEY_MAPPING_PARAM)}={rhs.value}")],
            )
        else:
            assert isinstance(rhs, FunctionExpr)
            params = []
            for e in rhs.parameters:
                assert isinstance(e, LiteralExpr)
                params.append(
                    LiteralExpr(None, f"{match.string(KEY_MAPPING_PARAM)}={e.value}")
                )
            return self.__build_replaced(
                condition.alias,
                match.optional_string(TABLE_MAPPING_PARAM),
                "hasAny",
                params,
            )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        cond_class = ConditionClass.IRRELEVANT
        condition = query.get_condition_from_ast()
        if condition is not None:
            cond_class = self.__classify_combined_conditions(condition)
            if cond_class == ConditionClass.NOT_OPTIMIZABLE:
                return
        having_cond = query.get_having_from_ast()
        if having_cond is not None:
            cond_class = self.__classify_combined_conditions(having_cond)
            if cond_class == ConditionClass.NOT_OPTIMIZABLE:
                return

        if cond_class != ConditionClass.OPTIMIZABLE:
            return

        if condition is not None:
            query.set_ast_condition(condition.transform(self.__replace_with_hash_map))
        if having_cond is not None:
            query.set_ast_having(having_cond.transform(self.__replace_with_hash_map))
