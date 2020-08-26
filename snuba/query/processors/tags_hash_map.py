from enum import Enum

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
    """
    Optimize tags conditions by relying on the tags_hash_map column.
    It transforms tags conditions in the form of
    `tags.value[indexOf(tags.key, 'my_tag')] = 'my_val'`
    into
    `has(_tags_hash_map, cityHash64('my_tag=my_val'))`

    Supported use case:
    - direct equality. Example above
    - tags expression nested into ifNull conditions like:
      `ifNull('tags.value[indexOf(tags.key, 'my_tag')]', '') = ...`
    - tags conditions in both where and having

    Unsupported use cases:
    - everything that cannot be checked through the tags hash map
      like !=, LIKE, IS NULL
    - it will not optimize a condition if another condition still
      requires unpacking the tags column. Like
      `tags[a] = b AND tags[b] LIKE 'asd'`
      That would load an additional column for almost no gain thus
      actually degrading performance.
    - `ifNull('tags.value[indexOf(tags.key, 'my_tag')]', '') = ''`
       this condition is equivalent to looking whether a tag is
       missing, which cannot be done with the hash map.
    - IN conditions. TODO
    """

    def __init__(self, column_name: str, hash_map_name: str) -> None:
        self.__column_name = column_name
        self.__hash_map_name = hash_map_name

        self.__optimizable_pattern = FunctionCall(
            alias=None,
            function_name=String("equals"),
            parameters=(
                Or(
                    [
                        mapping_pattern,
                        FunctionCall(
                            alias=None,
                            function_name=String("ifNull"),
                            parameters=(mapping_pattern, Literal(None, String(""))),
                        ),
                    ]
                ),
                Param("right_hand_side", Literal(None, Any(str))),
            ),
        )

    def __classify_combined_conditions(self, condition: Expression) -> ConditionClass:
        if not isinstance(condition, FunctionExpr):
            return ConditionClass.IRRELEVANT
        elif condition.function_name in (BooleanFunctions.AND, BooleanFunctions.OR):
            conditions = (
                get_first_level_and_conditions(condition)
                if condition.function_name == BooleanFunctions.AND
                else get_first_level_or_conditions(condition)
            )
            classified = {self.__classify_combined_conditions(c) for c in conditions}
            if ConditionClass.NOT_OPTIMIZABLE in classified:
                return ConditionClass.NOT_OPTIMIZABLE
            elif ConditionClass.OPTIMIZABLE in classified:
                return ConditionClass.OPTIMIZABLE
            else:
                return ConditionClass.IRRELEVANT
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
            assert isinstance(rhs, LiteralExpr)
            return (
                ConditionClass.NOT_OPTIMIZABLE
                # ifNull(tags[asd], '') = '' is not optimizable.
                if rhs.value == ""
                else ConditionClass.OPTIMIZABLE
            )
        elif match is None:
            # If this is not matching an optimizable condition, check
            # that it does not reference the optimizable column.
            # If it does it means we should not optimize this query.
            for exp in condition:
                if isinstance(exp, Column) and exp.column_name in (
                    f"{self.__column_name}.key",
                    f"{self.__column_name}.value",
                ):
                    return ConditionClass.NOT_OPTIMIZABLE
            return ConditionClass.IRRELEVANT
        else:
            return ConditionClass.IRRELEVANT

    def __replace_with_hash_map(self, condition: Expression) -> Expression:
        match = self.__optimizable_pattern.match(condition)
        if (
            match is None
            or match.string(KEY_COL_MAPPING_PARAM) != f"{self.__column_name}.key"
        ):
            return condition
        rhs = match.expression("right_hand_side")
        assert isinstance(rhs, LiteralExpr)
        return FunctionExpr(
            alias=condition.alias,
            function_name="has",
            parameters=(
                Column(
                    alias=None,
                    table_name=match.optional_string(TABLE_MAPPING_PARAM),
                    column_name=self.__hash_map_name,
                ),
                FunctionExpr(
                    alias=None,
                    function_name="cityHash64",
                    parameters=(
                        LiteralExpr(
                            None, f"{match.string(KEY_MAPPING_PARAM)}={rhs.value}"
                        ),
                    ),
                ),
            ),
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        cond_class = ConditionClass.IRRELEVANT
        condition = query.get_condition_from_ast()
        if condition is not None:
            cond_class = self.__classify_combined_conditions(condition)
            if cond_class == ConditionClass.NOT_OPTIMIZABLE:
                return

        having_cond_class = ConditionClass.IRRELEVANT
        having_cond = query.get_having_from_ast()
        if having_cond is not None:
            having_cond_class = self.__classify_combined_conditions(having_cond)
            if having_cond_class == ConditionClass.NOT_OPTIMIZABLE:
                return

        if (
            cond_class != ConditionClass.OPTIMIZABLE
            and having_cond_class != ConditionClass.OPTIMIZABLE
        ):
            return

        if condition is not None:
            query.set_ast_condition(condition.transform(self.__replace_with_hash_map))
        if having_cond is not None:
            query.set_ast_having(having_cond.transform(self.__replace_with_hash_map))
