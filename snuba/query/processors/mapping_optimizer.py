from enum import Enum

from dataclasses import dataclass
from snuba import environment
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
from snuba.query.matchers import (
    Any,
    FunctionCall,
    Literal,
    Or,
    Param,
    Pattern,
    String,
    MatchResult,
)
from snuba.request.request_settings import RequestSettings
from snuba.state import get_config
from snuba.utils.metrics.wrapper import MetricsWrapper
from typing import Optional

metrics = MetricsWrapper(environment.metrics, "processors.tags_hash_map")

ESCAPE_TRANSLATION = str.maketrans({"\\": "\\\\", "=": "\="})


class ConditionClass(Enum):
    IRRELEVANT = 1
    OPTIMIZABLE = 2
    NOT_OPTIMIZABLE = 3


class NonEmptyString(String):
    def match(self, node: Any[str]) -> Optional[MatchResult]:
        return MatchResult() if isinstance(node, str) and node != "" else None


class TagRewriter:
    def __init__(self, matching_pattern: Pattern[FunctionExpr]) -> None:
        self.matching_pattern = matching_pattern

    def replace_function(
        self, to_replace: Expression, match: MatchResult, hash_map_name: str
    ) -> Expression:
        raise NotImplementedError


class TagEquality(TagRewriter):
    def __init__(self) -> None:
        super().__init__(
            FunctionCall(
                function_name=String("equals"),
                parameters=(
                    Or(
                        [
                            mapping_pattern,
                            FunctionCall(
                                function_name=String("ifNull"),
                                parameters=(mapping_pattern, Literal(String(""))),
                            ),
                        ]
                    ),
                    Param(
                        "right_hand_side",
                        Literal(NonEmptyString("dont let me merge with this param")),
                    ),
                ),
            )
        )

    def replace_function(
        self, condition: Expression, match: MatchResult, hash_map_name: str
    ) -> Expression:
        rhs = match.expression("right_hand_side")
        assert isinstance(rhs, LiteralExpr)
        key = match.string(KEY_MAPPING_PARAM).translate(ESCAPE_TRANSLATION)
        return FunctionExpr(
            alias=condition.alias,
            function_name="has",
            parameters=(
                Column(
                    alias=None,
                    table_name=match.optional_string(TABLE_MAPPING_PARAM),
                    column_name=hash_map_name,
                ),
                # the problem is that the != case has a different rhs. Hence we need
                # to do something different for every optimizable expression
                FunctionExpr(
                    alias=None,
                    function_name="cityHash64",
                    parameters=(LiteralExpr(None, f"{key}={rhs.value}"),),
                ),
            ),
        )


class TagNotEqualsEmpty(TagRewriter):
    def __init__(self) -> None:
        super().__init__(
            FunctionCall(
                function_name=String("notEquals"),
                parameters=(
                    Or(
                        [
                            mapping_pattern,
                            FunctionCall(
                                function_name=String("ifNull"),
                                parameters=(mapping_pattern, Literal(String(""))),
                            ),
                        ]
                    ),
                    Param("right_hand_side", Literal(String(""))),
                ),
            )
        )

    def replace_function(
        self, condition: Expression, match: MatchResult, hash_map_name: str
    ) -> Expression:

        # LOOK AT ME: this will need to change because we cannot access the tag name in the matching expression
        # IN will suffer from this as well
        # assert isinstance(rhs, LiteralExpr)
        tag_name = match.string(KEY_MAPPING_PARAM).translate(ESCAPE_TRANSLATION)
        return FunctionExpr(
            alias=condition.alias,
            function_name="has",
            parameters=(
                Column(
                    alias=None,
                    table_name=match.optional_string(TABLE_MAPPING_PARAM),
                    column_name=hash_map_name,
                ),
                # the problem is that the != case has a different rhs. Hence we need
                # to do something different for every optimizable expression
                FunctionExpr(
                    alias=None,
                    function_name="cityHash64",
                    parameters=(LiteralExpr(None, f"{key}={tag_name}"),),
                ),
            ),
        )
        return condition


class MappingOptimizer(QueryProcessor):
    """
    Optimize tags conditions by relying on the tags_hash_map column.
    Such column is an array of hashes of `key=value` strings.
    This processor transforms tags conditions that are in the form of
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

    def __init__(self, column_name: str, hash_map_name: str, killswitch: str) -> None:
        self.__column_name = column_name
        self.__hash_map_name = hash_map_name
        self.__killswitch = killswitch

        # TODO: Add the support for IN connditions.
        self.__optimizable_patterns = [TagEquality(), TagNotEqualsEmpty()]

        # self.__optimizable_patterns = [
        #     FunctionCall(
        #         function_name=String("equals"),
        #         parameters=(
        #             Or(
        #                 [
        #                     mapping_pattern,
        #                     FunctionCall(
        #                         function_name=String("ifNull"),
        #                         parameters=(mapping_pattern, Literal(String(""))),
        #                     ),
        #                 ]
        #             ),
        #             Param(
        #                 "right_hand_side",
        #                 Literal(NonEmptyString("dont let me merge with this param")),
        #             ),
        #         ),
        #     ),
        #     FunctionCall(
        #         function_name=String("notEquals"),
        #         parameters=(
        #             Or(
        #                 [
        #                     mapping_pattern,
        #                     FunctionCall(
        #                         function_name=String("ifNull"),
        #                         parameters=(mapping_pattern, Literal(String(""))),
        #                     ),
        #                 ]
        #             ),
        #             Param("right_hand_side", Literal(String(""))),
        #         ),
        #     ),
        # ]

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
        # Expects condition arg to be an individual condition
        non_optimizable_match_found = False
        for pattern in self.__optimizable_patterns:
            match = pattern.matching_pattern.match(condition)
            if (
                match is not None
                and match.string(KEY_COL_MAPPING_PARAM) == f"{self.__column_name}.key"
            ):
                return ConditionClass.OPTIMIZABLE
            elif match is None:
                # If this condition is not matching an optimizable condition,
                # check that it does not reference the optimizable column.
                # If it does, it means we should not optimize this query.
                for exp in condition:
                    if isinstance(exp, Column) and exp.column_name in (
                        f"{self.__column_name}.key",
                        f"{self.__column_name}.value",
                    ):
                        non_optimizable_match_found = True
                return ConditionClass.IRRELEVANT
        if non_optimizable_match_found:
            # TODO: Word this better
            # none of the patterns yielded an optimizable match
            # but did yield non-optimizable matches, hence this condition
            # is not optimizable
            return ConditionClass.NOT_OPTIMIZABLE
        return ConditionClass.IRRELEVANT

    def __replace_with_hash(self, condition: Expression) -> Expression:
        # TODO (Vlad): we shouldn't have to run the matchers twice maybe? it may be possible to
        # save which patterns are applicable, although more than one can apply, in which case
        # that might be difficult. Think about it later
        for pattern in self.__optimizable_patterns:
            match = pattern.matching_pattern.match(condition)
            if (
                match is not None
                and match.string(KEY_COL_MAPPING_PARAM) == f"{self.__column_name}.key"
            ):
                pattern.replace_function(condition, match, self.__hash_map_name)
                # rhs = match.expression("right_hand_side")
                # assert isinstance(rhs, LiteralExpr)
                # key = match.string(KEY_MAPPING_PARAM).translate(ESCAPE_TRANSLATION)
                # return FunctionExpr(
                #     alias=condition.alias,
                #     function_name="has",
                #     parameters=(
                #         Column(
                #             alias=None,
                #             table_name=match.optional_string(TABLE_MAPPING_PARAM),
                #             column_name=self.__hash_map_name,
                #         ),
                #         # the problem is that the != case has a different rhs. Hence we need
                #         # to do something different for every optimizable expression
                #         FunctionExpr(
                #             alias=None,
                #             function_name="cityHash64",
                #             parameters=(LiteralExpr(None, f"{key}={rhs.value}"),),
                #         ),
                #     ),
                # )
        return condition

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        print("<" * 100)
        print(query)
        print(">" * 100)

        if not get_config(self.__killswitch, 1):
            return

        cond_class = ConditionClass.IRRELEVANT
        condition = query.get_condition()
        if condition is not None:
            cond_class = self.__classify_combined_conditions(condition)
            if cond_class == ConditionClass.NOT_OPTIMIZABLE:
                return

        having_cond_class = ConditionClass.IRRELEVANT
        having_cond = query.get_having()
        if having_cond is not None:
            having_cond_class = self.__classify_combined_conditions(having_cond)
            if having_cond_class == ConditionClass.NOT_OPTIMIZABLE:
                return

        metrics.increment("optimizable_query")

        if condition is not None and cond_class == ConditionClass.OPTIMIZABLE:
            query.set_ast_condition(condition.transform(self.__replace_with_hash))
        if having_cond is not None and having_cond_class == ConditionClass.OPTIMIZABLE:
            query.set_ast_having(having_cond.transform(self.__replace_with_hash))
