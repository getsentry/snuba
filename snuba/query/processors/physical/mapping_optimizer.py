import logging
from enum import Enum
from typing import Optional, Tuple

from snuba import environment
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_MAPPING_PARAM,
    KEY_MAPPING_PARAM,
    TABLE_MAPPING_PARAM,
    VALUE_COL_MAPPING_PARAM,
    mapping_pattern,
)
from snuba.query.conditions import (
    BooleanFunctions,
    combine_and_conditions,
    combine_or_conditions,
    get_first_level_and_conditions,
    get_first_level_or_conditions,
)
from snuba.query.expressions import Column, Expression
from snuba.query.expressions import FunctionCall as FunctionExpr
from snuba.query.expressions import Literal as LiteralExpr
from snuba.query.matchers import Any, AnyOptionalString
from snuba.query.matchers import Column as ColumnMatcher
from snuba.query.matchers import FunctionCall, Literal, Or, Param, String
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import get_config
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "processors.tags_hash_map")
logger = logging.getLogger("snuba.mapping_optimizer")

ESCAPE_TRANSLATION = str.maketrans({"\\": "\\\\", "=": "\="})


class ConditionClass(Enum):
    IRRELEVANT = 1
    OPTIMIZABLE = 2
    NOT_OPTIMIZABLE = 3


class MappingOptimizer(ClickhouseQueryProcessor):
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
        - There is a special case of this where the query will have
            (
                ifNull('tags.value[indexOf(tags.key, 'my_tag')]', '') != '' AND
                ifNull('tags.value[indexOf(tags.key, 'my_tag')]', '') = 'my_tag_value
            )
            in this case, the first condition is redundant and will be removed from the query
            NOTE: The ifNull wrapping is optional, direct comparison will be optimized away as well
    - IN conditions. TODO
    """

    def __init__(
        self,
        column_name: str,
        hash_map_name: str,
        killswitch: str,
        value_subcolumn_name: str = "value",
    ) -> None:
        self.__column_name = column_name
        self.__hash_map_name = hash_map_name
        self.__killswitch = killswitch
        self.__value_subcolumn_name = value_subcolumn_name

        # TODO: Add the support for IN conditions.
        self.__optimizable_pattern = FunctionCall(
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
                Param("right_hand_side", Literal(Any(str))),
            ),
        )
        self.__tag_exists_patterns = [
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
                    Param("right_hand_side", Literal(Any(str))),
                ),
            ),
            FunctionCall(
                function_name=String("has"),
                parameters=(
                    ColumnMatcher(
                        Param(TABLE_MAPPING_PARAM, AnyOptionalString()),
                        Param(VALUE_COL_MAPPING_PARAM, String(f"{column_name}.key")),
                    ),
                    Literal(Param(KEY_MAPPING_PARAM, Any(str))),
                ),
            ),
        ]

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
            # If this condition is not matching an optimizable condition,
            # check that it does not reference the optimizable column.
            # If it does, it means we should not optimize this query.
            for exp in condition:
                if isinstance(exp, Column) and exp.column_name in (
                    f"{self.__column_name}.key",
                    f"{self.__column_name}.{self.__value_subcolumn_name}",
                ):
                    return ConditionClass.NOT_OPTIMIZABLE
            return ConditionClass.IRRELEVANT
        else:
            return ConditionClass.IRRELEVANT

    def __replace_with_hash(self, condition: Expression) -> Expression:
        match = self.__optimizable_pattern.match(condition)
        if (
            match is None
            or match.string(KEY_COL_MAPPING_PARAM) != f"{self.__column_name}.key"
        ):
            return condition
        rhs = match.expression("right_hand_side")
        assert isinstance(rhs, LiteralExpr)
        key = match.scalar(KEY_MAPPING_PARAM)
        assert isinstance(key, (str, int))
        if isinstance(key, str):
            key = key.translate(ESCAPE_TRANSLATION)

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
                    parameters=(LiteralExpr(None, f"{key}={rhs.value}"),),
                ),
            ),
        )

    def _get_condition_without_redundant_checks(
        self, condition: Expression, query: Query
    ) -> Expression:
        """Optimizes the case where the query condition contains the following:

        valueOf('my_tag') != '' AND valueOf('my_tag') == "something"
                          ^                            ^
                          |                            |
                      existence check               value check

        the existence check in this clause is redundant and prevents the hashmap
        optimization from being applied.

        This function will remove all tag existence checks
        from the condition IFF they are ANDed with a value check for the *same tag name*

        Side effects:
            This function works by flattening first level AND conditions to find clauses where
            existence checks and value checks are ANDed together. When the AND conditions are recombined,
            they are not guaranteed to be in the same structure (but are guaranteed to be functionally equivalent)

            Example:
                ┌───┐         ┌───┐
                │AND│         │AND│
                ├──┬┘         └┬──┤
                │  │           │  │
             ┌──┴┐ c           a ┌┴──┐
             │AND│    becomes    │AND│
             └┬─┬┘               ├──┬┘
              │ │                │  │
              a b                b  c
        """
        if not isinstance(condition, FunctionExpr):
            return condition
        elif condition.function_name == BooleanFunctions.OR:
            sub_conditions = get_first_level_or_conditions(condition)
            pruned_conditions = [
                self._get_condition_without_redundant_checks(c, query)
                for c in sub_conditions
            ]
            return combine_or_conditions(pruned_conditions)
        elif condition.function_name == BooleanFunctions.AND:
            sub_conditions = get_first_level_and_conditions(condition)
            tag_eq_match_keys = set()
            matched_tag_exists_conditions = {}
            for condition_id, cond in enumerate(sub_conditions):
                tag_exist_match = None
                for tag_exists_pattern in self.__tag_exists_patterns:
                    tag_exist_match = tag_exists_pattern.match(cond)
                    if tag_exist_match:
                        matched_tag_exists_conditions[condition_id] = tag_exist_match
                if not tag_exist_match:
                    eq_match = self.__optimizable_pattern.match(cond)
                    if eq_match:
                        tag_eq_match_keys.add(eq_match.scalar(KEY_MAPPING_PARAM))
            useful_conditions = []
            for condition_id, cond in enumerate(sub_conditions):
                tag_exist_match = matched_tag_exists_conditions.get(condition_id, None)
                if tag_exist_match:
                    requested_tag = tag_exist_match.scalar("key")
                    if requested_tag in tag_eq_match_keys:
                        # the clause is redundant, thus we continue the loop
                        # and do not add it to useful_conditions
                        continue
                useful_conditions.append(
                    self._get_condition_without_redundant_checks(cond, query)
                )
            return combine_and_conditions(useful_conditions)
        else:
            return condition

    def __get_reduced_and_classified_query_clause(
        self, clause: Optional[Expression], query: Query
    ) -> Tuple[Optional[Expression], ConditionClass]:
        cond_class = ConditionClass.IRRELEVANT
        if clause is not None:
            new_clause = self._get_condition_without_redundant_checks(clause, query)
            cond_class = self.__classify_combined_conditions(new_clause)
            return new_clause, cond_class
        else:
            return clause, cond_class

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        if not get_config(self.__killswitch, 1):
            return
        condition, cond_class = self.__get_reduced_and_classified_query_clause(
            query.get_condition(), query
        )
        query.set_ast_condition(condition)
        if cond_class == ConditionClass.NOT_OPTIMIZABLE:
            return

        having_cond, having_cond_class = self.__get_reduced_and_classified_query_clause(
            query.get_having(), query
        )
        query.set_ast_having(having_cond)
        if having_cond_class == ConditionClass.NOT_OPTIMIZABLE:
            return

        if not (
            cond_class == ConditionClass.OPTIMIZABLE
            or having_cond_class == ConditionClass.OPTIMIZABLE
        ):
            return

        metrics.increment("optimizable_query")
        query.add_experiment("tags_hashmap_applied", 1)

        if condition is not None:
            query.set_ast_condition(condition.transform(self.__replace_with_hash))
        if having_cond is not None:
            query.set_ast_having(having_cond.transform(self.__replace_with_hash))
