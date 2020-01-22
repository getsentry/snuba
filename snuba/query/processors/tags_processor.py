from typing import Optional, Sequence

from snuba import state
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    is_binary_condition,
    is_in_condition,
)
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.query_processor import QueryProcessor
from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings


class TagsProcessor(QueryProcessor):
    def __has_tags_key(self, select_clause: Sequence[Expression]) -> bool:
        for exp in select_clause:
            tags_key_found = any(
                [
                    col.column_name == "tags_key"
                    for col in select_clause
                    if isinstance(col, Column)
                ]
            )
            if tags_key_found:
                return True
        return False

    def __extract_top_level_tag_conditions(
        self, condition: Expression
    ) -> Sequence[str]:
        if (
            is_binary_condition(condition, ConditionFunctions.EQ)
            and isinstance(condition.parameters[0], Column)
            and condition.parameters[0].column_name == "tags_key"
            and isinstance(condition.parameters[1], Literal)
        ):
            return [condition.parameters[1].value]

        if (
            is_in_condition(condition)
            and isinstance(condition.parameters[0], Column)
            and condition.parameters[0].column_name == "tags_key"
        ):
            # The parameters of the inner function `a IN tuple(b,c,d)`
            literals = condition.parameters[1].parameters
            return [
                literal.value for literal in literals if isinstance(literal, Literal)
            ]

        if is_binary_condition(condition, BooleanFunctions.AND):
            return self.__extract_top_level_tag_conditions(
                condition.parameters[0]
            ) + self.__extract_top_level_tag_conditions(condition.parameters[1])

        return []

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        if not state.get_config("ast_tag_processor_enabled", 0):
            return

        select_clause = query.get_selected_columns_from_ast() or []
        tags_key_found = self.__has_tags_key(select_clause)

        if not tags_key_found:
            return

        def extract_tags_from_condition(cond: Optional[Expression]) -> Sequence[str]:
            if not cond:
                return []
            if any([is_binary_condition(cond, BooleanFunctions.OR) for cond in cond]):
                return None
            return self.__extract_top_level_tag_conditions(cond)

        cond_tags_key = extract_tags_from_condition(query.get_condition_from_ast())
        if cond_tags_key is None:
            return
        having_tags_key = extract_tags_from_condition(query.get_having_from_ast())
        if having_tags_key is None:
            return

        tag_keys = cond_tags_key + having_tags_key

        if not tag_keys:
            return

        replaced_name = f"tags_key[{', '.join(tag_keys)}]"
        query.replace_column("tags_key", replaced_name)
