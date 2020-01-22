from typing import Sequence

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
        select_clause = query.get_selected_columns_from_ast() or []
        tags_key_found = self.__has_tags_key(select_clause)

        if not tags_key_found:
            return

        tag_keys = []
        if query.get_condition_from_ast():
            if any(
                [
                    is_binary_condition(cond, BooleanFunctions.OR)
                    for cond in query.get_condition_from_ast()
                ]
            ):
                return

            tag_keys.extend(
                self.__extract_top_level_tag_conditions(query.get_condition_from_ast())
            )
        if query.get_having_from_ast():
            if any(
                [
                    is_binary_condition(cond, BooleanFunctions.OR)
                    for cond in query.get_having_from_ast()
                ]
            ):
                return

            tag_keys.extend(
                self.__extract_top_level_tag_conditions(query.get_having_from_ast())
            )

        if not tag_keys:
            return

        replaced_name = f"tags_key[{', '.join(tag_keys)}]"
        query.replace_column("tags_key", replaced_name)
