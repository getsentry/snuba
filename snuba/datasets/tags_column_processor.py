from __future__ import annotations

import re

from typing import Any, Mapping, Optional, Sequence, Set, Union

from dataclasses import dataclass
from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.escaping import escape_identifier
from snuba.query.expressions import Column, Expression, Literal
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    is_binary_condition,
    is_in_condition,
)
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.util import (
    alias_expr,
    escape_literal,
    qualified_column,
)

# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile(r"^([a-zA-Z0-9_\.]+)\[([a-zA-Z0-9_\.:-]+)\]$")


@dataclass(frozen=True)
class ParsedNestedColumn:
    """
    Provides some structure to the tags/contexts column to avoid parsing the string
    in multiple places around the code.
    """

    col_name: str
    parameter: Optional[str]

    @classmethod
    def parse_column_expression(cls, col_expr: str) -> ParsedNestedColumn:
        match = NESTED_COL_EXPR_RE.match(col_expr)
        if match:
            col_prefix = match[1]
            param_string = match[2]

            if col_prefix in ("tags", "contexts"):
                assert param_string
            return ParsedNestedColumn(col_prefix, param_string)

        if col_expr in ("tags_key", "tags_value"):
            return ParsedNestedColumn(col_expr, None)

        return None

    def is_joined_column(self) -> bool:
        return self.col_name in ("tags_key", "tags_value")

    def is_single_column(self) -> bool:
        return self.col_name in ("tags", "contexts")


class TagColumnProcessor:
    """
    Provides the query processing features for tables that have
    tags and contexts nested columns. This simply extracts these
    features originally developed in the Events dataset.
    """

    def __init__(
        self,
        columns: ColumnSet,
        promoted_columns: Mapping[str, Set[str]],
        column_tag_map: Mapping[str, Mapping[str, str]],
    ) -> None:
        # The ColumnSet of the dataset. Used to format promoted
        # columns with the right type.
        self.__columns = columns
        # Keeps a dictionary of promoted columns. The key of the mapping
        # can be 'tags' or 'contexts'. The values is a set of flattened
        # columns.
        self.__promoted_columns = promoted_columns
        # A mapping between column representing promoted tags and the
        # corresponding tag.
        self.__column_tag_map = column_tag_map

    def process_column_expression(
        self,
        column_name: str,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ) -> Union[None, Any]:
        """
        This method resolves the tag or context, processes it and formats
        the column expression for the query. It is supposed to be called
        by column_expr methods in the datasets.

        It returns None if the column is not a tag or context.
        """
        parsed_col = ParsedNestedColumn.parse_column_expression(column_name)
        if not parsed_col:
            return None
        if parsed_col.is_single_column():
            return self.__tag_expr(parsed_col, table_alias)
        elif parsed_col.is_joined_column():
            # TODO: Should we support contexts?
            return self.__tags_expr(parsed_col, query, parsing_context, table_alias)
        # We should never get here if we got an instance of ParsedNestedColumn
        raise ValueError(f"Invalid tag/context column structure {column_name}")

    def __get_tag_column_map(self) -> Mapping[str, Mapping[str, str]]:
        # And a reverse map from the tags the client expects to the database columns
        return {
            col: dict(map(reversed, trans.items()))
            for col, trans in self.__column_tag_map.items()
        }

    def __string_col(self, col: str) -> str:
        col_type = self.__columns.get(col, None)
        col_type = str(col_type) if col_type else None

        if col_type and "String" in col_type and "FixedString" not in col_type:
            return escape_identifier(col)
        else:
            return "toString({})".format(escape_identifier(col))

    def __tag_expr(self, parsed_col: ParsedNestedColumn, table_alias: str = "",) -> str:
        """
        Return an expression for the value of a single named tag.

        For tags/contexts, we expand the expression depending on whether the tag is
        "promoted" to a top level column, or whether we have to look in the tags map.
        """
        # For promoted tags, return the column name.
        assert parsed_col.parameter
        tag_name = parsed_col.parameter
        col = parsed_col.col_name
        if col in self.__promoted_columns:
            actual_tag = self.__get_tag_column_map()[col].get(tag_name, tag_name)
            if actual_tag in self.__promoted_columns[col]:
                return qualified_column(self.__string_col(actual_tag), table_alias)

        # For the rest, return an expression that looks it up in the nested tags.
        return "{col}.value[indexOf({col}.key, {tag})]".format(
            **{
                "col": qualified_column(col, table_alias),
                "tag": escape_literal(tag_name),
            }
        )

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

    def __get_filter_tags(self, query: Query) -> Sequence[str]:
        if not state.get_config("ast_tag_processor_enabled", 1):
            return []

        select_clause = query.get_selected_columns_from_ast() or []

        tags_key_found = any(
            [
                col.column_name == "tags_key"
                for expression in select_clause
                for col in expression
                if isinstance(col, Column)
            ]
        )

        if not tags_key_found:
            return []

        def extract_tags_from_condition(cond: Optional[Expression]) -> Sequence[str]:
            if not cond:
                return []
            if any([is_binary_condition(cond, BooleanFunctions.OR) for cond in cond]):
                return None
            return self.__extract_top_level_tag_conditions(cond)

        cond_tags_key = extract_tags_from_condition(query.get_condition_from_ast())
        if cond_tags_key is None:
            return []
        having_tags_key = extract_tags_from_condition(query.get_having_from_ast())
        if having_tags_key is None:
            return []

        return cond_tags_key + having_tags_key

    def __tags_expr(
        self,
        parsed_col: ParsedNestedColumn,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ) -> str:
        """
        Return an expression that array-joins on tags to produce an output with one
        row per tag.

        It can also apply an arrayFilter in the arrayJoin if an equivalent condition
        is found in the query in order to reduce the size of the arrayJoin.
        """
        col, k_or_v = parsed_col.col_name.split("_", 1)
        nested_tags_only = state.get_config("nested_tags_only", 1)

        qualified_col = qualified_column(col, table_alias)
        # Generate parallel lists of keys and values to arrayJoin on
        if nested_tags_only:
            key_list = "{}.key".format(qualified_col)
            val_list = "{}.value".format(qualified_col)
        else:
            promoted = self.__promoted_columns[col]
            col_map = self.__column_tag_map[col]
            key_list = "arrayConcat([{}], {}.key)".format(
                ", ".join("'{}'".format(col_map.get(p, p)) for p in promoted),
                qualified_col,
            )
            val_list = "arrayConcat([{}], {}.value)".format(
                ", ".join(self.__string_col(p) for p in promoted), qualified_col
            )

        qualified_key = qualified_column("tags_key", table_alias)
        qualified_value = qualified_column("tags_value", table_alias)
        cols_used = query.get_all_referenced_columns() & set(
            [qualified_key, qualified_value]
        )

        filter_tags = ",".join([f"'{tag}'" for tag in self.__get_filter_tags(query)])
        if len(cols_used) == 2:
            # If we use both tags_key and tags_value in this query, arrayjoin
            # on (key, value) tag tuples.
            mapping = f"arrayMap((x,y) -> [x,y], {key_list}, {val_list})"
            if filter_tags:
                filtering_expression = (
                    f"arrayFilter(pair -> pair[1] IN ({filter_tags}), %s)"
                )
                filtering = filtering_expression % mapping
            else:
                filtering = mapping

            expr = f"arrayJoin({filtering})"

            # put the all_tags expression in the alias cache so we can use the alias
            # to refer to it next time (eg. 'all_tags[1] AS tags_key'). instead of
            # expanding the whole tags expression again.
            expr = alias_expr(expr, "all_tags", parsing_context)
            return "({})[{}]".format(expr, 1 if k_or_v == "key" else 2)
        else:
            # If we are only ever going to use one of tags_key or tags_value, don't
            # bother creating the k/v tuples to arrayJoin on, or the all_tags alias
            # to re-use as we won't need it.
            if filter_tags:
                return (
                    f"arrayJoin(arrayFilter(tag -> tag IN ({filter_tags}), {key_list}))"
                )
            else:
                return f"arrayJoin({key_list if k_or_v == 'key' else val_list})"
