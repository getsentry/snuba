from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Set, Union

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.escaping import escape_identifier
from snuba.query.columns import alias_expr
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    is_binary_condition,
    is_in_condition,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.parser.strings import NESTED_COL_EXPR_RE
from snuba.query.parsing import ParsingContext
from snuba.util import escape_literal, qualified_column


@dataclass(frozen=True)
class ParsedNestedColumn:
    """
    Provides some structure to the tags/contexts column to avoid parsing the string
    in multiple places around the code.
    """

    col_name: str
    tag_name: Optional[str]

    @classmethod
    def __is_individual_column(cls, col_name: str) -> bool:
        return col_name in ("tags", "contexts")

    @classmethod
    def __is_joined_column(cls, col_name: str) -> bool:
        return col_name in ("tags_key", "tags_value")

    @classmethod
    def parse_column_expression(cls, col_expr: str) -> Optional[ParsedNestedColumn]:
        match = NESTED_COL_EXPR_RE.match(col_expr)
        if match:
            col_prefix = match[1]
            param_string = match[2]

            if cls.__is_individual_column(col_prefix):
                return ParsedNestedColumn(col_prefix, param_string)

        if cls.__is_joined_column(col_expr):
            return ParsedNestedColumn(col_expr, None)

        return None

    def is_joined_column(self) -> bool:
        return self.__is_joined_column(self.col_name)

    def is_single_column(self) -> bool:
        return self.__is_individual_column(self.col_name)


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
        col_type_name = str(col_type) if col_type else None

        if (
            col_type_name
            and "String" in col_type_name
            and "FixedString" not in col_type_name
        ):
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
        assert parsed_col.tag_name
        tag_name = parsed_col.tag_name
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

    def __extract_top_level_tag_conditions(self, condition: Expression) -> List[str]:
        """
        Finds the top level conditions that include tags_key in an expression.
        For top level condition. In Snuba conditions are expressed in layers, the top level
        ones are in AND, the nested ones are in OR and so on.
        We can only apply the arrayFilter optimization to tag keys conditions that are not in
        OR with other columns. To simplify the problem, we only consider those conditions that
        are included in the first level of the query:
        [['tagskey' '=' 'a'],['col' '=' 'b'],['col2' '=' 'c']]  works
        [[['tagskey' '=' 'a'], ['col2' '=' 'b']], ['tagskey' '=' 'c']] does not
        """

        if is_binary_condition(condition, ConditionFunctions.EQ):
            # This is to make mypy happy.
            assert isinstance(condition, FunctionCall)
            if (
                isinstance(condition.parameters[0], FunctionCall)
                and condition.parameters[0].function_name == "arrayJoin"
                and condition.parameters[0].parameters
                == (Column(None, "tags.key", None),)
                and isinstance(condition.parameters[1], Literal)
            ):
                # Tags are strings, but mypy does not know that
                return [str(condition.parameters[1].value)]

        if is_in_condition(condition):
            assert isinstance(condition, FunctionCall)
            if (
                isinstance(condition.parameters[0], FunctionCall)
                and condition.parameters[0].function_name == "arrayJoin"
                and condition.parameters[0].parameters
                == (Column(None, "tags.key", None),)
            ):
                # The parameters of the inner function `a IN tuple(b,c,d)`
                assert isinstance(condition.parameters[1], FunctionCall)
                literals = condition.parameters[1].parameters
                return [
                    str(literal.value)
                    for literal in literals
                    if isinstance(literal, Literal)
                ]

        if is_binary_condition(condition, BooleanFunctions.AND):
            assert isinstance(condition, FunctionCall)
            return self.__extract_top_level_tag_conditions(
                condition.parameters[0]
            ) + self.__extract_top_level_tag_conditions(condition.parameters[1])

        return []

    def __get_filter_tags(self, query: Query) -> List[str]:
        """
        Identifies the tag names we can apply the arrayFilter optimization on.
        Which means: if the tags_key column is in the select clause and there are
        one or more top level conditions on the tags_key column.
        """
        if not state.get_config("ast_tag_processor_enabled", 0):
            return []

        select_clause = query.get_selected_columns_from_ast() or []

        tags_key_found = any(
            f.function_name == "arrayJoin"
            and f.parameters == (Column(None, "tags.key", None),)
            for expression in select_clause
            for f in expression
            if isinstance(f, FunctionCall)
        )

        if not tags_key_found:
            return []

        def extract_tags_from_condition(
            cond: Optional[Expression],
        ) -> Optional[List[str]]:
            if not cond:
                return []
            if any(is_binary_condition(cond, BooleanFunctions.OR) for cond in cond):
                return None
            return self.__extract_top_level_tag_conditions(cond)

        cond_tags_key = extract_tags_from_condition(query.get_condition_from_ast())
        if cond_tags_key is None:
            # This means we found an OR. Cowardly we give up even though there could
            # be cases where this condition is still optimizable.
            return []
        having_tags_key = extract_tags_from_condition(query.get_having_from_ast())
        if having_tags_key is None:
            # Same as above
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
                filtering = (
                    f"arrayFilter(pair -> pair[1] IN ({filter_tags}), {mapping})"
                )
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
