import re
from typing import Any, Mapping, Set, Union

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.escaping import escape_identifier
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.util import (
    alias_expr,
    escape_literal,
    qualified_column,
)

# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile(r"^([a-zA-Z0-9_\.]+)\[([a-zA-Z0-9_\.:-]+)\]$")


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
        matched = NESTED_COL_EXPR_RE.match(column_name)
        if matched and matched[1] in ["tags", "contexts"]:
            return self.__tag_expr(column_name, table_alias)
        elif column_name in ["tags_key", "tags_value"]:
            return self.__tags_expr(column_name, query, parsing_context, table_alias)
        return None

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

    def __tag_expr(self, column_name: str, table_alias: str = "",) -> str:
        """
        Return an expression for the value of a single named tag.

        For tags/contexts, we expand the expression depending on whether the tag is
        "promoted" to a top level column, or whether we have to look in the tags map.
        """
        col, tag = NESTED_COL_EXPR_RE.match(column_name).group(1, 2)
        # For promoted tags, return the column name.
        if col in self.__promoted_columns:
            actual_tag = self.__get_tag_column_map()[col].get(tag, tag)
            if actual_tag in self.__promoted_columns[col]:
                return qualified_column(self.__string_col(actual_tag), table_alias)

        # For the rest, return an expression that looks it up in the nested tags.
        return u"{col}.value[indexOf({col}.key, {tag})]".format(
            **{"col": qualified_column(col, table_alias), "tag": escape_literal(tag)}
        )

    def __tags_expr(
        self,
        column_name: str,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ) -> str:
        """
        Return an expression that array-joins on tags to produce an output with one
        row per tag.
        """
        assert column_name in ["tags_key", "tags_value"]
        col, k_or_v = column_name.split("_", 1)
        nested_tags_only = state.get_config("nested_tags_only", 1)

        qualified_col = qualified_column(col, table_alias)
        # Generate parallel lists of keys and values to arrayJoin on
        if nested_tags_only:
            key_list = "{}.key".format(qualified_col)
            val_list = "{}.value".format(qualified_col)
        else:
            promoted = self.__promoted_columns[col]
            col_map = self.__column_tag_map[col]
            key_list = u"arrayConcat([{}], {}.key)".format(
                u", ".join(u"'{}'".format(col_map.get(p, p)) for p in promoted),
                qualified_col,
            )
            val_list = u"arrayConcat([{}], {}.value)".format(
                ", ".join(self.__string_col(p) for p in promoted), qualified_col
            )

        qualified_key = qualified_column("tags_key", table_alias)
        qualified_value = qualified_column("tags_value", table_alias)
        cols_used = query.get_all_referenced_columns() & set(
            [qualified_key, qualified_value]
        )
        if len(cols_used) == 2:
            # If we use both tags_key and tags_value in this query, arrayjoin
            # on (key, value) tag tuples.
            expr = (u"arrayJoin(arrayMap((x,y) -> [x,y], {}, {}))").format(
                key_list, val_list
            )

            # put the all_tags expression in the alias cache so we can use the alias
            # to refer to it next time (eg. 'all_tags[1] AS tags_key'). instead of
            # expanding the whole tags expression again.
            expr = alias_expr(expr, "all_tags", parsing_context)
            return u"({})[{}]".format(expr, 1 if k_or_v == "key" else 2)
        else:
            # If we are only ever going to use one of tags_key or tags_value, don't
            # bother creating the k/v tuples to arrayJoin on, or the all_tags alias
            # to re-use as we won't need it.
            return "arrayJoin({})".format(key_list if k_or_v == "key" else val_list)
