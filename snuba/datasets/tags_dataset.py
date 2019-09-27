import re
from typing import Any, Mapping, Set, Union

from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.util import (
    alias_expr,
    all_referenced_columns,
    escape_literal,
    escape_col,
)

# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile(r'^(tags|contexts)\[([a-zA-Z0-9_\.:-]+)\]$')


class TagColumnProcessor:

    def __init__(self,
        columns: ColumnSet,
        promoted_columns: Mapping[str, Set[str]],
        column_tag_map: Mapping[str, Mapping[str, str]]
    ) -> None:
        self.__columns = columns
        self.__promoted_columns = promoted_columns
        self.__column_tag_map = column_tag_map

    def _process_tags_expression(self, column_name, body) -> Union[None, Any]:
        if NESTED_COL_EXPR_RE.match(column_name):
            return self.__tag_expr(column_name)
        elif column_name in ['tags_key', 'tags_value']:
            return self.__tags_expr(column_name, body)
        else:
            return None

    def get_tag_column_map(self):
        # And a reverse map from the tags the client expects to the database columns
        return {
            col: dict(map(reversed, trans.items())) for col, trans in self.__column_tag_map.items()
        }

    def __string_col(self, col):
        col_type = self.__columns.get(col, None)
        col_type = str(col_type) if col_type else None

        if col_type and 'String' in col_type and 'FixedString' not in col_type:
            return escape_col(col)
        else:
            return 'toString({})'.format(escape_col(col))

    def __tag_expr(self, column_name):
        """
        Return an expression for the value of a single named tag.

        For tags/contexts, we expand the expression depending on whether the tag is
        "promoted" to a top level column, or whether we have to look in the tags map.
        """
        col, tag = NESTED_COL_EXPR_RE.match(column_name).group(1, 2)

        # For promoted tags, return the column name.
        if col in self.__promoted_columns:
            actual_tag = self.get_tag_column_map()[col].get(tag, tag)
            if actual_tag in self.__promoted_columns[col]:
                return self.__string_col(actual_tag)

        # For the rest, return an expression that looks it up in the nested tags.
        return u'{col}.value[indexOf({col}.key, {tag})]'.format(**{
            'col': col,
            'tag': escape_literal(tag)
        })

    def __tags_expr(self, column_name, body):
        """
        Return an expression that array-joins on tags to produce an output with one
        row per tag.
        """
        assert column_name in ['tags_key', 'tags_value']
        col, k_or_v = column_name.split('_', 1)
        nested_tags_only = state.get_config('nested_tags_only', 1)

        # Generate parallel lists of keys and values to arrayJoin on
        if nested_tags_only:
            key_list = '{}.key'.format(col)
            val_list = '{}.value'.format(col)
        else:
            promoted = self.__promoted_columns[col]
            col_map = self.__column_tag_map[col]
            key_list = u'arrayConcat([{}], {}.key)'.format(
                u', '.join(u'\'{}\''.format(col_map.get(p, p)) for p in promoted),
                col
            )
            val_list = u'arrayConcat([{}], {}.value)'.format(
                ', '.join(self.__string_col(p) for p in promoted),
                col
            )

        cols_used = all_referenced_columns(body) & set(['tags_key', 'tags_value'])
        if len(cols_used) == 2:
            # If we use both tags_key and tags_value in this query, arrayjoin
            # on (key, value) tag tuples.
            expr = (u'arrayJoin(arrayMap((x,y) -> [x,y], {}, {}))').format(
                key_list,
                val_list
            )

            # put the all_tags expression in the alias cache so we can use the alias
            # to refer to it next time (eg. 'all_tags[1] AS tags_key'). instead of
            # expanding the whole tags expression again.
            expr = alias_expr(expr, 'all_tags', body)
            return u'({})[{}]'.format(expr, 1 if k_or_v == 'key' else 2)
        else:
            # If we are only ever going to use one of tags_key or tags_value, don't
            # bother creating the k/v tuples to arrayJoin on, or the all_tags alias
            # to re-use as we won't need it.
            return 'arrayJoin({})'.format(key_list if k_or_v == 'key' else val_list)
