from typing import Mapping, NamedTuple, Optional

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.clickhouse.translators.snuba.mappers import (
    KEY_COL_TAG_PARAM,
    KEY_TAG_PARAM,
    TABLE_TAG_PARAM,
    VALUE_COL_TAG_PARAM,
    tag_pattern,
)
from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.request.request_settings import RequestSettings


class SubscriptableMatch(NamedTuple):
    # The table name associated with the nested column found in the query.
    table_name: Optional[str]
    # The nested column name
    column_name: str
    # The key found in the query (like key in tags[key])
    key: str


def match_subscriptable_reference(exp: Expression) -> Optional[SubscriptableMatch]:
    """
    Finds the expression, in the Clickhouse query, that loads the value
    of a specific tag (or any nested column that represents a mapping,
    like contexts).
    It builds a SubscriptableMatch object to be used by the code that
    processes such expression for convenience.

    This is the shape of the expression matched here:
    `arrayElement("tags.value", indexOf("tags.key", "myTag"))`
    The key field must be called `key` and the value field must be
    called `value`.
    """

    match = tag_pattern.match(exp)
    if match is None:
        return None

    # TODO: There is should be a structured Column class (#963 - #966)
    # to deal with references to nested columns instead of splitting
    # the column name string.

    value_col_split = match.string(VALUE_COL_TAG_PARAM).split(".", 2)
    key_col_split = match.string(KEY_COL_TAG_PARAM).split(".", 2)

    if len(value_col_split) != 2 or len(key_col_split) != 2:
        return None

    val_column, val_field = value_col_split
    key_column, key_field = key_col_split

    if val_column != key_column or key_field != "key" or val_field != "value":
        return None

    table_name = match.scalar(TABLE_TAG_PARAM)
    return SubscriptableMatch(
        table_name=str(table_name) if table_name is not None else None,
        column_name=val_column,
        key=match.string(KEY_TAG_PARAM),
    )


class MappingColumnPromoter(QueryProcessor):
    """
    Promotes expressions that access the value of a mapping column by
    replacing them with the corresponding promoted column provided in
    the constructor.

    Example: tags["myTag"]
        -> arrayElement("tags.value", indexOf("tags.key", "myTag"))
        -> toString(promoted_MyTag)

    This happens if there is a promoted_MyTag column in the storage
    that maps to the myTag tag.
    """

    def __init__(self, mapping_specs: Mapping[str, Mapping[str, str]]) -> None:
        # The configuration for this processor. The key of the
        # mapping is the name of the nested column. The value is
        # a mapping between key in the mapping column and promoted
        # column name.
        self.__specs = mapping_specs

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def transform_nested_column(exp: Expression) -> Expression:
            subscript = match_subscriptable_reference(exp)
            if subscript is None:
                return exp

            if subscript.column_name in self.__specs:
                promoted_col_name = self.__specs[subscript.column_name].get(
                    subscript.key
                )
                if promoted_col_name:
                    col_type = (
                        query.get_data_source()
                        .get_columns()
                        .get(promoted_col_name, None)
                    )
                    col_type_name = str(col_type) if col_type else None

                    ret_col = Column(exp.alias, subscript.table_name, promoted_col_name)
                    # We need to pass the content of the promoted column to a toString
                    # function when the promoted column is not a string since the
                    # supported values of mapping columns are strings and the clients
                    # expect such.
                    if (
                        col_type_name
                        and "String" in col_type_name
                        and "FixedString" not in col_type_name
                    ):
                        return ret_col
                    else:
                        return FunctionCall(exp.alias, "toString", (ret_col,))

            return exp

        query.transform_expressions(transform_nested_column)
