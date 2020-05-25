from typing import Mapping, NamedTuple, Optional

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.request.request_settings import RequestSettings


class PromotedColumnsSpec(NamedTuple):
    # The name of the key field in the mapping column
    key_field: str
    # The name of the value field in the mapping column
    val_field: str
    # The mapping between keys in the column and real columns
    column_mapping: Mapping[str, str]


MappingColumnPromotionSpec = Mapping[str, PromotedColumnsSpec]


class SubscriptableMatch(NamedTuple):
    base_table_name: Optional[str]
    base_column: str
    key_field: str
    val_field: str
    key: str


def match_subscriptable_reference(exp: Expression) -> Optional[SubscriptableMatch]:
    if not (
        isinstance(exp, FunctionCall)
        and exp.function_name == "arrayElement"
        and len(exp.parameters) == 2
        and isinstance(exp.parameters[0], Column)
        and isinstance(exp.parameters[1], FunctionCall)
        and exp.parameters[1].function_name == "indexOf"
        and len(exp.parameters[1].parameters) == 2
        and isinstance(exp.parameters[1].parameters[0], Column)
        and isinstance(exp.parameters[1].parameters[1], Literal)
    ):
        return None

    # TODO: There is should be a structured Column class to deal with references
    # to nested columns instead of splitting the column name string. Resolution
    # of such column is tightly coupled to how we will do entity resolution.

    # exp.parameters[0] is a Column with a name like `tags.value`
    value_col_name = exp.parameters[0].column_name.split(".", 2)
    # exp.parameters[1].parameters[0] is a Column with a name like `tags.key`
    key_col_name = exp.parameters[1].parameters[0].column_name.split(".", 2)

    if not (len(value_col_name) == 2 and len(key_col_name) == 2):
        return None

    val_column, val_field = value_col_name
    key_column, key_field = key_col_name

    if val_column != key_column:
        return None

    key_literal = exp.parameters[1].parameters[1]
    if not isinstance(key_literal.value, str):
        return None

    return SubscriptableMatch(
        base_table_name=exp.parameters[0].table_name,
        base_column=val_column,
        key_field=key_field,
        val_field=val_field,
        key=key_literal.value,
    )


class MappingColumnPromoter(QueryProcessor):
    """
    Promotes expression to access the value of a mapping column by replacing them with
    the corresponding promoted column.

    Example: tags["myTag"] -> arrayElement("tags.value", indexOf("tags.key", "myTag"))
     -> toString(promoted_MyTag)

    This happens if there is a promoted_MyTag column in the storage that maps to the myTag tag.
    """

    def __init__(
        self, columns: ColumnSet, mapping_spec: MappingColumnPromotionSpec
    ) -> None:
        # The ColumnSet of the dataset. Used to format promoted
        # columns with the right type.
        self.__columns = columns
        # Maps nested column keys to real column names.
        self.__spec = mapping_spec

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def transform_nested_column(exp: Expression) -> Expression:
            subscript = match_subscriptable_reference(exp)

            if subscript is None:
                return exp

            if (
                subscript.base_column in self.__spec
                and subscript.val_field == self.__spec[subscript.val_field].val_field
                and subscript.key_field == self.__spec[subscript.key_field].key_field
            ):
                promoted_col = self.__spec[subscript.val_field].column_mapping.get(
                    subscript.key
                )
                if promoted_col:
                    col_type = self.__columns.get(promoted_col, None)
                    col_type_name = str(col_type) if col_type else None

                    ret_col = Column(exp.alias, promoted_col, subscript.base_table_name)
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
