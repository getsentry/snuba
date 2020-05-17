from typing import Mapping, NamedTuple

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


NestedMappingSpec = Mapping[str, PromotedColumnsSpec]


class MappingColumnPromoter(QueryProcessor):
    """
    Promotes mapping expressions by replacing them with the corresponding promoted
    column.

    Example: tags["myTag"] -> arrayElement("tags.value", indexOf("tags.key", "myTag"))
     -> toString(promoted_MyTag)

    if there is a promoted_MyTag column in the storage that maps to the myTag tag.
    """

    def __init__(self, columns: ColumnSet, mapping_spec: NestedMappingSpec) -> None:
        # The ColumnSet of the dataset. Used to format promoted
        # columns with the right type.
        self.__columns = columns
        # Maps nested column keys to real column names.
        self.__spec = mapping_spec

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def transform_nested_column(exp: Expression) -> Expression:
            # TODO: use the matcher system developed in #942 when merged to make this
            # less verbose and put the code that generates this expression (in #936) in
            # the same place as the matcher expression so they do not diverge.
            #
            # Now it identifies arrayElement("tags.value", indexOf("tags.key", "myTag"))
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
                return exp

            val_column = exp.parameters[0]
            val_column_splits = val_column.column_name.split(".", 2)

            key_column = exp.parameters[1].parameters[0]
            key_column_splits = key_column.column_name.split(".", 2)

            if not (
                len(val_column_splits) == 2
                and len(key_column_splits) == 2
                and val_column_splits[0] == key_column_splits[0]
            ):
                return exp

            key = exp.parameters[1].parameters[1]
            if not isinstance(key, str):
                return exp

            column_name = key_column_splits[0]
            if (
                column_name in self.__spec
                and val_column_splits[1] == self.__spec[column_name].val_field
                and key_column_splits[1] == self.__spec[column_name].key_field
            ):
                promoted_col = self.__spec[column_name].column_mapping.get(key.value)
                if promoted_col:
                    col_type = self.__columns.get(promoted_col, None)
                    col_type_name = str(col_type) if col_type else None

                    ret_col = Column(exp.alias, promoted_col, key_column.table_name)
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
