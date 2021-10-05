from typing import Dict, Sequence, Set

from snuba.clickhouse.columns import SchemaModifiers
from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.datasets.storage import ReadableTableStorage
from snuba.request.request_settings import RequestSettings


class NullColumnCaster(QueryProcessor):
    def _find_mismatched_null_columns(self) -> Set[str]:
        res = set()
        col_name_to_nullable: Dict[str, bool] = {}
        for table_storage in self.__merge_table_sources:
            for col in table_storage.get_schema().get_columns():
                col_is_nullable = False
                modifiers = col.type.get_modifiers()
                if isinstance(modifiers, SchemaModifiers):
                    col_is_nullable = modifiers.nullable
                other_storage_column_is_nullable = col_name_to_nullable.get(
                    col.name, None
                )
                if (
                    other_storage_column_is_nullable is not None
                    and other_storage_column_is_nullable != col_is_nullable
                ):
                    res.add(col.name)
                col_name_to_nullable[col.name] = col_is_nullable

        return res

    def __init__(self, merge_table_sources: Sequence[ReadableTableStorage]):
        self.__merge_table_sources = merge_table_sources
        self._mismatched_null_columns = self._find_mismatched_null_columns()

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        pass
