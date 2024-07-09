from typing import Sequence

from snuba.clickhouse.query import Query
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class ColumnFilterProcessor(ClickhouseQueryProcessor):
    """
    This processor should check the WHERE clause for the delete to make sure that it has the appropriate columns to filter by when deleting.
    If the storage has multiple tables to delete from, the columns should be valid for all the tables (e.g. raw and aggregated)
    """

    def __init__(self, column_filters: Sequence[str]) -> None:
        self.__column_filters = column_filters

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        pass
