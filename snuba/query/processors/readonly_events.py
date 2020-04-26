from snuba import state
from snuba.clickhouse.query import Query
from snuba.datasets.schemas.tables import TableSource
from snuba.clickhouse.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class ReadOnlyTableSelector(QueryProcessor):
    """
    Replaces the data source in the query with a TableSource if the table
    name of the original datasource is the one provided to the constructor,
    the query is not consistent, and this processor is enabled.
    """

    def __init__(self, table_to_replace: str, read_only_table: str) -> None:
        self.__table_to_replace = table_to_replace
        self.__read_only_table = read_only_table

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        readonly_enabled = state.get_config("enable_events_readonly_table", False)
        if not readonly_enabled:
            return

        if request_settings.get_consistent():
            return

        data_source = query.get_data_source()

        if data_source.format_from() != self.__table_to_replace:
            return

        new_source = TableSource(
            table_name=self.__read_only_table,
            columns=data_source.get_columns(),
            mandatory_conditions=data_source.get_mandatory_conditions(),
            prewhere_candidates=data_source.get_prewhere_candidates(),
        )
        query.set_data_source(new_source)
