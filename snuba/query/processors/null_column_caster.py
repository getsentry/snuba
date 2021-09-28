from snuba.clickhouse.query import Query
from snuba.clickhouse.processors import QueryProcessor
from snuba.request import RequestSettings

class NullColumnCaster(QueryProcessor):


    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
