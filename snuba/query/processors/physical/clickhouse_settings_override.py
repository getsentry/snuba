from typing import Any, MutableMapping

from snuba.clickhouse.query import Query
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class ClickhouseSettingsOverride(ClickhouseQueryProcessor):
    """
    Overrides arbitrary clickhouse settings via a dictionary specifying the clickhouse settings to override.
    """

    def __init__(self, settings: MutableMapping[str, Any]) -> None:
        self.__settings = settings

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        new_settings: MutableMapping[str, Any] = {}
        new_settings.update(query_settings.get_clickhouse_settings())
        new_settings.update(self.__settings)
        query_settings.set_clickhouse_settings(new_settings)
