from typing import Any, MutableMapping

from snuba.clickhouse.query import Query
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.state import get_str_config


class ClickhouseSettingsOverride(ClickhouseQueryProcessor):
    """
    Overrides arbitrary clickhouse settings via a dictionary specifying the clickhouse settings to override.

    the `overwrite_existing` parameter controls whether the processor overwrites the clickhouse settings already present on the query
    settings or only updates the ones which are not set.

    E.g.

    processor_settings = {"max_rows_to_group_by": 1, "groupby_overflow_mode": "break"}

    query_settings.clickhouse_settings = {"max_rows_to_group_by": 12345}

    if overwrite_existing == True:
        query_settings.clickhouse_settings = {"max_rows_to_group_by": 1, "groupby_overflow_mode": "break"}
    if overwrite_existing == False:
        query_settings.clickhouse_settings = {"max_rows_to_group_by": 12345, "groupby_overflow_mode": "break"}

    """

    def __init__(
        self, settings: MutableMapping[str, Any], overwrite_existing: bool = True
    ) -> None:
        self.__settings = settings
        self.__overwrite_existing = overwrite_existing

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        new_settings: MutableMapping[str, Any] = {}
        if self.__overwrite_existing:
            new_settings.update(query_settings.get_clickhouse_settings())
            new_settings.update(self.__settings)
        else:
            new_settings.update(self.__settings)
            new_settings.update(query_settings.get_clickhouse_settings())

        if get_str_config("ignore_clickhouse_settings_override", default=""):
            ignored_settings = get_str_config("ignore_clickhouse_settings_override")
            new_settings = {
                setting: value
                for setting, value in new_settings.items()
                if setting not in ignored_settings
            }

        query_settings.set_clickhouse_settings(new_settings)
