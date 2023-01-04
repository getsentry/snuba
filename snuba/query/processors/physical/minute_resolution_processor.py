from datetime import timedelta

from snuba.clickhouse.query import Query
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.query.exceptions import ValidationException
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class MinuteResolutionProcessor(ClickhouseQueryProcessor):
    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        # NOTE: the product side is restricted to a 6h window, however it rounds
        # outwards, which extends the window to 7h.
        from_date, to_date = get_time_range(query, "started")
        if not from_date or not to_date or (to_date - from_date) > timedelta(hours=7):
            raise ValidationException(
                "Minute-resolution queries are restricted to a 7-hour time window."
            )
