from datetime import timedelta
from typing import Sequence

from snuba import environment
from snuba.clickhouse.query_dsl.accessors import get_time_range
from snuba.datasets.entities.storage_selectors import QueryStorageSelector
from snuba.datasets.storage import (
    EntityStorageConnection,
    EntityStorageConnectionNotFound,
    ReadableTableStorage,
    WritableTableStorage,
)
from snuba.query.logical import Query
from snuba.query.processors.logical.timeseries_processor import (
    extract_granularity_from_query,
)
from snuba.query.query_settings import QuerySettings, SubscriptionQuerySettings
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api.sessions")


class SessionsQueryStorageSelector(QueryStorageSelector):
    def select_storage(
        self,
        query: Query,
        query_settings: QuerySettings,
        storage_connections: Sequence[EntityStorageConnection],
    ) -> EntityStorageConnection:
        # If the passed in `query_settings` arg is an instance of `SubscriptionQuerySettings`,
        # then it is a crash rate alert subscription, and hence we decide on whether to use the
        # materialized storage or the raw storage by examining the time_window.
        # If the `time_window` <=1h, then select the raw storage otherwise select materialized
        # storage
        # NOTE: This storage selector does not support multiple readable storages.
        # NOTE: If we were to support other types of subscriptions over the sessions dataset that
        # do not follow this method used to identify which storage to use, we would need to
        # find a different way to distinguish them.
        if isinstance(query_settings, SubscriptionQuerySettings):
            from_date, to_date = get_time_range(query, "started")
            if from_date and to_date:
                use_materialized_storage = to_date - from_date > timedelta(hours=1)
            else:
                use_materialized_storage = True
        else:
            granularity = extract_granularity_from_query(query, "started") or 3600
            use_materialized_storage = granularity >= 3600 and (granularity % 3600) == 0

        metrics.increment(
            "query.selector",
            tags={
                "selected_storage": "materialized"
                if use_materialized_storage
                else "raw",
            },
        )
        if use_materialized_storage:
            for storage_connection in storage_connections:
                if (
                    not storage_connection.is_writable
                    and type(storage_connection.storage) is ReadableTableStorage
                ):
                    return storage_connection
        else:
            for storage_connection in storage_connections:
                if storage_connection.is_writable and isinstance(
                    storage_connection.storage, WritableTableStorage
                ):
                    return storage_connection
        raise EntityStorageConnectionNotFound("Cannot find storage.")
