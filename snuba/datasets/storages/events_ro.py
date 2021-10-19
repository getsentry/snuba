from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey

from snuba.datasets.storages.events_common import (
    all_columns,
    mandatory_conditions,
    query_processors,
    query_splitters,
)


schema = TableSchema(
    columns=all_columns,
    local_table_name="sentry_local",
    dist_table_name="sentry_dist_ro",
    storage_set_key=StorageSetKey.EVENTS_RO,
    mandatory_conditions=mandatory_conditions,
)

storage = ReadableTableStorage(
    storage_key=StorageKey.EVENTS_RO,
    storage_set_key=StorageSetKey.EVENTS_RO,
    schema=schema,
    query_processors=query_processors,
    query_splitters=query_splitters,
)
