from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages import StorageKey

from snuba.datasets.storages.errors_common import (
    all_columns,
    mandatory_conditions,
    prewhere_candidates,
    query_processors,
    query_splitters,
)


schema = TableSchema(
    columns=all_columns,
    local_table_name="errors_local",
    dist_table_name="errors_dist_ro",
    storage_set_key=StorageSetKey.EVENTS_RO,
    mandatory_conditions=mandatory_conditions,
    prewhere_candidates=prewhere_candidates,
)

storage = ReadableTableStorage(
    storage_key=StorageKey.ERRORS_RO,
    storage_set_key=StorageSetKey.EVENTS_RO,
    schema=schema,
    query_processors=query_processors,
    query_splitters=query_splitters,
)
