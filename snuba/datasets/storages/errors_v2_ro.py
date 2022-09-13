from snuba.clusters.storage_set_key import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages.errors_common import (
    all_columns,
    mandatory_conditions,
    query_splitters,
)
from snuba.datasets.storages.errors_v2 import query_processors
from snuba.datasets.storages.storage_key import StorageKey

schema = TableSchema(
    columns=all_columns,
    local_table_name="errors_local",
    dist_table_name="errors_dist_ro",
    storage_set_key=StorageSetKey.ERRORS_V2_RO,
    mandatory_conditions=mandatory_conditions,
)

storage = ReadableTableStorage(
    storage_key=StorageKey.ERRORS_V2_RO,
    storage_set_key=StorageSetKey.ERRORS_V2_RO,
    schema=schema,
    query_processors=query_processors,
    query_splitters=query_splitters,
)
