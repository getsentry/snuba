from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.transactions_common import (
    columns,
    mandatory_condition_checkers,
    query_processors,
    query_splitters,
)

schema = TableSchema(
    columns=columns,
    local_table_name="transactions_local",
    dist_table_name="transactions_dist",
    storage_set_key=StorageSetKey.TRANSACTIONS_RO,
    mandatory_conditions=[],
)

storage = ReadableTableStorage(
    storage_key=StorageKey.TRANSACTIONS_RO,
    storage_set_key=StorageSetKey.TRANSACTIONS_RO,
    schema=schema,
    query_processors=query_processors,
    query_splitters=query_splitters,
    mandatory_condition_checkers=mandatory_condition_checkers,
)
