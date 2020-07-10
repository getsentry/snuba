from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.storage import ReadableTableStorage
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storages import StorageKey

from snuba.datasets.storages.events_common import (
    all_columns,
    mandatory_conditions,
    prewhere_candidates,
    query_processors,
    query_splitters,
    sample_expr,
)


schema = ReplacingMergeTreeSchema(
    columns=all_columns,
    local_table_name="sentry_local",
    dist_table_name="sentry_dist_ro",
    storage_set_key=StorageSetKey.EVENTS_RO,
    mandatory_conditions=mandatory_conditions,
    prewhere_candidates=prewhere_candidates,
    order_by="(project_id, toStartOfDay(timestamp), %s)" % sample_expr,
    partition_by="(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))",
    version_column="deleted",
    sample_expr=sample_expr,
)

storage = ReadableTableStorage(
    storage_key=StorageKey.EVENTS_RO,
    storage_set_key=StorageSetKey.EVENTS_RO,
    schemas=StorageSchemas(read_schema=schema, write_schema=None),
    query_processors=query_processors,
    query_splitters=query_splitters,
)
