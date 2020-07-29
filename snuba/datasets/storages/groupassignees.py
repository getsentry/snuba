from snuba.clickhouse.columns import ColumnSet, DateTime, Nullable, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)
from snuba.datasets.cdc.message_filters import CdcTableNameMessageFilter
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.cdc import CdcStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.processors.prewhere import PrewhereProcessor


columns = ColumnSet(
    [
        # columns to maintain the dataset
        # Kafka topic offset
        ("offset", UInt(64)),
        ("record_deleted", UInt(8)),
        # PG columns
        ("project_id", UInt(64)),
        ("group_id", UInt(64)),
        ("date_added", Nullable(DateTime())),
        ("user_id", Nullable(UInt(64))),
        ("team_id", Nullable(UInt(64))),
    ]
)

schema = ReplacingMergeTreeSchema(
    columns=columns,
    local_table_name="groupassignee_local",
    dist_table_name="groupassignee_dist",
    storage_set_key=StorageSetKey.EVENTS,
    order_by="(project_id, group_id)",
    partition_by=None,
    version_column="offset",
)

POSTGRES_TABLE = "sentry_groupasignee"

storage = CdcStorage(
    storage_key=StorageKey.GROUPASSIGNEES,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=[PrewhereProcessor()],
    stream_loader=KafkaStreamLoader(
        processor=GroupAssigneeProcessor(POSTGRES_TABLE),
        default_topic="cdc",
        pre_filter=CdcTableNameMessageFilter(POSTGRES_TABLE),
    ),
    default_control_topic="cdc_control",
    postgres_table=POSTGRES_TABLE,
    row_processor=lambda row: GroupAssigneeRow.from_bulk(row).to_clickhouse(),
)
