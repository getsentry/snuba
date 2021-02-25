from snuba.clickhouse.columns import ColumnSet, DateTime
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.cdc import CdcStorage
from snuba.datasets.cdc.groupassignee_processor import (
    GroupAssigneeProcessor,
    GroupAssigneeRow,
)
from snuba.datasets.cdc.message_filters import CdcTableNameMessageFilter
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
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
        ("date_added", DateTime(Modifiers(nullable=True))),
        ("user_id", UInt(64, Modifiers(nullable=True))),
        ("team_id", UInt(64, Modifiers(nullable=True))),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="groupassignee_local",
    dist_table_name="groupassignee_dist",
    storage_set_key=StorageSetKey.EVENTS,
)

POSTGRES_TABLE = "sentry_groupasignee"

storage = CdcStorage(
    storage_key=StorageKey.GROUPASSIGNEES,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=[PrewhereProcessor(["project_id"])],
    stream_loader=build_kafka_stream_loader_from_settings(
        StorageKey.GROUPASSIGNEES,
        processor=GroupAssigneeProcessor(POSTGRES_TABLE),
        default_topic_name="cdc",
        pre_filter=CdcTableNameMessageFilter(POSTGRES_TABLE),
    ),
    default_control_topic="cdc_control",
    postgres_table=POSTGRES_TABLE,
    row_processor=lambda row: GroupAssigneeRow.from_bulk(row).to_clickhouse(),
)
