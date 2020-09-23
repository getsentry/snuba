from snuba.clickhouse.columns import ColumnSet, DateTime, NullableOld as Nullable, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.cdc import CdcStorage
from snuba.datasets.cdc.groupedmessage_processor import (
    GroupedMessageProcessor,
    GroupedMessageRow,
)
from snuba.datasets.cdc.message_filters import CdcTableNameMessageFilter
from snuba.datasets.schemas import MandatoryCondition
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal

columns = ColumnSet(
    [
        # columns to maintain the dataset
        # Kafka topic offset
        ("offset", UInt(64)),
        # GroupStatus in Sentry does not have a 'DELETED' state that reflects the deletion
        # of the record. Having a dedicated clickhouse-only flag to identify this case seems
        # more consistent than add an additional value into the status field below that does not
        # exists on the Sentry side.
        ("record_deleted", UInt(8)),
        # PG columns
        ("project_id", UInt(64)),
        ("id", UInt(64)),
        ("status", Nullable(UInt(8))),
        ("last_seen", Nullable(DateTime())),
        ("first_seen", Nullable(DateTime())),
        ("active_at", Nullable(DateTime())),
        ("first_release_id", Nullable(UInt(64))),
    ]
)

schema = ReplacingMergeTreeSchema(
    columns=columns,
    local_table_name="groupedmessage_local",
    dist_table_name="groupedmessage_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=[
        MandatoryCondition(
            ("record_deleted", "=", 0),
            binary_condition(
                None,
                ConditionFunctions.EQ,
                Column(None, None, "record_deleted"),
                Literal(None, 0),
            ),
        )
    ],
    prewhere_candidates=["project_id", "id"],
    order_by="(project_id, id)",
    partition_by=None,
    version_column="offset",
    sample_expr="id",
)

POSTGRES_TABLE = "sentry_groupedmessage"

storage = CdcStorage(
    storage_key=StorageKey.GROUPEDMESSAGES,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=[],
    stream_loader=KafkaStreamLoader(
        processor=GroupedMessageProcessor(POSTGRES_TABLE),
        default_topic="cdc",
        pre_filter=CdcTableNameMessageFilter(POSTGRES_TABLE),
    ),
    default_control_topic="cdc_control",
    postgres_table=POSTGRES_TABLE,
    row_processor=lambda row: GroupedMessageRow.from_bulk(row).to_clickhouse(),
)
