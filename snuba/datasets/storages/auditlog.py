from snuba.clickhouse.columns import UUID, ColumnSet, DateTime, String, UInt
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.auditlog_processor import AuditlogProcessor
from snuba.datasets.message_filters import KafkaHeaderFilterWithBypass
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic

columns = ColumnSet(
    [
        ("event_id", UUID()),
        ("timestamp", DateTime()),
        ("event_type", String()),
        ("user", String()),
        ("details", String()),
        ("project_id", UInt(64)),
    ]
)

schema = WritableTableSchema(
    columns=columns,
    local_table_name="audit_log_local",
    dist_table_name="audit_log_dist",
    storage_set_key=StorageSetKey.AUDIT_LOG,
)

storage = WritableTableStorage(
    storage_key=StorageKey.AUDIT_LOG,
    storage_set_key=StorageSetKey.AUDIT_LOG,
    schema=schema,
    query_processors=[],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=AuditlogProcessor(),
        default_topic=Topic.AUDIT_LOG,
        commit_log_topic=Topic.AUDIT_LOG_COMMIT,
        pre_filter=KafkaHeaderFilterWithBypass("transaction_forwarder", "0", 100),
        subscription_result_topic=Topic.AUDIT_LOG_RESULTS,
        subscription_scheduled_topic=Topic.AUDIT_LOG_SUBSCRIPTION,
        subscription_scheduler_mode=SchedulingWatermarkMode.GLOBAL,
    ),
)
