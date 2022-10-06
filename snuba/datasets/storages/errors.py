from snuba import util
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_replacer import ErrorsReplacer
from snuba.datasets.message_filters import KafkaHeaderWithBypassFilter
from snuba.datasets.processors.errors_processor import ErrorsProcessor
from snuba.datasets.schemas.tables import WritableTableSchema, WriteFormat
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages.errors_common import (
    all_columns,
    mandatory_conditions,
    promoted_tag_columns,
    query_processors,
    query_splitters,
    required_columns,
)
from snuba.datasets.storages.storage_key import StorageKey
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.physical.conditions_enforcer import ProjectIdEnforcer
from snuba.replacers.replacer_processor import ReplacerState
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic

schema = WritableTableSchema(
    columns=all_columns,
    local_table_name="errors_local",
    dist_table_name="errors_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=mandatory_conditions,
    part_format=[util.PartSegment.RETENTION_DAYS, util.PartSegment.DATE],
)

storage = WritableTableStorage(
    storage_key=StorageKey.ERRORS,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=query_processors,
    query_splitters=query_splitters,
    mandatory_condition_checkers=[ProjectIdEnforcer()],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=ErrorsProcessor(promoted_tag_columns),
        pre_filter=KafkaHeaderWithBypassFilter("transaction_forwarder", "1", 100),
        default_topic=Topic.EVENTS,
        replacement_topic=Topic.EVENT_REPLACEMENTS,
        commit_log_topic=Topic.COMMIT_LOG,
        subscription_scheduler_mode=SchedulingWatermarkMode.PARTITION,
        subscription_scheduled_topic=Topic.SUBSCRIPTION_SCHEDULED_EVENTS,
        subscription_result_topic=Topic.SUBSCRIPTION_RESULTS_EVENTS,
    ),
    # This is the default, just showing where it goes for the PR
    write_format=WriteFormat.JSON,
    replacer_processor=ErrorsReplacer(
        schema=schema,
        required_columns=required_columns,
        tag_column_map={"tags": promoted_tag_columns, "contexts": {}},
        promoted_tags={"tags": list(promoted_tag_columns.keys()), "contexts": []},
        state_name=ReplacerState.ERRORS,
    ),
)
