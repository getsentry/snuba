from snuba import util
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_replacer import ErrorsReplacer, ReplacerState
from snuba.datasets.events_processor import EventsProcessor
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.events_common import (
    all_columns,
    get_promoted_tags,
    get_tag_column_map,
    mandatory_conditions,
    promoted_tag_columns,
    query_processors,
    query_splitters,
    required_columns,
)
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.utils.streams.topics import Topic


schema = WritableTableSchema(
    columns=all_columns,
    local_table_name="sentry_local",
    dist_table_name="sentry_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=mandatory_conditions,
    part_format=[util.PartSegment.DATE, util.PartSegment.RETENTION_DAYS],
)


storage = WritableTableStorage(
    storage_key=StorageKey.EVENTS,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=query_processors,
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=EventsProcessor(promoted_tag_columns),
        default_topic=Topic.EVENTS,
        replacement_topic=Topic.EVENT_REPLACEMENTS_LEGACY,
        commit_log_topic=Topic.COMMIT_LOG,
    ),
    query_splitters=query_splitters,
    replacer_processor=ErrorsReplacer(
        schema=schema,
        required_columns=[col.escaped for col in required_columns],
        tag_column_map=get_tag_column_map(),
        promoted_tags=get_promoted_tags(),
        state_name=ReplacerState.EVENTS,
        use_promoted_prewhere=True,
    ),
)
