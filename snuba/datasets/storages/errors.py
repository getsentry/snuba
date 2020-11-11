from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_processor import ErrorsProcessor
from snuba.datasets.errors_replacer import ErrorsReplacer, ReplacerState
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.errors_common import (
    all_columns,
    mandatory_conditions,
    prewhere_candidates,
    promoted_tag_columns,
    query_processors,
    query_splitters,
    required_columns,
)
from snuba.datasets.table_storage import KafkaStreamLoader

schema = WritableTableSchema(
    columns=all_columns,
    local_table_name="errors_local",
    dist_table_name="errors_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=mandatory_conditions,
    prewhere_candidates=prewhere_candidates,
)

storage = WritableTableStorage(
    storage_key=StorageKey.ERRORS,
    storage_set_key=StorageSetKey.EVENTS,
    schema=schema,
    query_processors=query_processors,
    query_splitters=query_splitters,
    stream_loader=KafkaStreamLoader(
        processor=ErrorsProcessor(promoted_tag_columns),
        default_topic="events",
        replacement_topic="errors-replacements",
    ),
    replacer_processor=ErrorsReplacer(
        schema=schema,
        required_columns=required_columns,
        tag_column_map={"tags": promoted_tag_columns, "contexts": {}},
        promoted_tags={"tags": list(promoted_tag_columns.keys()), "contexts": []},
        state_name=ReplacerState.ERRORS,
        use_promoted_prewhere=False,
    ),
)
