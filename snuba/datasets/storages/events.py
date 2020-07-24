from typing import Mapping, Sequence

from snuba.clickhouse.columns import ColumnType
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.errors_replacer import ErrorsReplacer, ReplacerState
from snuba.datasets.events_processor import EventsProcessor
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey

from snuba.datasets.storages.events_common import (
    all_columns,
    get_promoted_tags,
    get_tag_column_map,
    mandatory_conditions,
    prewhere_candidates,
    promoted_tag_columns,
    query_processors,
    query_splitters,
    required_columns,
    TAGS_HASH_MAP_COLUMN,
)

from snuba.datasets.table_storage import KafkaStreamLoader


def events_migrations(
    clickhouse_table: str, current_schema: Mapping[str, ColumnType]
) -> Sequence[str]:
    # Add/remove known migrations
    ret = []
    if "group_id" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN group_id UInt64 DEFAULT 0" % clickhouse_table
        )

    if "device_model" in current_schema:
        ret.append("ALTER TABLE %s DROP COLUMN device_model" % clickhouse_table)

    if "sdk_integrations" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN sdk_integrations Array(String)"
            % clickhouse_table
        )

    if "modules.name" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN modules Nested(name String, version String)"
            % clickhouse_table
        )

    if "culprit" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN culprit Nullable(String)" % clickhouse_table
        )

    if "search_message" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN search_message Nullable(String)"
            % clickhouse_table
        )

    if "title" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN title Nullable(String)" % clickhouse_table
        )

    if "location" not in current_schema:
        ret.append(
            "ALTER TABLE %s ADD COLUMN location Nullable(String)" % clickhouse_table
        )

    if "_tags_flattened" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN _tags_flattened String DEFAULT '' AFTER tags"
        )

    if "message_timestamp" not in current_schema:
        ret.append(
            f"ALTER TABLE {clickhouse_table} ADD COLUMN message_timestamp DateTime AFTER partition"
        )

    if "_tags_hash_map" not in current_schema:
        ret.append(
            (
                f"ALTER TABLE {clickhouse_table} ADD COLUMN _tags_hash_map Array(UInt64) "
                f"MATERIALIZED {TAGS_HASH_MAP_COLUMN} AFTER _tags_flattened"
            )
        )

    return ret


sample_expr = "cityHash64(toString(event_id))"

schema = ReplacingMergeTreeSchema(
    columns=all_columns,
    local_table_name="sentry_local",
    dist_table_name="sentry_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=mandatory_conditions,
    prewhere_candidates=prewhere_candidates,
    order_by="(project_id, toStartOfDay(timestamp), %s)" % sample_expr,
    partition_by="(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))",
    version_column="deleted",
    sample_expr=sample_expr,
    migration_function=events_migrations,
    # Tags hashmap is a materialized column. Clickhouse does not allow
    # us to create a materialized column that references a nested one
    # during create statement
    # (https://github.com/ClickHouse/ClickHouse/issues/12586), so the
    # materialization is added with a migration.
    skipped_cols_on_creation={"_tags_hash_map"},
)


storage = WritableTableStorage(
    storage_key=StorageKey.EVENTS,
    storage_set_key=StorageSetKey.EVENTS,
    schemas=StorageSchemas(read_schema=schema, write_schema=schema),
    query_processors=query_processors,
    stream_loader=KafkaStreamLoader(
        processor=EventsProcessor(promoted_tag_columns),
        default_topic="events",
        replacement_topic="event-replacements",
        commit_log_topic="snuba-commit-log",
    ),
    query_splitters=query_splitters,
    replacer_processor=ErrorsReplacer(
        write_schema=schema,
        read_schema=schema,
        required_columns=[col.escaped for col in required_columns],
        tag_column_map=get_tag_column_map(),
        promoted_tags=get_promoted_tags(),
        state_name=ReplacerState.EVENTS,
    ),
)
