from collections import ChainMap
from typing import FrozenSet, Mapping, Sequence

from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    ColumnType,
    DateTime,
    FixedString,
    Float,
    Nested,
    Nullable,
    String,
    UInt,
)
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.errors_replacer import ErrorsReplacer, ReplacerState
from snuba.datasets.events_processor import EventsProcessor
from snuba.datasets.schemas import MandatoryCondition
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.events_bool_contexts import EventsBooleanContextsProcessor
from snuba.datasets.storages.events_column_processor import EventsColumnProcessor
from snuba.datasets.storages.processors.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.datasets.table_storage import KafkaStreamLoader
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.readonly_events import ReadOnlyTableSelector
from snuba.web.split import ColumnSplitQueryStrategy, TimeSplitQueryStrategy


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

    return ret


metadata_columns = ColumnSet(
    [
        # optional stream related data
        ("offset", Nullable(UInt(64))),
        ("partition", Nullable(UInt(16))),
        ("message_timestamp", DateTime()),
    ]
)

promoted_tag_columns = ColumnSet(
    [
        # These are the classic tags, they are saved in Snuba exactly as they
        # appear in the event body.
        ("level", Nullable(String())),
        ("logger", Nullable(String())),
        ("server_name", Nullable(String())),  # future name: device_id?
        ("transaction", Nullable(String())),
        ("environment", Nullable(String())),
        ("sentry:release", Nullable(String())),
        ("sentry:dist", Nullable(String())),
        ("sentry:user", Nullable(String())),
        ("site", Nullable(String())),
        ("url", Nullable(String())),
    ]
)

promoted_context_tag_columns = ColumnSet(
    [
        # These are promoted tags that come in in `tags`, but are more closely
        # related to contexts.  To avoid naming confusion with Clickhouse nested
        # columns, they are stored in the database with s/./_/
        # promoted tags
        ("app_device", Nullable(String())),
        ("device", Nullable(String())),
        ("device_family", Nullable(String())),
        ("runtime", Nullable(String())),
        ("runtime_name", Nullable(String())),
        ("browser", Nullable(String())),
        ("browser_name", Nullable(String())),
        ("os", Nullable(String())),
        ("os_name", Nullable(String())),
        ("os_rooted", Nullable(UInt(8))),
    ]
)

promoted_context_columns = ColumnSet(
    [
        ("os_build", Nullable(String())),
        ("os_kernel_version", Nullable(String())),
        ("device_name", Nullable(String())),
        ("device_brand", Nullable(String())),
        ("device_locale", Nullable(String())),
        ("device_uuid", Nullable(String())),
        ("device_model_id", Nullable(String())),
        ("device_arch", Nullable(String())),
        ("device_battery_level", Nullable(Float(32))),
        ("device_orientation", Nullable(String())),
        ("device_simulator", Nullable(UInt(8))),
        ("device_online", Nullable(UInt(8))),
        ("device_charging", Nullable(UInt(8))),
    ]
)

required_columns = ColumnSet(
    [
        ("event_id", FixedString(32)),
        ("project_id", UInt(64)),
        ("group_id", UInt(64)),
        ("timestamp", DateTime()),
        ("deleted", UInt(8)),
        ("retention_days", UInt(16)),
    ]
)

all_columns = (
    required_columns
    + [
        # required for non-deleted
        ("platform", Nullable(String())),
        ("message", Nullable(String())),
        ("primary_hash", Nullable(FixedString(32))),
        ("received", Nullable(DateTime())),
        ("search_message", Nullable(String())),
        ("title", Nullable(String())),
        ("location", Nullable(String())),
        # optional user
        ("user_id", Nullable(String())),
        ("username", Nullable(String())),
        ("email", Nullable(String())),
        ("ip_address", Nullable(String())),
        # optional geo
        ("geo_country_code", Nullable(String())),
        ("geo_region", Nullable(String())),
        ("geo_city", Nullable(String())),
        ("sdk_name", Nullable(String())),
        ("sdk_version", Nullable(String())),
        ("type", Nullable(String())),
        ("version", Nullable(String())),
    ]
    + metadata_columns
    + promoted_context_columns
    + promoted_tag_columns
    + promoted_context_tag_columns
    + [
        # other tags
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_flattened", String()),
        # other context
        ("contexts", Nested([("key", String()), ("value", String())])),
        # http interface
        ("http_method", Nullable(String())),
        ("http_referer", Nullable(String())),
        # exception interface
        (
            "exception_stacks",
            Nested(
                [
                    ("type", Nullable(String())),
                    ("value", Nullable(String())),
                    ("mechanism_type", Nullable(String())),
                    ("mechanism_handled", Nullable(UInt(8))),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", Nullable(String())),
                    ("filename", Nullable(String())),
                    ("package", Nullable(String())),
                    ("module", Nullable(String())),
                    ("function", Nullable(String())),
                    ("in_app", Nullable(UInt(8))),
                    ("colno", Nullable(UInt(32))),
                    ("lineno", Nullable(UInt(32))),
                    ("stack_level", UInt(16)),
                ]
            ),
        ),
        # These are columns we added later in the life of the (current) production
        # database. They don't necessarily belong here in a logical/readability sense
        # but they are here to match the order of columns in production becase
        # `insert_distributed_sync` is very sensitive to column existence and ordering.
        ("culprit", Nullable(String())),
        ("sdk_integrations", Array(String())),
        ("modules", Nested([("name", String()), ("version", String())])),
    ]
)

sample_expr = "cityHash64(toString(event_id))"
schema = ReplacingMergeTreeSchema(
    columns=all_columns,
    local_table_name="sentry_local",
    dist_table_name="sentry_dist",
    storage_set_key=StorageSetKey.EVENTS,
    mandatory_conditions=[
        MandatoryCondition(
            ("deleted", "=", 0),
            binary_condition(
                None,
                ConditionFunctions.EQ,
                Column(None, None, "deleted"),
                Literal(None, 0),
            ),
        )
    ],
    prewhere_candidates=[
        "event_id",
        "group_id",
        "tags[sentry:release]",
        "message",
        "title",
        "environment",
        "project_id",
    ],
    order_by="(project_id, toStartOfDay(timestamp), %s)" % sample_expr,
    partition_by="(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))",
    version_column="deleted",
    sample_expr=sample_expr,
    migration_function=events_migrations,
)


def get_promoted_columns() -> Mapping[str, FrozenSet[str]]:
    # The set of columns, and associated keys that have been promoted
    # to the top level table namespace.
    return {
        "tags": frozenset(
            col.flattened
            for col in (promoted_tag_columns + promoted_context_tag_columns)
        ),
        "contexts": frozenset(col.flattened for col in promoted_context_columns),
    }


def get_column_tag_map() -> Mapping[str, Mapping[str, str]]:
    # For every applicable promoted column,  a map of translations from the column
    # name  we save in the database to the tag we receive in the query.

    return {
        "tags": {
            col.flattened: col.flattened.replace("_", ".")
            for col in promoted_context_tag_columns
        },
        "contexts": {
            col.flattened: col.flattened.replace("_", ".", 1)
            for col in promoted_context_columns
        },
    }


def get_tag_column_map() -> Mapping[str, Mapping[str, str]]:
    # And a reverse map from the tags the client expects to the database columns
    return {
        col: dict(map(reversed, trans.items()))
        for col, trans in get_column_tag_map().items()
    }


def get_promoted_tags() -> Mapping[str, Sequence[str]]:
    # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
    # and they can/will use a promoted column.
    return {
        col: [get_column_tag_map()[col].get(x, x) for x in get_promoted_columns()[col]]
        for col in get_promoted_columns()
    }


storage = WritableTableStorage(
    storage_key=StorageKey.EVENTS,
    storage_set_key=StorageSetKey.EVENTS,
    schemas=StorageSchemas(read_schema=schema, write_schema=schema),
    query_processors=[
        PostReplacementConsistencyEnforcer(
            project_column="project_id",
            # key migration is on going. As soon as all the keys we are interested
            # into in redis are stored with "EVENTS" in the name, we can change this.
            replacer_state_name=None,
        ),
        # TODO: This one should become an entirely separate storage and picked
        # in the storage selector.
        ReadOnlyTableSelector("sentry_dist", "sentry_dist_ro"),
        EventsColumnProcessor(),
        MappingColumnPromoter(
            mapping_specs={
                "tags": ChainMap(
                    {col.flattened: col.flattened for col in promoted_tag_columns},
                    {
                        col.flattened.replace("_", ".", 1): col.flattened
                        for col in promoted_context_tag_columns
                    },
                ),
                "contexts": {
                    col.flattened.replace("_", ".", 1): col.flattened
                    for col in promoted_context_columns
                },
            },
        ),
        # This processor must not be ported to the errors dataset. We should
        # not support promoting tags/contexts with boolean values. There is
        # no way to convert them back consistently to the value provided by
        # the client when the event is ingested, in all ways to access
        # tags/contexts. Once the errors dataset is in use, we will not have
        # boolean promoted tags/contexts so this constraint will be easy
        # to enforce.
        EventsBooleanContextsProcessor(),
        ArrayJoinKeyValueOptimizer("tags"),
        PrewhereProcessor(),
    ],
    stream_loader=KafkaStreamLoader(
        processor=EventsProcessor(promoted_tag_columns),
        default_topic="events",
        replacement_topic="event-replacements",
        commit_log_topic="snuba-commit-log",
    ),
    query_splitters=[
        ColumnSplitQueryStrategy(
            id_column="event_id",
            project_column="project_id",
            timestamp_column="timestamp",
        ),
        TimeSplitQueryStrategy(timestamp_col="timestamp"),
    ],
    replacer_processor=ErrorsReplacer(
        write_schema=schema,
        read_schema=schema,
        required_columns=[col.escaped for col in required_columns],
        tag_column_map=get_tag_column_map(),
        promoted_tags=get_promoted_tags(),
        state_name=ReplacerState.EVENTS,
    ),
)
