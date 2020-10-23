from collections import ChainMap
from typing import FrozenSet, Mapping, Sequence

from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    FixedString,
    Float,
    Nested,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt, nullable, readonly
from snuba.datasets.storages.events_bool_contexts import EventsBooleanContextsProcessor
from snuba.datasets.storages.events_column_processor import EventsColumnProcessor
from snuba.datasets.storages.processors.replaced_groups import (
    PostReplacementConsistencyEnforcer,
)
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.expressions import Column, Literal
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.web.split import ColumnSplitQueryStrategy, TimeSplitQueryStrategy

metadata_columns = ColumnSet(
    [
        # optional stream related data
        ("offset", UInt(64, nullable())),
        ("partition", UInt(16, nullable())),
        ("message_timestamp", DateTime()),
    ]
)

promoted_tag_columns = ColumnSet(
    [
        # These are the classic tags, they are saved in Snuba exactly as they
        # appear in the event body.
        ("level", String(nullable())),
        ("logger", String(nullable())),
        ("server_name", String(nullable())),  # future name: device_id?
        ("transaction", String(nullable())),
        ("environment", String(nullable())),
        ("sentry:release", String(nullable())),
        ("sentry:dist", String(nullable())),
        ("sentry:user", String(nullable())),
        ("site", String(nullable())),
        ("url", String(nullable())),
    ]
)

promoted_context_tag_columns = ColumnSet(
    [
        # These are promoted tags that come in in `tags`, but are more closely
        # related to contexts.  To avoid naming confusion with Clickhouse nested
        # columns, they are stored in the database with s/./_/
        # promoted tags
        ("app_device", String(nullable())),
        ("device", String(nullable())),
        ("device_family", String(nullable())),
        ("runtime", String(nullable())),
        ("runtime_name", String(nullable())),
        ("browser", String(nullable())),
        ("browser_name", String(nullable())),
        ("os", String(nullable())),
        ("os_name", String(nullable())),
        ("os_rooted", UInt(8, nullable())),
    ]
)

promoted_context_columns = ColumnSet(
    [
        ("os_build", String(nullable())),
        ("os_kernel_version", String(nullable())),
        ("device_name", String(nullable())),
        ("device_brand", String(nullable())),
        ("device_locale", String(nullable())),
        ("device_uuid", String(nullable())),
        ("device_model_id", String(nullable())),
        ("device_arch", String(nullable())),
        ("device_battery_level", Float(32, nullable())),
        ("device_orientation", String(nullable())),
        ("device_simulator", UInt(8, nullable())),
        ("device_online", UInt(8, nullable())),
        ("device_charging", UInt(8, nullable())),
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
        ("platform", String(nullable())),
        ("message", String(nullable())),
        ("primary_hash", FixedString(32, nullable())),
        ("received", DateTime(nullable())),
        ("search_message", String(nullable())),
        ("title", String(nullable())),
        ("location", String(nullable())),
        # optional user
        ("user_id", String(nullable())),
        ("username", String(nullable())),
        ("email", String(nullable())),
        ("ip_address", String(nullable())),
        # optional geo
        ("geo_country_code", String(nullable())),
        ("geo_region", String(nullable())),
        ("geo_city", String(nullable())),
        ("sdk_name", String(nullable())),
        ("sdk_version", String(nullable())),
        ("type", String(nullable())),
        ("version", String(nullable())),
    ]
    + metadata_columns
    + promoted_context_columns
    + promoted_tag_columns
    + promoted_context_tag_columns
    + [
        # other tags
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_flattened", String()),
        ("_tags_hash_map", Array(UInt(64), readonly())),
        # other context
        ("contexts", Nested([("key", String()), ("value", String())])),
        # http interface
        ("http_method", String(nullable())),
        ("http_referer", String(nullable())),
        # exception interface
        (
            "exception_stacks",
            Nested(
                [
                    ("type", String(nullable())),
                    ("value", String(nullable())),
                    ("mechanism_type", String(nullable())),
                    ("mechanism_handled", UInt(8, nullable())),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", String(nullable())),
                    ("filename", String(nullable())),
                    ("package", String(nullable())),
                    ("module", String(nullable())),
                    ("function", String(nullable())),
                    ("in_app", UInt(8, nullable())),
                    ("colno", UInt(32, nullable())),
                    ("lineno", UInt(32, nullable())),
                    ("stack_level", UInt(16)),
                ]
            ),
        ),
        # These are columns we added later in the life of the (current) production
        # database. They don't necessarily belong here in a logical/readability sense
        # but they are here to match the order of columns in production becase
        # `insert_distributed_sync` is very sensitive to column existence and ordering.
        ("culprit", String(nullable())),
        ("sdk_integrations", Array(String())),
        ("modules", Nested([("name", String()), ("version", String())])),
        ("release", (String(Modifiers(nullable=True, readonly=True)))),
        ("dist", (String(Modifiers(nullable=True, readonly=True)))),
        ("user", (String(Modifiers(nullable=True, readonly=True)))),
    ]
)


def get_promoted_context_col_mapping() -> Mapping[str, str]:
    """
    Produces the mapping between contexts and the related
    promoted columns.
    """
    return {
        col.flattened.replace("_", ".", 1): col.flattened
        for col in promoted_context_columns
    }


def get_promoted_context_tag_col_mapping() -> Mapping[str, str]:
    """
    Produces the mapping between contexts-tags and the related
    promoted columns.
    """
    return {
        col.flattened.replace("_", ".", 1): col.flattened
        for col in promoted_context_tag_columns
    }


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
            col: context
            for context, col in get_promoted_context_tag_col_mapping().items()
        },
        "contexts": {
            col: context for context, col in get_promoted_context_col_mapping().items()
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


mandatory_conditions = [
    binary_condition(
        None, ConditionFunctions.EQ, Column(None, None, "deleted"), Literal(None, 0),
    ),
]

prewhere_candidates = [
    "event_id",
    "group_id",
    "tags[sentry:release]",
    "sentry:release",
    "message",
    "title",
    "environment",
    "project_id",
]

query_processors = [
    PostReplacementConsistencyEnforcer(
        project_column="project_id",
        # key migration is on going. As soon as all the keys we are interested
        # into in redis are stored with "EVENTS" in the name, we can change this.
        replacer_state_name=None,
    ),
    EventsColumnProcessor(),
    MappingColumnPromoter(
        mapping_specs={
            "tags": ChainMap(
                {col.flattened: col.flattened for col in promoted_tag_columns},
                get_promoted_context_tag_col_mapping(),
            ),
            "contexts": get_promoted_context_col_mapping(),
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
    MappingOptimizer("tags", "_tags_hash_map", "events_tags_hash_map_enabled"),
    ArrayJoinKeyValueOptimizer("tags"),
    PrewhereProcessor(),
]


query_splitters = [
    ColumnSplitQueryStrategy(
        id_column="event_id", project_column="project_id", timestamp_column="timestamp",
    ),
    TimeSplitQueryStrategy(timestamp_col="timestamp"),
]
