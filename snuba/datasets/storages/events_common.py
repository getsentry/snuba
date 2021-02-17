from collections import ChainMap
from typing import FrozenSet, Mapping, Sequence, cast

from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    FixedString,
    Float,
    Nested,
)
from snuba.clickhouse.columns import SchemaModifiers as Modifiers
from snuba.clickhouse.columns import String, UInt
from snuba.datasets.storages.events_bool_contexts import EventsBooleanContextsProcessor
from snuba.datasets.storages.group_id_column_processor import GroupIdColumnProcessor
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
        ("offset", UInt(64, Modifiers(nullable=True))),
        ("partition", UInt(16, Modifiers(nullable=True))),
        ("message_timestamp", DateTime()),
    ]
)

promoted_tag_columns = ColumnSet(
    [
        # These are the classic tags, they are saved in Snuba exactly as they
        # appear in the event body.
        ("level", String(Modifiers(nullable=True))),
        ("logger", String(Modifiers(nullable=True))),
        ("server_name", String(Modifiers(nullable=True))),  # future name: device_id?
        ("transaction", String(Modifiers(nullable=True))),
        ("environment", String(Modifiers(nullable=True))),
        ("sentry:release", String(Modifiers(nullable=True))),
        ("sentry:dist", String(Modifiers(nullable=True))),
        ("sentry:user", String(Modifiers(nullable=True))),
        ("site", String(Modifiers(nullable=True))),
        ("url", String(Modifiers(nullable=True))),
    ]
)

promoted_context_tag_columns = ColumnSet(
    [
        # These are promoted tags that come in in `tags`, but are more closely
        # related to contexts.  To avoid naming confusion with Clickhouse nested
        # columns, they are stored in the database with s/./_/
        # promoted tags
        ("app_device", String(Modifiers(nullable=True))),
        ("device", String(Modifiers(nullable=True))),
        ("device_family", String(Modifiers(nullable=True))),
        ("runtime", String(Modifiers(nullable=True))),
        ("runtime_name", String(Modifiers(nullable=True))),
        ("browser", String(Modifiers(nullable=True))),
        ("browser_name", String(Modifiers(nullable=True))),
        ("os", String(Modifiers(nullable=True))),
        ("os_name", String(Modifiers(nullable=True))),
        ("os_rooted", UInt(8, Modifiers(nullable=True))),
    ]
)

promoted_context_columns = ColumnSet(
    [
        ("os_build", String(Modifiers(nullable=True))),
        ("os_kernel_version", String(Modifiers(nullable=True))),
        ("device_name", String(Modifiers(nullable=True))),
        ("device_brand", String(Modifiers(nullable=True))),
        ("device_locale", String(Modifiers(nullable=True))),
        ("device_uuid", String(Modifiers(nullable=True))),
        ("device_model_id", String(Modifiers(nullable=True))),
        ("device_arch", String(Modifiers(nullable=True))),
        ("device_battery_level", Float(32, Modifiers(nullable=True))),
        ("device_orientation", String(Modifiers(nullable=True))),
        ("device_simulator", UInt(8, Modifiers(nullable=True))),
        ("device_online", UInt(8, Modifiers(nullable=True))),
        ("device_charging", UInt(8, Modifiers(nullable=True))),
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
        ("platform", String(Modifiers(nullable=True))),
        ("message", String(Modifiers(nullable=True))),
        ("primary_hash", FixedString(32, Modifiers(nullable=True))),
        ("hierarchical_hashes", Array(FixedString(32))),
        ("received", DateTime(Modifiers(nullable=True))),
        ("search_message", String(Modifiers(nullable=True))),
        ("title", String(Modifiers(nullable=True))),
        ("location", String(Modifiers(nullable=True))),
        # optional user
        ("user_id", String(Modifiers(nullable=True))),
        ("username", String(Modifiers(nullable=True))),
        ("email", String(Modifiers(nullable=True))),
        ("ip_address", String(Modifiers(nullable=True))),
        # optional geo
        ("geo_country_code", String(Modifiers(nullable=True))),
        ("geo_region", String(Modifiers(nullable=True))),
        ("geo_city", String(Modifiers(nullable=True))),
        ("sdk_name", String(Modifiers(nullable=True))),
        ("sdk_version", String(Modifiers(nullable=True))),
        ("type", String(Modifiers(nullable=True))),
        ("version", String(Modifiers(nullable=True))),
    ]
    + metadata_columns
    + promoted_context_columns
    + promoted_tag_columns
    + promoted_context_tag_columns
    + [
        # other tags
        ("tags", Nested([("key", String()), ("value", String())])),
        ("_tags_flattened", String()),
        (
            "_tags_hash_map",
            cast(Array[Modifiers], Array(UInt(64), Modifiers(readonly=True))),
        ),
        # other context
        ("contexts", Nested([("key", String()), ("value", String())])),
        # http interface
        ("http_method", String(Modifiers(nullable=True))),
        ("http_referer", String(Modifiers(nullable=True))),
        # exception interface
        (
            "exception_stacks",
            Nested(
                [
                    ("type", String(Modifiers(nullable=True))),
                    ("value", String(Modifiers(nullable=True))),
                    ("mechanism_type", String(Modifiers(nullable=True))),
                    ("mechanism_handled", UInt(8, Modifiers(nullable=True))),
                ]
            ),
        ),
        (
            "exception_frames",
            Nested(
                [
                    ("abs_path", String(Modifiers(nullable=True))),
                    ("filename", String(Modifiers(nullable=True))),
                    ("package", String(Modifiers(nullable=True))),
                    ("module", String(Modifiers(nullable=True))),
                    ("function", String(Modifiers(nullable=True))),
                    ("in_app", UInt(8, Modifiers(nullable=True))),
                    ("colno", UInt(32, Modifiers(nullable=True))),
                    ("lineno", UInt(32, Modifiers(nullable=True))),
                    ("stack_level", UInt(16, Modifiers())),
                ]
            ),
        ),
        # These are columns we added later in the life of the (current) production
        # database. They don't necessarily belong here in a logical/readability sense
        # but they are here to match the order of columns in production becase
        # `insert_distributed_sync` is very sensitive to column existence and ordering.
        ("culprit", String(Modifiers(nullable=True))),
        ("sdk_integrations", Array(String())),
        ("modules", Nested([("name", String()), ("version", String())])),
        ("release", String(Modifiers(nullable=True, readonly=True))),
        ("dist", String(Modifiers(nullable=True, readonly=True))),
        ("user", String(Modifiers(nullable=True, readonly=True))),
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
        col: dict(map(reversed, trans.items()))  # type: ignore
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
        ConditionFunctions.EQ, Column(None, None, "deleted"), Literal(None, 0),
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
    GroupIdColumnProcessor(),
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
