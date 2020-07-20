from collections import ChainMap
from typing import FrozenSet, Mapping, Sequence

from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    FixedString,
    Float,
    Nested,
    Nullable,
    String,
    UInt,
)
from snuba.datasets.schemas import MandatoryCondition
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
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.web.split import ColumnSplitQueryStrategy, TimeSplitQueryStrategy

TAGS_HASH_MAP_COLUMN = """
arrayMap((k, v) -> cityHash64(
    concat(
        replaceRegexpAll(k, '(\\\\=|\\\\\\\\)', '\\\\\\\\\\\\1'),
        '=',
        v)
    ),
    tags.key,
    tags.value
)
"""


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
        # Tags hashmap is a materialized column. Clickhouse does not allow
        # us to create a materialized column that references a nested one
        # during create statement
        # (https://github.com/ClickHouse/ClickHouse/issues/12586), so the
        # materialization is added with a migration.
        ("_tags_hash_map", Array(UInt(64))),
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
    MandatoryCondition(
        ("deleted", "=", 0),
        binary_condition(
            None,
            ConditionFunctions.EQ,
            Column(None, None, "deleted"),
            Literal(None, 0),
        ),
    )
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
    ArrayJoinKeyValueOptimizer("tags"),
    PrewhereProcessor(),
]


query_splitters = [
    ColumnSplitQueryStrategy(
        id_column="event_id", project_column="project_id", timestamp_column="timestamp",
    ),
    TimeSplitQueryStrategy(timestamp_col="timestamp"),
]
