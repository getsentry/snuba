from snuba import util
from snuba.clusters.storage_sets import StorageSetKey
from snuba.datasets.errors_processor import ErrorsProcessor
from snuba.datasets.errors_replacer import ErrorsReplacer
from snuba.datasets.message_filters import KafkaHeaderFilter
from snuba.datasets.schemas.tables import WritableTableSchema
from snuba.datasets.storage import WritableTableStorage
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.errors_common import (
    all_columns,
    mandatory_conditions,
    prewhere_candidates,
    promoted_context_columns,
    promoted_tag_columns,
    query_splitters,
    required_columns,
)
from snuba.datasets.table_storage import build_kafka_stream_loader_from_settings
from snuba.query.processors.arrayjoin_keyvalue_optimizer import (
    ArrayJoinKeyValueOptimizer,
)
from snuba.query.processors.conditions_enforcer import ProjectIdEnforcer
from snuba.query.processors.empty_tag_condition_processor import (
    EmptyTagConditionProcessor,
)
from snuba.query.processors.events_bool_contexts import EventsBooleanContextsProcessor
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.query.processors.physical.type_converters.hexint_column_processor import (
    HexIntColumnProcessor,
)
from snuba.query.processors.physical.type_converters.uuid_array_column_processor import (
    UUIDArrayColumnProcessor,
)
from snuba.query.processors.physical.type_converters.uuid_column_processor import (
    UUIDColumnProcessor,
)
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.processors.replaced_groups import PostReplacementConsistencyEnforcer
from snuba.query.processors.slice_of_map_optimizer import SliceOfMapOptimizer
from snuba.query.processors.table_rate_limit import TableRateLimit
from snuba.query.processors.tuple_unaliaser import TupleUnaliaser
from snuba.query.processors.type_condition_optimizer import TypeConditionOptimizer
from snuba.query.processors.uniq_in_select_and_having import (
    UniqInSelectAndHavingProcessor,
)
from snuba.query.processors.user_column_processor import UserColumnProcessor
from snuba.replacers.replacer_processor import ReplacerState
from snuba.subscriptions.utils import SchedulingWatermarkMode
from snuba.utils.streams.topics import Topic

# The query processor list is different than the one defined in errors_common.py because the
# PostReplacementConsistencyEnforcer needs to be initialized with ReplacerState.ERRORS_V2 for
# the new storage.
query_processors = [
    UniqInSelectAndHavingProcessor(),
    TupleUnaliaser(),
    PostReplacementConsistencyEnforcer(
        project_column="project_id",
        replacer_state_name=ReplacerState.ERRORS_V2,
    ),
    MappingColumnPromoter(
        mapping_specs={
            "tags": promoted_tag_columns,
            "contexts": promoted_context_columns,
        }
    ),
    UserColumnProcessor(),
    UUIDColumnProcessor({"event_id", "primary_hash", "trace_id"}),
    HexIntColumnProcessor({"span_id"}),
    UUIDArrayColumnProcessor({"hierarchical_hashes"}),
    SliceOfMapOptimizer(),
    EventsBooleanContextsProcessor(),
    TypeConditionOptimizer(),
    MappingOptimizer("tags", "_tags_hash_map", "events_tags_hash_map_enabled"),
    EmptyTagConditionProcessor(),
    ArrayJoinKeyValueOptimizer("tags"),
    PrewhereProcessor(
        prewhere_candidates,
        # Environment and release are excluded from prewhere in case of final
        # queries because of a Clickhouse bug.
        # group_id instead is excluded since `final` is applied after prewhere.
        # thus, in this case, we could be filtering out rows that should be
        # merged together by the final.
        omit_if_final=["environment", "release", "group_id"],
    ),
    TableRateLimit(),
]

schema = WritableTableSchema(
    columns=all_columns,
    local_table_name="errors_local",
    dist_table_name="errors_dist",
    storage_set_key=StorageSetKey.ERRORS_V2,
    mandatory_conditions=mandatory_conditions,
    part_format=[util.PartSegment.RETENTION_DAYS, util.PartSegment.DATE],
)

storage = WritableTableStorage(
    storage_key=StorageKey.ERRORS_V2,
    storage_set_key=StorageSetKey.ERRORS_V2,
    schema=schema,
    query_processors=query_processors,
    query_splitters=query_splitters,
    mandatory_condition_checkers=[ProjectIdEnforcer()],
    stream_loader=build_kafka_stream_loader_from_settings(
        processor=ErrorsProcessor(promoted_tag_columns),
        pre_filter=KafkaHeaderFilter("transaction_forwarder", "1"),
        default_topic=Topic.EVENTS,
        replacement_topic=Topic.EVENT_REPLACEMENTS,
        commit_log_topic=Topic.COMMIT_LOG,
        subscription_scheduler_mode=SchedulingWatermarkMode.PARTITION,
        subscription_scheduled_topic=Topic.SUBSCRIPTION_SCHEDULED_EVENTS,
        subscription_result_topic=Topic.SUBSCRIPTION_RESULTS_EVENTS,
    ),
    replacer_processor=ErrorsReplacer(
        schema=schema,
        required_columns=required_columns,
        tag_column_map={"tags": promoted_tag_columns, "contexts": {}},
        promoted_tags={"tags": list(promoted_tag_columns.keys()), "contexts": []},
        state_name=ReplacerState.ERRORS_V2,
    ),
)
