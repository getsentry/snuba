import random
from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
from typing import List, Mapping, Optional, Sequence, Set, Tuple, Union

from snuba import environment, settings, state
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
from snuba.clickhouse.query_dsl.accessors import get_object_ids_in_query_ast
from snuba.clickhouse.translators.snuba import SnubaClickhouseStrictTranslator
from snuba.clickhouse.translators.snuba.allowed import (
    ColumnMapper,
    CurriedFunctionCallMapper,
    FunctionCallMapper,
    SubscriptableReferenceMapper,
)
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToColumn,
    ColumnToFunction,
    ColumnToLiteral,
    ColumnToMapping,
    SubscriptableMapper,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.datasets.entities.events import BaseEventsEntity, EventsQueryStorageSelector
from snuba.datasets.entities.transactions import BaseTransactionsEntity
from snuba.datasets.entity import Entity
from snuba.datasets.plans.single_storage import (
    SelectedStorageQueryPlanBuilder,
    SingleStorageQueryPlanBuilder,
)
from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_storage
from snuba.pipeline.pipeline_delegator import PipelineDelegator
from snuba.pipeline.simple_pipeline import EntityQueryPlanner, SimplePipelineBuilder
from snuba.query.dsl import identity
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.extensions import QueryExtension
from snuba.query.logical import Query as LogicalQuery
from snuba.query.matchers import Any
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or
from snuba.query.matchers import String as StringMatch
from snuba.query.processors import QueryProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.project_rate_limiter import ProjectRateLimiterProcessor
from snuba.query.processors.tags_expander import TagsExpanderProcessor
from snuba.query.processors.timeseries_processor import TimeSeriesProcessor
from snuba.query.project_extension import ProjectExtension
from snuba.query.timeseries_extension import TimeSeriesExtension
from snuba.query.validation.validators import EntityRequiredColumnValidator
from snuba.request.request_settings import RequestSettings
from snuba.util import qualified_column
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.threaded_function_delegator import Result
from snuba.web import QueryResult

metrics = MetricsWrapper(environment.metrics, "snuplicator.sampling")


@dataclass(frozen=True)
class DefaultNoneColumnMapper(ColumnMapper):
    """
    This maps a list of column names to None (NULL in SQL) as it is done
    in the discover column_expr method today. It should not be used for
    any other reason or use case, thus it should not be moved out of
    the discover dataset file.
    """

    columns: ColumnSet

    def attempt_map(
        self, expression: Column, children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if expression.column_name in self.columns:
            return identity(
                Literal(None, None),
                expression.alias
                or qualified_column(
                    expression.column_name, expression.table_name or ""
                ),
            )
        else:
            return None


@dataclass
class DefaultNoneFunctionMapper(FunctionCallMapper):
    """
    Maps the list of function names to NULL.
    """

    function_names: Set[str]

    def __post_init__(self) -> None:
        self.function_match = FunctionCallMatch(
            Or([StringMatch(func) for func in self.function_names])
        )

    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if self.function_match.match(expression):
            return identity(Literal(None, None), expression.alias)

        return None


@dataclass(frozen=True)
class DefaultIfNullFunctionMapper(FunctionCallMapper):
    """
    If a function is being called on a column that doesn't exist, or is being
    called on NULL, change the entire function to be NULL.
    """

    function_match = FunctionCallMatch(
        StringMatch("identity"), (LiteralMatch(value=Any(type(None))),)
    )

    def attempt_map(
        self,
        expression: FunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:

        # HACK: Quick fix to avoid this function dropping important conditions from the query
        logical_functions = {"and", "or", "xor"}

        if expression.function_name in logical_functions:
            return None

        parameters = tuple(p.accept(children_translator) for p in expression.parameters)
        for param in parameters:
            # All impossible columns will have been converted to the identity function.
            # So we know that if a function has the identity function as a parameter, we can
            # collapse the entire expression.
            fmatch = self.function_match.match(param)
            if fmatch is not None:
                return identity(Literal(None, None), expression.alias)

        return None


@dataclass(frozen=True)
class DefaultIfNullCurriedFunctionMapper(CurriedFunctionCallMapper):
    """
    If a curried function is being called on a column that doesn't exist, or is being
    called on NULL, change the entire function to be NULL.
    """

    function_match = FunctionCallMatch(StringMatch("identity"), (LiteralMatch(),))

    def attempt_map(
        self,
        expression: CurriedFunctionCall,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[Union[CurriedFunctionCall, FunctionCall]]:
        internal_function = expression.internal_function.accept(children_translator)
        assert isinstance(internal_function, FunctionCall)  # mypy
        parameters = tuple(p.accept(children_translator) for p in expression.parameters)
        for param in parameters:
            # All impossible columns that have been converted to NULL will be the identity function.
            # So we know that if a function has the identity function as a parameter, we can
            # collapse the entire expression.
            fmatch = self.function_match.match(param)
            if fmatch is not None:
                return identity(Literal(None, None), expression.alias)

        return None


@dataclass(frozen=True)
class DefaultNoneSubscriptMapper(SubscriptableReferenceMapper):
    """
    This maps a subscriptable reference to None (NULL in SQL) as it is done
    in the discover column_expr method today. It should not be used for
    any other reason or use case, thus it should not be moved out of
    the discover dataset file.
    """

    subscript_names: Set[str]

    def attempt_map(
        self,
        expression: SubscriptableReference,
        children_translator: SnubaClickhouseStrictTranslator,
    ) -> Optional[FunctionCall]:
        if expression.column.column_name in self.subscript_names:
            return identity(Literal(None, None), expression.alias)
        else:
            return None


EVENTS_COLUMNS = ColumnSet(
    [
        ("group_id", UInt(64, Modifiers(nullable=True))),
        ("primary_hash", FixedString(32, Modifiers(nullable=True))),
        # Promoted tags
        ("level", String(Modifiers(nullable=True))),
        ("logger", String(Modifiers(nullable=True))),
        ("server_name", String(Modifiers(nullable=True))),
        ("site", String(Modifiers(nullable=True))),
        ("url", String(Modifiers(nullable=True))),
        ("location", String(Modifiers(nullable=True))),
        ("culprit", String(Modifiers(nullable=True))),
        ("received", DateTime(Modifiers(nullable=True))),
        ("sdk_integrations", Array(String(), Modifiers(nullable=True))),
        ("version", String(Modifiers(nullable=True))),
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
                    ("stack_level", UInt(16)),
                ]
            ),
        ),
        ("modules", Nested([("name", String()), ("version", String())])),
    ]
)

TRANSACTIONS_COLUMNS = ColumnSet(
    [
        ("transaction_hash", UInt(64, Modifiers(nullable=True))),
        ("transaction_op", String(Modifiers(nullable=True))),
        ("transaction_status", UInt(8, Modifiers(nullable=True))),
        ("duration", UInt(32, Modifiers(nullable=True))),
        ("measurements", Nested([("key", String()), ("value", Float(64))]),),
        ("span_op_breakdowns", Nested([("key", String()), ("value", Float(64))]),),
    ]
)


events_translation_mappers = TranslationMappers(
    columns=[DefaultNoneColumnMapper(TRANSACTIONS_COLUMNS)],
    functions=[DefaultNoneFunctionMapper({"apdex", "failure_rate"})],
    subscriptables=[DefaultNoneSubscriptMapper({"measurements", "span_op_breakdowns"})],
)

transaction_translation_mappers = TranslationMappers(
    columns=[
        ColumnToLiteral(None, "group_id", 0),
        DefaultNoneColumnMapper(EVENTS_COLUMNS),
    ],
    functions=[DefaultNoneFunctionMapper({"isHandled", "notHandled"})],
)

null_function_translation_mappers = TranslationMappers(
    curried_functions=[DefaultIfNullCurriedFunctionMapper()],
    functions=[DefaultIfNullFunctionMapper()],
)


class SampledSimplePipelineBuilder(SimplePipelineBuilder):
    def build_planner(
        self, query: LogicalQuery, settings: RequestSettings,
    ) -> EntityQueryPlanner:
        new_query = deepcopy(query)
        sampling_rate = state.get_config("snuplicator-sampling-rate", 1.0)
        assert isinstance(sampling_rate, float)
        new_query.set_sample(sampling_rate)
        return super().build_planner(new_query, settings)


def is_in_experiment(query: LogicalQuery, referrer: str) -> bool:
    if referrer != "tagstore.__get_tag_keys":
        return False

    project_ids = get_object_ids_in_query_ast(query, "project_id")
    if not project_ids:
        return False

    test_projects_raw = state.get_config("snuplicator-sampling-projects", "")
    test_projects = set()
    if (
        isinstance(test_projects_raw, str) and test_projects_raw != ""
    ):  # should be in the form [1,2,3]
        test_projects_raw = test_projects_raw[1:-1]
        test_projects = set(int(p) for p in test_projects_raw.split(",") if p)
    elif isinstance(test_projects_raw, (int, float)):
        test_projects = {int(test_projects_raw)}

    return project_ids.issubset(test_projects)


def sampling_selector_func(query: LogicalQuery, referrer: str) -> Tuple[str, List[str]]:
    if is_in_experiment(query, referrer):
        sample_query_rate = state.get_config(
            "snuplicator-sampling-experiment-rate", 0.0
        )
        assert isinstance(sample_query_rate, float)
        if random.random() < sample_query_rate:
            return "primary", ["sampler"]

    return "primary", []


def sampling_callback_func(
    query: LogicalQuery,
    settings: RequestSettings,
    referrer: str,
    primary_result: Optional[Result[QueryResult]],
    results: List[Result[QueryResult]],
) -> None:
    if not is_in_experiment(query, referrer):
        return

    if primary_result is None and not results:
        metrics.increment(
            "query_result",
            tags={"match": "empty", "primary": "none", "referrer": referrer},
        )
        return

    if primary_result is None:
        primary_result = results.pop(0)

    primary_function_id = primary_result.function_id
    primary_result_data = primary_result.result.result["data"]
    secondary_result = results.pop(0) if len(results) > 0 else None

    # compare results
    comparison = "one_result"
    if secondary_result:
        metrics.timing(
            "query_result_timing",
            secondary_result.execution_time,
            tags={"function": secondary_result.function_id},
        )
        secondary_result_data = secondary_result.result.result["data"]

        primary_found_keys = set(row["tags_key"] for row in primary_result_data)
        secondary_found_keys = set(row["tags_key"] for row in secondary_result_data)

        if primary_found_keys == secondary_found_keys:
            comparison = "match"
        else:
            comparison = "no_match"

    metrics.increment(
        "query_result",
        tags={
            "match": comparison,
            "primary": primary_function_id,
            "referrer": referrer,
        },
    )
    metrics.timing(
        "query_result_timing",
        primary_result.execution_time,
        tags={"function": primary_function_id},
    )


class DiscoverEntity(Entity):
    """
    Entity that represents both errors and transactions. This is currently backed
    by the events storage but will eventually be switched to use use the merge table storage.
    """

    def __init__(self) -> None:
        self.__common_columns = ColumnSet(
            [
                ("event_id", FixedString(32)),
                ("project_id", UInt(64)),
                ("type", String(Modifiers(nullable=True))),
                ("timestamp", DateTime()),
                ("platform", String(Modifiers(nullable=True))),
                ("environment", String(Modifiers(nullable=True))),
                ("release", String(Modifiers(nullable=True))),
                ("dist", String(Modifiers(nullable=True))),
                ("user", String(Modifiers(nullable=True))),
                ("transaction", String(Modifiers(nullable=True))),
                ("message", String(Modifiers(nullable=True))),
                ("title", String(Modifiers(nullable=True))),
                # User
                ("user_id", String(Modifiers(nullable=True))),
                ("username", String(Modifiers(nullable=True))),
                ("email", String(Modifiers(nullable=True))),
                ("ip_address", String(Modifiers(nullable=True))),
                # SDK
                ("sdk_name", String(Modifiers(nullable=True))),
                ("sdk_version", String(Modifiers(nullable=True))),
                # geo location context
                ("geo_country_code", String(Modifiers(nullable=True))),
                ("geo_region", String(Modifiers(nullable=True))),
                ("geo_city", String(Modifiers(nullable=True))),
                ("http_method", String(Modifiers(nullable=True))),
                ("http_referer", String(Modifiers(nullable=True))),
                # Other tags and context
                ("tags", Nested([("key", String()), ("value", String())])),
                ("contexts", Nested([("key", String()), ("value", String())])),
                ("trace_id", String(Modifiers(nullable=True))),
                ("span_id", UInt(64, Modifiers(nullable=True))),
            ]
        )
        self.__events_columns = EVENTS_COLUMNS
        self.__transactions_columns = TRANSACTIONS_COLUMNS

        events_storage = get_storage(StorageKey.EVENTS)

        events_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=SelectedStorageQueryPlanBuilder(
                selector=EventsQueryStorageSelector(
                    mappers=events_translation_mappers.concat(
                        transaction_translation_mappers
                    )
                    .concat(null_function_translation_mappers)
                    .concat(
                        TranslationMappers(
                            # XXX: Remove once we are using errors
                            columns=[
                                ColumnToMapping(
                                    None, "release", None, "tags", "sentry:release"
                                ),
                                ColumnToMapping(
                                    None, "dist", None, "tags", "sentry:dist"
                                ),
                                ColumnToMapping(
                                    None, "user", None, "tags", "sentry:user"
                                ),
                            ],
                            subscriptables=[
                                SubscriptableMapper(None, "tags", None, "tags"),
                                SubscriptableMapper(None, "contexts", None, "contexts"),
                            ],
                        )
                    )
                )
            ),
        )

        discover_storage = get_storage(StorageKey.DISCOVER)
        discover_storage_plan_builder = SingleStorageQueryPlanBuilder(
            storage=discover_storage,
            mappers=events_translation_mappers.concat(transaction_translation_mappers)
            .concat(null_function_translation_mappers)
            .concat(
                TranslationMappers(
                    columns=[
                        ColumnToFunction(
                            None,
                            "ip_address",
                            "coalesce",
                            (
                                FunctionCall(
                                    None,
                                    "IPv4NumToString",
                                    (Column(None, None, "ip_address_v4"),),
                                ),
                                FunctionCall(
                                    None,
                                    "IPv6NumToString",
                                    (Column(None, None, "ip_address_v6"),),
                                ),
                            ),
                        ),
                        ColumnToColumn(None, "transaction", None, "transaction_name"),
                        ColumnToColumn(None, "username", None, "user_name"),
                        ColumnToColumn(None, "email", None, "user_email"),
                        ColumnToMapping(
                            None,
                            "geo_country_code",
                            None,
                            "contexts",
                            "geo.country_code",
                            nullable=True,
                        ),
                        ColumnToMapping(
                            None,
                            "geo_region",
                            None,
                            "contexts",
                            "geo.region",
                            nullable=True,
                        ),
                        ColumnToMapping(
                            None,
                            "geo_city",
                            None,
                            "contexts",
                            "geo.city",
                            nullable=True,
                        ),
                        ColumnToFunction(
                            None,
                            "user",
                            "nullIf",
                            (Column(None, None, "user"), Literal(None, "")),
                        ),
                    ]
                )
            )
            .concat(
                TranslationMappers(
                    subscriptables=[
                        SubscriptableMapper(None, "tags", None, "tags"),
                        SubscriptableMapper(None, "contexts", None, "contexts"),
                    ],
                )
            ),
        )
        discover_pipeline_builder = SimplePipelineBuilder(
            query_plan_builder=discover_storage_plan_builder
        )

        pipeline_builder: Union[PipelineDelegator, SimplePipelineBuilder]
        if settings.ERRORS_ROLLOUT_ALL:
            storage = discover_storage
            sampled_pipeline_builder = SampledSimplePipelineBuilder(
                query_plan_builder=discover_storage_plan_builder
            )

            pipeline_builder = PipelineDelegator(
                query_pipeline_builders={
                    "primary": discover_pipeline_builder,
                    "sampler": sampled_pipeline_builder,
                },
                selector_func=sampling_selector_func,
                callback_func=sampling_callback_func,
            )
        else:
            storage = events_storage
            pipeline_builder = events_pipeline_builder

        super().__init__(
            storages=[storage],
            query_pipeline_builder=pipeline_builder,
            abstract_column_set=(
                self.__common_columns
                + self.__events_columns
                + self.__transactions_columns
            ),
            join_relationships={},
            writable_storage=None,
            validators=[EntityRequiredColumnValidator({"project_id"})],
            required_time_column="timestamp",
        )

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [
            TimeSeriesProcessor({"time": "timestamp"}, ("timestamp",)),
            TagsExpanderProcessor(),
            BasicFunctionsProcessor(),
            ProjectRateLimiterProcessor(project_column="project_id"),
        ]

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(project_column="project_id"),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }


class DiscoverEventsEntity(BaseEventsEntity):
    """
    Identical to EventsEntity except it maps columns and functions present in the
    transactions entity to null. This logic will eventually move to Sentry and this
    entity can be deleted and replaced with the EventsEntity directly.
    """

    def __init__(self) -> None:
        super().__init__(
            custom_mappers=events_translation_mappers.concat(
                null_function_translation_mappers
            )
        )


class DiscoverTransactionsEntity(BaseTransactionsEntity):
    """
    Identical to TransactionsEntity except it maps columns and functions present
    in the events entity to null. This logic will eventually move to Sentry and this
    entity can be deleted and replaced with the TransactionsEntity directly.
    """

    def __init__(self) -> None:
        super().__init__(
            custom_mappers=transaction_translation_mappers.concat(
                null_function_translation_mappers
            )
        )
