from datetime import timedelta
from typing import FrozenSet, Mapping, Sequence, Union

from snuba.clickhouse.columns import (
    Array,
    ColumnSet,
    DateTime,
    FixedString,
    IPv4,
    IPv6,
    LowCardinality,
    Materialized,
    Nested,
    Nullable,
    String,
    UInt,
    UUID,
    WithCodecs,
    WithDefault,
)
from snuba.datasets.dataset import ColumnSplitSpec, TimeSeriesDataset
from snuba.datasets.dataset_schemas import StorageSchemas
from snuba.datasets.errors_processor import ErrorsProcessor
from snuba.datasets.errors_replacer import ErrorsReplacer, ReplacerState
from snuba.datasets.storage import SingleStorageSelector, WritableTableStorage
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension


class ErrorsDataset(TimeSeriesDataset):
    """
    Represents the collections of all event types that are not transactions.

    This is meant to replace Events. They will both exist during the migration.
    """

    def __init__(self) -> None:
        all_columns = ColumnSet(
            [
                ("org_id", UInt(64)),
                ("project_id", UInt(64)),
                ("timestamp", DateTime()),
                ("event_id", WithCodecs(UUID(), ["NONE"])),
                (
                    "event_hash",
                    WithCodecs(
                        Materialized(UInt(64), "cityHash64(toString(event_id))",),
                        ["NONE"],
                    ),
                ),
                ("platform", LowCardinality(String())),
                ("environment", LowCardinality(Nullable(String()))),
                ("release", LowCardinality(Nullable(String()))),
                ("dist", LowCardinality(Nullable(String()))),
                ("ip_address_v4", Nullable(IPv4())),
                ("ip_address_v6", Nullable(IPv6())),
                ("user", WithDefault(String(), "''")),
                ("user_hash", Materialized(UInt(64), "cityHash64(user)"),),
                ("user_id", Nullable(String())),
                ("user_name", Nullable(String())),
                ("user_email", Nullable(String())),
                ("sdk_name", LowCardinality(Nullable(String()))),
                ("sdk_version", LowCardinality(Nullable(String()))),
                ("tags", Nested([("key", String()), ("value", String())])),
                ("_tags_flattened", String()),
                ("contexts", Nested([("key", String()), ("value", String())])),
                ("_contexts_flattened", String()),
                ("transaction_name", WithDefault(LowCardinality(String()), "''")),
                (
                    "transaction_hash",
                    Materialized(UInt(64), "cityHash64(transaction_name)"),
                ),
                ("span_id", Nullable(UInt(64))),
                ("trace_id", Nullable(UUID())),
                ("partition", UInt(16)),
                ("offset", WithCodecs(UInt(64), ["DoubleDelta", "LZ4"])),
                ("retention_days", UInt(16)),
                ("deleted", UInt(8)),
                ("group_id", UInt(64)),
                ("primary_hash", FixedString(32)),
                ("primary_hash_hex", Materialized(UInt(64), "hex(primary_hash)")),
                ("event_string", WithCodecs(String(), ["NONE"])),
                ("received", DateTime()),
                ("message", String()),
                ("title", String()),
                ("culprit", String()),
                ("level", LowCardinality(String())),
                ("location", Nullable(String())),
                ("version", LowCardinality(Nullable(String()))),
                ("type", LowCardinality(String())),
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
                            ("colno", Nullable(UInt(32))),
                            ("filename", Nullable(String())),
                            ("function", Nullable(String())),
                            ("lineno", Nullable(UInt(32))),
                            ("in_app", Nullable(UInt(8))),
                            ("package", Nullable(String())),
                            ("module", Nullable(String())),
                            ("stack_level", Nullable(UInt(16))),
                        ]
                    ),
                ),
                ("sdk_integrations", Array(String())),
                ("modules", Nested([("name", String()), ("version", String())])),
            ]
        )

        self.__promoted_tag_columns = {
            "environment": "environment",
            "sentry:release": "release",
            "sentry:dist": "dist",
            "sentry:user": "user",
            "transaction": "transaction_name",
            "level": "level",
        }

        schema = ReplacingMergeTreeSchema(
            columns=all_columns,
            local_table_name="errors_local",
            dist_table_name="errors_dist",
            mandatory_conditions=[("deleted", "=", 0)],
            prewhere_candidates=[
                "event_id",
                "group_id",
                "tags[sentry:release]",
                "message",
                "environment",
                "project_id",
            ],
            order_by="(org_id, project_id, toStartOfDay(timestamp), primary_hash_hex, event_hash)",
            partition_by="(toMonday(timestamp), if(retention_days = 30, 30, 90))",
            version_column="deleted",
            sample_expr="event_hash",
            ttl_expr="timestamp + toIntervalDay(retention_days)",
            settings={"index_granularity": "8192"},
        )

        required_columns = [
            "org_id",
            "event_id",
            "project_id",
            "group_id",
            "timestamp",
            "deleted",
            "retention_days",
        ]

        storage = WritableTableStorage(
            schemas=StorageSchemas(read_schema=schema, write_schema=schema),
            table_writer=TableWriter(
                write_schema=schema,
                stream_loader=KafkaStreamLoader(
                    processor=ErrorsProcessor(self.__promoted_tag_columns),
                    default_topic="events",
                    replacement_topic="errors-replacements",
                ),
                replacer_processor=ErrorsReplacer(
                    write_schema=schema,
                    read_schema=schema,
                    required_columns=required_columns,
                    tag_column_map={
                        "tags": self.__promoted_tag_columns,
                        "contexts": {},
                    },
                    promoted_tags={
                        "tags": self.__promoted_tag_columns.keys(),
                        "contexts": {},
                    },
                    state_name=ReplacerState.ERRORS,
                ),
            ),
            query_processors=[PrewhereProcessor()],
        )

        storage_selector = SingleStorageSelector(storage=storage)

        super().__init__(
            storages=[storage],
            storage_selector=storage_selector,
            abstract_column_set=schema.get_columns(),
            writable_storage=storage,
            time_group_columns={"time": "timestamp", "rtime": "received"},
            time_parse_columns=("timestamp", "received"),
        )

        self.__tags_processor = TagColumnProcessor(
            columns=all_columns,
            promoted_columns=self._get_promoted_columns(),
            column_tag_map=self._get_column_tag_map(),
        )

    def get_split_query_spec(self) -> Union[None, ColumnSplitSpec]:
        return ColumnSplitSpec(
            id_column="event_id",
            project_column="project_id",
            timestamp_column="timestamp",
        )

    def column_expr(
        self,
        column_name,
        query: Query,
        parsing_context: ParsingContext,
        table_alias: str = "",
    ):
        processed_column = self.__tags_processor.process_column_expression(
            column_name, query, parsing_context, table_alias
        )
        return processed_column or super().column_expr(
            column_name, query, parsing_context, table_alias
        )

    def _get_promoted_columns(self) -> Mapping[str, FrozenSet[str]]:
        return {
            "tags": frozenset(self.__promoted_tag_columns.values()),
            "contexts": frozenset(),
        }

    def _get_column_tag_map(self) -> Mapping[str, Mapping[str, str]]:
        return {
            "tags": {col: tag for tag, col in self.__promoted_tag_columns.items()},
            "contexts": {},
        }

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectWithGroupsProcessor(
                    project_column="project_id",
                    replacer_state_name=ReplacerState.ERRORS,
                )
            ),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [BasicFunctionsProcessor()]
