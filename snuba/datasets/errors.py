from datetime import timedelta
from typing import Mapping, Sequence, Union

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
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.datasets.errors_processor import ErrorsProcessor
from snuba.datasets.schemas.tables import ReplacingMergeTreeSchema
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query import Query
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor


class ErrorsDataset(TimeSeriesDataset):
    """
    Represents the collections of all event types that are not transactions.

    This is meant to replace Events. They will both exist during the migration.
    """

    def __init__(self) -> None:
        metadata_columns = ColumnSet(
            [
                ("offset", WithCodecs(UInt(64), ["DoubleDelta", "LZ4"])),
                ("partition", UInt(16)),
            ]
        )

        promoted_tag_columns = ColumnSet(
            [
                # These are the classic tags, they are saved in Snuba exactly as they
                # appear in the event body.
                ("level", LowCardinality(String())),
                # ("logger", Nullable(String())),
                # ("server_name", Nullable(String())),  # future name: device_id?
                ("environment", LowCardinality(Nullable(String()))),
                # ("sentry:release", Nullable(String())),
                ("release", LowCardinality(Nullable(String()))),
                # ("sentry:dist", Nullable(String())),
                ("dist", LowCardinality(Nullable(String()))),
                # ("sentry:user", Nullable(String())),
                ("user", WithDefault(String(), "''")),
                # ("site", Nullable(String())),
                # ("url", Nullable(String())),
                ("transaction_name", LowCardinality(String())),
            ]
        )

        required_columns = ColumnSet(
            [
                ("event_id", WithCodecs(UUID(), ["NONE"])),
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
                (
                    "event_hash",
                    WithCodecs(
                        Materialized(UInt(64), "cityHash64(toString(event_id))",),
                        ["NONE"],
                    ),
                ),
                ("event_string", WithCodecs(String(), ["NONE"])),
                ("platform", LowCardinality(String())),
                ("message", String()),
                ("primary_hash", FixedString(32)),
                ("primary_hash_hex", Materialized(UInt(64), "hex(primary_hash)")),
                ("received", DateTime()),
                # ("search_message", Nullable(String())),
                ("title", String()),
                ("location", Nullable(String())),
                # optional user
                ("user_hash", Materialized(UInt(64), "cityHash64(user)"),),
                ("user_id", Nullable(String())),
                ("user_name", Nullable(String())),
                ("user_email", Nullable(String())),
                ("ip_address_v4", Nullable(IPv4())),
                ("ip_address_v6", Nullable(IPv6())),
                # ("ip_address", Nullable(String())),
                # optional geo
                # ("geo_country_code", Nullable(String())),
                # ("geo_region", Nullable(String())),
                # ("geo_city", Nullable(String())),
                ("sdk_name", LowCardinality(String())),
                ("sdk_version", LowCardinality(String())),
                ("type", LowCardinality(String())),
                ("version", LowCardinality(String())),
                (
                    "transaction_hash",
                    Materialized(UInt(64), "cityHash64(transaction_name)"),
                ),
                ("span_id", Nullable(UInt(64))),
                ("trace_id", Nullable(UUID())),
            ]
            + metadata_columns
            + promoted_tag_columns
            + [
                # other tags
                ("tags", Nested([("key", String()), ("value", String())])),
                ("_tags_flattened", String()),
                # other context
                ("contexts", Nested([("key", String()), ("value", String())])),
                ("_contexts_flattened", String()),
                # http interface
                # ("http_method", Nullable(String())),
                # ("http_referer", Nullable(String())),
                # exception interface
                (
                    "exception_stacks",
                    Nested(
                        [
                            ("type", String()),
                            ("value", String()),
                            ("mechanism_type", Nullable(String())),
                            ("mechanism_handled", UInt(8)),
                        ]
                    ),
                ),
                (
                    "exception_frames",
                    Nested(
                        [
                            ("abs_path", String()),
                            ("colno", UInt(32)),
                            ("filename", String()),
                            ("function", String()),
                            ("lineno", UInt(32)),
                            ("in_app", UInt(8)),
                            ("package", Nullable(String())),
                            ("module", String()),
                            ("stack_level", UInt(16)),
                        ]
                    ),
                ),
                ("culprit", String()),
                ("sdk_integrations", Array(String())),
                ("modules", Nested([("name", String()), ("version", String())])),
            ]
        )

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
            order_by="(project_id, toStartOfDay(timestamp), primary_hash, event_hash)",
            partition_by="(toMonday(timestamp), if(retention_days = 30, 30, 90))",
            version_column="deleted",
            sample_expr="event_hash",
            ttl_expr="timestamp + toIntervalDay(retention_days)",
            settings={"index_granularity": 8192},
        )

        dataset_schemas = DatasetSchemas(read_schema=schema, write_schema=schema,)

        table_writer = TableWriter(
            write_schema=schema,
            stream_loader=KafkaStreamLoader(
                processor=ErrorsProcessor(promoted_tag_columns), default_topic="events",
            ),
        )

        super().__init__(
            dataset_schemas=dataset_schemas,
            table_writer=table_writer,
            time_group_columns={"time": "timestamp", "rtime": "received"},
            time_parse_columns=("timestamp", "received"),
        )

        self.__metadata_columns = metadata_columns
        self.__promoted_tag_columns = promoted_tag_columns
        self.__required_columns = required_columns

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

    def get_promoted_tag_columns(self):
        return self.__promoted_tag_columns

    def get_required_columns(self):
        return self.__required_columns

    def _get_promoted_columns(self):
        # The set of columns, and associated keys that have been promoted
        # to the top level table namespace.
        return {
            "tags": frozenset(
                col.flattened for col in (self.get_promoted_tag_columns())
            ),
            "contexts": frozenset(),
        }

    def _get_column_tag_map(self):
        # For every applicable promoted column,  a map of translations from the column
        # name  we save in the database to the tag we receive in the query.
        ret = {
            "tags": {
                col.flattened: col.flattened.replace("_", ".")
                for col in self.get_promoted_tag_columns()
                if col.flattened not in ("release", "dist", "user", "transaction_name")
            },
            "contexts": {},
        }
        ret["tags"]["release"] = "sentry:release"
        ret["tags"]["dist"] = "sentry:dist"
        ret["tags"]["user"] = "sentry:user"
        ret["tags"]["transaction_name"] = "transaction"

    def get_tag_column_map(self):
        # And a reverse map from the tags the client expects to the database columns
        return {
            col: dict(map(reversed, trans.items()))
            for col, trans in self._get_column_tag_map().items()
        }

    def get_promoted_tags(self):
        # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
        # and they can/will use a promoted column.
        return {
            col: [
                self._get_column_tag_map()[col].get(x, x)
                for x in self._get_promoted_columns()[col]
            ]
            for col in self._get_promoted_columns()
        }

    def get_extensions(self) -> Mapping[str, QueryExtension]:
        return {
            "project": ProjectExtension(
                processor=ProjectWithGroupsProcessor(project_column="project_id")
            ),
            "timeseries": TimeSeriesExtension(
                default_granularity=3600,
                default_window=timedelta(days=5),
                timestamp_column="timestamp",
            ),
        }

    def get_query_processors(self) -> Sequence[QueryProcessor]:
        return [BasicFunctionsProcessor(), PrewhereProcessor()]
