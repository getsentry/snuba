from datetime import timedelta
from typing import Mapping, Sequence, Union

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
from snuba.datasets.dataset import ColumnSplitSpec, TimeSeriesDataset
from snuba.datasets.dataset_schemas import DatasetSchemas
from snuba.datasets.table_storage import TableWriter, KafkaStreamLoader
from snuba.datasets.events_processor import EventsProcessor
from snuba.datasets.schemas.tables import (
    MigrationSchemaColumn,
    ReplacingMergeTreeSchema,
)
from snuba.datasets.tags_column_processor import TagColumnProcessor
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.processors.prewhere import PrewhereProcessor
from snuba.query.query import Query
from snuba.query.extensions import QueryExtension
from snuba.query.parsing import ParsingContext
from snuba.query.query_processor import QueryProcessor
from snuba.query.timeseries import TimeSeriesExtension
from snuba.query.project_extension import ProjectExtension, ProjectWithGroupsProcessor
from snuba.util import qualified_column


def events_migrations(
    clickhouse_table: str, current_schema: Mapping[str, MigrationSchemaColumn]
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

    return ret


class EventsDataset(TimeSeriesDataset):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self) -> None:
        metadata_columns = ColumnSet(
            [
                # optional stream related data
                ("offset", Nullable(UInt(64))),
                ("partition", Nullable(UInt(16))),
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
            mandatory_conditions=[("deleted", "=", 0)],
            prewhere_candidates=[
                "event_id",
                "group_id",
                "tags[sentry:release]",
                "message",
                "environment",
                "project_id",
            ],
            order_by="(project_id, toStartOfDay(timestamp), %s)" % sample_expr,
            partition_by="(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))",
            version_column="deleted",
            sample_expr=sample_expr,
            migration_function=events_migrations,
        )

        dataset_schemas = DatasetSchemas(read_schema=schema, write_schema=schema,)

        table_writer = TableWriter(
            write_schema=schema,
            stream_loader=KafkaStreamLoader(
                processor=EventsProcessor(promoted_tag_columns),
                default_topic="events",
                replacement_topic="event-replacements",
                commit_log_topic="snuba-commit-log",
            ),
        )

        super(EventsDataset, self).__init__(
            dataset_schemas=dataset_schemas,
            table_writer=table_writer,
            time_group_columns={"time": "timestamp", "rtime": "received"},
            time_parse_columns=("timestamp", "received"),
        )

        self.__metadata_columns = metadata_columns
        self.__promoted_tag_columns = promoted_tag_columns
        self.__promoted_context_tag_columns = promoted_context_tag_columns
        self.__promoted_context_columns = promoted_context_columns
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
        if processed_column:
            # If processed_column is None, this was not a tag/context expression
            return processed_column
        elif column_name == "group_id":
            return f"nullIf({qualified_column('group_id', table_alias)}, 0)"
        elif column_name == "message":
            # Because of the rename from message->search_message without backfill,
            # records will have one or the other of these fields.
            # TODO this can be removed once all data has search_message filled in.
            search_message = qualified_column("search_message", table_alias)
            message = qualified_column("message", table_alias)
            return f"coalesce({search_message}, {message})"
        else:
            return super().column_expr(column_name, query, parsing_context, table_alias)

    def get_promoted_tag_columns(self):
        return self.__promoted_tag_columns

    def _get_promoted_context_tag_columns(self):
        return self.__promoted_context_tag_columns

    def _get_promoted_context_columns(self):
        return self.__promoted_context_columns

    def get_required_columns(self):
        return self.__required_columns

    def _get_promoted_columns(self):
        # The set of columns, and associated keys that have been promoted
        # to the top level table namespace.
        return {
            "tags": frozenset(
                col.flattened
                for col in (
                    self.get_promoted_tag_columns()
                    + self._get_promoted_context_tag_columns()
                )
            ),
            "contexts": frozenset(
                col.flattened for col in self._get_promoted_context_columns()
            ),
        }

    def _get_column_tag_map(self):
        # For every applicable promoted column,  a map of translations from the column
        # name  we save in the database to the tag we receive in the query.
        promoted_context_tag_columns = self._get_promoted_context_tag_columns()

        return {
            "tags": {
                col.flattened: col.flattened.replace("_", ".")
                for col in promoted_context_tag_columns
            },
            "contexts": {},
        }

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
