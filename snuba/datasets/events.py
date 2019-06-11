from snuba.clickhouse import Array, ColumnSet, DateTime, FixedString, Float, Nested, Nullable, UInt, String
from snuba.datasets import DataSet
from snuba.datasets.schema import ReplacingMergeTreeSchema
from snuba.processor import MessageProcessor, process_message


class EventsDataSet(DataSet):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self):
        metadata_columns = ColumnSet([
            # optional stream related data
            ('offset', Nullable(UInt(64))),
            ('partition', Nullable(UInt(16))),
        ])

        promoted_tag_columns = ColumnSet([
            # These are the classic tags, they are saved in Snuba exactly as they
            # appear in the event body.
            ('level', Nullable(String())),
            ('logger', Nullable(String())),
            ('server_name', Nullable(String())),  # future name: device_id?
            ('transaction', Nullable(String())),
            ('environment', Nullable(String())),
            ('sentry:release', Nullable(String())),
            ('sentry:dist', Nullable(String())),
            ('sentry:user', Nullable(String())),
            ('site', Nullable(String())),
            ('url', Nullable(String())),
        ])

        promoted_context_tag_columns = ColumnSet([
            # These are promoted tags that come in in `tags`, but are more closely
            # related to contexts.  To avoid naming confusion with Clickhouse nested
            # columns, they are stored in the database with s/./_/
            # promoted tags
            ('app_device', Nullable(String())),
            ('device', Nullable(String())),
            ('device_family', Nullable(String())),
            ('runtime', Nullable(String())),
            ('runtime_name', Nullable(String())),
            ('browser', Nullable(String())),
            ('browser_name', Nullable(String())),
            ('os', Nullable(String())),
            ('os_name', Nullable(String())),
            ('os_rooted', Nullable(UInt(8))),
        ])

        promoted_context_columns = ColumnSet([
            ('os_build', Nullable(String())),
            ('os_kernel_version', Nullable(String())),
            ('device_name', Nullable(String())),
            ('device_brand', Nullable(String())),
            ('device_locale', Nullable(String())),
            ('device_uuid', Nullable(String())),
            ('device_model_id', Nullable(String())),
            ('device_arch', Nullable(String())),
            ('device_battery_level', Nullable(Float(32))),
            ('device_orientation', Nullable(String())),
            ('device_simulator', Nullable(UInt(8))),
            ('device_online', Nullable(UInt(8))),
            ('device_charging', Nullable(UInt(8))),
        ])

        required_columns = ColumnSet([
            ('event_id', FixedString(32)),
            ('project_id', UInt(64)),
            ('group_id', UInt(64)),
            ('timestamp', DateTime()),
            ('deleted', UInt(8)),
            ('retention_days', UInt(16)),
        ])

        all_columns = required_columns + [
            # required for non-deleted
            ('platform', Nullable(String())),
            ('message', Nullable(String())),
            ('primary_hash', Nullable(FixedString(32))),
            ('received', Nullable(DateTime())),

            ('search_message', Nullable(String())),
            ('title', Nullable(String())),
            ('location', Nullable(String())),

            # optional user
            ('user_id', Nullable(String())),
            ('username', Nullable(String())),
            ('email', Nullable(String())),
            ('ip_address', Nullable(String())),

            # optional geo
            ('geo_country_code', Nullable(String())),
            ('geo_region', Nullable(String())),
            ('geo_city', Nullable(String())),

            ('sdk_name', Nullable(String())),
            ('sdk_version', Nullable(String())),
            ('type', Nullable(String())),
            ('version', Nullable(String())),
        ] + metadata_columns \
            + promoted_context_columns \
            + promoted_tag_columns \
            + promoted_context_tag_columns \
            + [
                # other tags
                ('tags', Nested([
                    ('key', String()),
                    ('value', String()),
                ])),

                # other context
                ('contexts', Nested([
                    ('key', String()),
                    ('value', String()),
                ])),

                # http interface
                ('http_method', Nullable(String())),
                ('http_referer', Nullable(String())),

                # exception interface
                ('exception_stacks', Nested([
                    ('type', Nullable(String())),
                    ('value', Nullable(String())),
                    ('mechanism_type', Nullable(String())),
                    ('mechanism_handled', Nullable(UInt(8))),
                ])),
                ('exception_frames', Nested([
                    ('abs_path', Nullable(String())),
                    ('filename', Nullable(String())),
                    ('package', Nullable(String())),
                    ('module', Nullable(String())),
                    ('function', Nullable(String())),
                    ('in_app', Nullable(UInt(8))),
                    ('colno', Nullable(UInt(32))),
                    ('lineno', Nullable(UInt(32))),
                    ('stack_level', UInt(16)),
                ])),

                # These are columns we added later in the life of the (current) production
                # database. They don't necessarily belong here in a logical/readability sense
                # but they are here to match the order of columns in production becase
                # `insert_distributed_sync` is very sensitive to column existence and ordering.
                ('culprit', Nullable(String())),
                ('sdk_integrations', Array(String())),
                ('modules', Nested([
                    ('name', String()),
                    ('version', String()),
                ])),
        ]

        sample_expr = 'cityHash64(toString(event_id))'
        schema = ReplacingMergeTreeSchema(
            columns=all_columns,
            local_table_name='sentry_local',
            dist_table_name='sentry_dist',
            order_by='(project_id, toStartOfDay(timestamp), %s)' % sample_expr,
            partition_by='(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))',
            version_column='deleted',
            sample_expr=sample_expr)

        super(EventsDataSet, self).__init__(
            schema=schema,
            processor=EventsProcessor(promoted_tag_columns),
            message_topic="events",
            message_consumer_group="snuba-consumers",
            forward_topic="event-replacements",
            forward_consumer_group="snuba-replacers",
            commit_log_topic="snuba-commit-log"
        )

        self.__metadata_columns = metadata_columns
        self.__promoted_tag_columns = promoted_tag_columns
        self.__promoted_context_tag_columns = promoted_context_tag_columns
        self.__promoted_context_columns = promoted_context_columns
        self.__required_columns = required_columns

    def default_conditions(self, body):
        return [
            ('deleted', '=', 0),
        ]

    # These method should be removed once we will have dataset specific query processing in
    # the dataset class instead of util.py and when the dataset specific logic for processing
    # Kafka messages will be in the dataset as well.

    def get_metadata_columns(self):
        return self.__metadata_columns

    def get_promoted_tag_columns(self):
        return self.__promoted_tag_columns

    def get_promoted_context_tag_columns(self):
        return self.__promoted_context_tag_columns

    def get_promoted_context_columns(self):
        return self.__promoted_context_columns

    def get_required_columns(self):
        return self.__required_columns


class EventsProcessor(MessageProcessor):
    def __init__(self, promoted_tag_columns):
        self.__promoted_tag_columns = promoted_tag_columns

    def process_message(self, message):
        return process_message(self.__promoted_tag_columns,
            message)
