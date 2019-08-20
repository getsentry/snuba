import re
from snuba import state
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
from snuba.datasets import TimeSeriesDataset
from snuba.datasets.events_processor import EventsProcessor
from snuba.datasets.schema import ReplacingMergeTreeSchema
from snuba.schemas import EVENTS_QUERY_SCHEMA
from snuba.util import (
    alias_expr,
    all_referenced_columns,
    escape_literal,
    string_col,
)


# A column name like "tags[url]"
NESTED_COL_EXPR_RE = re.compile(r'^(tags|contexts)\[([a-zA-Z0-9_\.:-]+)\]$')


class EventsDataset(TimeSeriesDataset):
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

        super(EventsDataset, self).__init__(
            write_schema=schema,
            processor=EventsProcessor(promoted_tag_columns),
            default_topic="events",
            default_replacement_topic="event-replacements",
            default_commit_log_topic="snuba-commit-log",
            time_group_columns={
                'time': 'timestamp',
                'rtime': 'received'
            },
        )

        self.__metadata_columns = metadata_columns
        self.__promoted_tag_columns = promoted_tag_columns
        self.__promoted_context_tag_columns = promoted_context_tag_columns
        self.__promoted_context_columns = promoted_context_columns
        self.__required_columns = required_columns

    def default_conditions(self):
        return [
            ('deleted', '=', 0),
        ]

    def column_expr(self, column_name, body):
        if NESTED_COL_EXPR_RE.match(column_name):
            return self._tag_expr(column_name)
        elif column_name in ['tags_key', 'tags_value']:
            return self._tags_expr(column_name, body)
        elif column_name == 'issue' or column_name == 'group_id':
            return 'nullIf(group_id, 0)'
        elif column_name == 'message':
            # Because of the rename from message->search_message without backfill,
            # records will have one or the other of these fields.
            # TODO this can be removed once all data has search_message filled in.
            return 'coalesce(search_message, message)'
        else:
            return super().column_expr(column_name, body)

    def get_metadata_columns(self):
        return self.__metadata_columns

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
            'tags': frozenset(col.flattened for col in (self.get_promoted_tag_columns() + self._get_promoted_context_tag_columns())),
            'contexts': frozenset(col.flattened for col in self._get_promoted_context_columns()),
        }

    def _get_column_tag_map(self):
        # For every applicable promoted column,  a map of translations from the column
        # name  we save in the database to the tag we receive in the query.
        promoted_context_tag_columns = self._get_promoted_context_tag_columns()

        return {
            'tags': {col.flattened: col.flattened.replace('_', '.') for col in promoted_context_tag_columns},
            'contexts': {},
        }

    def get_tag_column_map(self):
        # And a reverse map from the tags the client expects to the database columns
        return {
            col: dict(map(reversed, trans.items())) for col, trans in self._get_column_tag_map().items()
        }

    def get_promoted_tags(self):
        # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
        # and they can/will use a promoted column.
        return {
            col: [self._get_column_tag_map()[col].get(x, x) for x in self._get_promoted_columns()[col]]
            for col in self._get_promoted_columns()
        }

    def _tag_expr(self, column_name):
        """
        Return an expression for the value of a single named tag.

        For tags/contexts, we expand the expression depending on whether the tag is
        "promoted" to a top level column, or whether we have to look in the tags map.
        """
        col, tag = NESTED_COL_EXPR_RE.match(column_name).group(1, 2)

        # For promoted tags, return the column name.
        if col in self._get_promoted_columns():
            actual_tag = self.get_tag_column_map()[col].get(tag, tag)
            if actual_tag in self._get_promoted_columns()[col]:
                return string_col(self.get_read_schema(), actual_tag)

        # For the rest, return an expression that looks it up in the nested tags.
        return u'{col}.value[indexOf({col}.key, {tag})]'.format(**{
            'col': col,
            'tag': escape_literal(tag)
        })

    def _tags_expr(self, column_name, body):
        """
        Return an expression that array-joins on tags to produce an output with one
        row per tag.
        """
        assert column_name in ['tags_key', 'tags_value']
        col, k_or_v = column_name.split('_', 1)
        nested_tags_only = state.get_config('nested_tags_only', 1)

        # Generate parallel lists of keys and values to arrayJoin on
        if nested_tags_only:
            key_list = '{}.key'.format(col)
            val_list = '{}.value'.format(col)
        else:
            promoted = self._get_promoted_columns()[col]
            col_map = self._get_column_tag_map()[col]
            key_list = u'arrayConcat([{}], {}.key)'.format(
                u', '.join(u'\'{}\''.format(col_map.get(p, p)) for p in promoted),
                col
            )
            val_list = u'arrayConcat([{}], {}.value)'.format(
                ', '.join(string_col(self.get_read_schema(), p) for p in promoted),
                col
            )

        cols_used = all_referenced_columns(body) & set(['tags_key', 'tags_value'])
        if len(cols_used) == 2:
            # If we use both tags_key and tags_value in this query, arrayjoin
            # on (key, value) tag tuples.
            expr = (u'arrayJoin(arrayMap((x,y) -> [x,y], {}, {}))').format(
                key_list,
                val_list
            )

            # put the all_tags expression in the alias cache so we can use the alias
            # to refer to it next time (eg. 'all_tags[1] AS tags_key'). instead of
            # expanding the whole tags expression again.
            expr = alias_expr(expr, 'all_tags', body)
            return u'({})[{}]'.format(expr, 1 if k_or_v == 'key' else 2)
        else:
            # If we are only ever going to use one of tags_key or tags_value, don't
            # bother creating the k/v tuples to arrayJoin on, or the all_tags alias
            # to re-use as we won't need it.
            return 'arrayJoin({})'.format(key_list if k_or_v == 'key' else val_list)

    def get_query_schema(self):
        return EVENTS_QUERY_SCHEMA
