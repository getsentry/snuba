from collections import deque
from datetime import datetime, timedelta
from hashlib import md5
import jsonschema
import logging
import re
import simplejson as json
import six
import _strptime  # fixes _strptime deferred import issue

from snuba.datasets import DataSet
from snuba import clickhouse, settings, schemas, state, util, processor
from snuba.clickhouse import *
from snuba.processor import Processor, InvalidMessage, EventTooOld, NEEDS_FINAL, EXCLUDE_GROUPS
from snuba.util import escape_string
from snuba.writer import row_from_processed_event

class EventsTableSchema(TableSchema):
    def __init__(self, *args, **kwargs):
        super(EventsTableSchema, self).__init__(*args, **kwargs)

        self.CLICKHOUSE_CLUSTER = None
        self.DATABASE = 'default'
        self.LOCAL_TABLE = 'sentry_local'
        self.DIST_TABLE = 'sentry_dist'
        self.QUERY_TABLE = self.DIST_TABLE # For prod, queries are run against the dist table
        self.CAN_DROP = False

        self.SAMPLE_EXPR = 'cityHash64(toString(event_id))'
        self.ORDER_BY = '(project_id, toStartOfDay(timestamp), %s)' % self.SAMPLE_EXPR
        self.PARTITION_BY = '(toMonday(timestamp), if(equals(retention_days, 30), 30, 90))'
        self.VERSION_COLUMN = 'deleted'
        self.SHARDING_KEY = 'cityHash64(toString(event_id))'
        self.RETENTION_DAYS = 90

        self.METADATA_COLUMNS = ColumnSet([
            # optional stream related data
            ('offset', Nullable(UInt(64))),
            ('partition', Nullable(UInt(16))),
        ])

        self.PROMOTED_TAG_COLUMNS = ColumnSet([
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

        self.PROMOTED_CONTEXT_TAG_COLUMNS = ColumnSet([
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

        self.PROMOTED_CONTEXT_COLUMNS = ColumnSet([
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

        self.REQUIRED_COLUMNS = ColumnSet([
            ('event_id', FixedString(32)),
            ('project_id', UInt(64)),
            ('group_id', UInt(64)),
            ('timestamp', DateTime()),
            ('deleted', UInt(8)),
            ('retention_days', UInt(16)),
        ])

        self.ALL_COLUMNS = self.REQUIRED_COLUMNS + [
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
        ] + self.METADATA_COLUMNS \
          + self.PROMOTED_CONTEXT_COLUMNS \
          + self.PROMOTED_TAG_COLUMNS \
          + self.PROMOTED_CONTEXT_TAG_COLUMNS \
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

            # interfaces

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

        # TODO the following mappings are only used by the EventsDataSet to
        # expand tags[] expressions so perhaps they should be defined there.

        # The set of columns, and associated keys that have been promoted
        # to the top level table namespace.
        self.PROMOTED_COLS = {
            'tags': frozenset(col.flattened for col in (self.PROMOTED_TAG_COLUMNS + self.PROMOTED_CONTEXT_TAG_COLUMNS)),
            'contexts': frozenset(col.flattened for col in self.PROMOTED_CONTEXT_COLUMNS),
        }

        # For every applicable promoted column,  a map of translations from the column
        # name  we save in the database to the tag we receive in the query.
        self.COLUMN_TAG_MAP = {
            'tags': {col.flattened: col.flattened.replace('_', '.') for col in self.PROMOTED_CONTEXT_TAG_COLUMNS},
            'contexts': {},
        }

        # And a reverse map from the tags the client expects to the database columns
        self.TAG_COLUMN_MAP = {
            col: dict(map(reversed, trans.items())) for col, trans in self.COLUMN_TAG_MAP.items()
        }

        # The canonical list of foo.bar strings that you can send as a `tags[foo.bar]` query
        # and they can/will use a promoted column.
        self.PROMOTED_TAGS = {
            col: [self.COLUMN_TAG_MAP[col].get(x, x) for x in self.PROMOTED_COLS[col]]
            for col in self.PROMOTED_COLS
        }

# Dev and Test tables use the same column schema as the production schema,
# but return a different (non-replicated) table engine for the local table.
class DevEventsTableSchema(EventsTableSchema):
    def __init__(self, *args, **kwargs):
        super(DevEventsTableSchema, self).__init__(*args, **kwargs)

        self.LOCAL_TABLE = 'dev_events'
        self.QUERY_TABLE = self.LOCAL_TABLE # For dev/test, queries are run against the local table
        self.CAN_DROP = True

    def get_local_engine(self):
        return """
            ReplacingMergeTree(%(version_column)s)
            PARTITION BY %(partition_by)s
            ORDER BY %(order_by)s
            SAMPLE BY %(sample_expr)s ;""" % {
            'order_by': self.ORDER_BY,
            'partition_by': self.PARTITION_BY,
            'version_column': self.VERSION_COLUMN,
            'sample_expr': self.SAMPLE_EXPR,
        }

    def migrate(self, clickhouse):
        from snuba import migrate
        migrate.run(clickhouse, self)


class TestEventsTableSchema(DevEventsTableSchema):
    def __init__(self, *args, **kwargs):
        super(TestEventsTableSchema, self).__init__(*args, **kwargs)

        self.LOCAL_TABLE = 'test_events'
        self.QUERY_TABLE = self.LOCAL_TABLE
        self.CAN_DROP = True

    def migrate(self, clickhouse):
        pass


class EventsProcessor(Processor):
    """
    A processor for regular (error type) events.
    """
    KAFKA_MESSAGE_SCHEMA = {
        'type': 'array',
        'minItems': 3,
        # common attributes for all protocols
        'items': [
            {'enum': [0,1,2]}, # valid versions
            {'type': 'string'}, # action
        ],
        'anyOf': [
             # Version 0
             {
                'maxItems': 3,
                'items': [
                    {'const': 0},
                    {'const': 'insert'},
                    {'type': 'object'}, # data
                ]
            },
             # Version 1
             {
                'maxItems': 4,
                'items': [
                    {'const': 1},
                    {'enum': ['insert']},
                    {'type': 'object'}, # data
                    {'type': 'object'}, # state (optional)
                ]
            },
             # Version 2
             {
                'maxItems': 4,
                'items': [
                    {'const': 2},
                    {'enum': [
                        'insert', 'start_delete_groups', 'start_merge', 'start_unmerge',
                        'start_delete_tag', 'end_delete_groups', 'end_merge', 'end_unmerge',
                        'end_delete_tag',
                    ]},
                    {'type': 'object'}, # data
                    {'type': 'object'}, # state (optional)
                ]
            },

        ]
    }

    def __init__(self, SCHEMA):
        super(EventsProcessor, self).__init__(SCHEMA)

        self.REQUIRED_COLUMN_NAMES = [col.escaped for col in self.SCHEMA.REQUIRED_COLUMNS]
        self.ALL_COLUMN_NAMES = [col.escaped for col in self.SCHEMA.ALL_COLUMNS]

        self.SEEN_MERGE_TXN_CACHE = deque(maxlen=100)

        self.MESSAGE_TOPIC = 'events'
        self.MESSAGE_CONSUMER_GROUP = 'snuba-consumers'
        self.REPLACEMENTS_TOPIC = 'event-replacements'
        self.REPLACEMENTS_CONSUMER_GROUP = 'snuba-replacers'
        self.COMMIT_LOG_TOPIC = 'snuba-commit-log'

    @property
    def key_function(self):
        # TODO have schema validate that messages contain a project_id
        return lambda x: six.text_type(x[2]['project_id']).encode('utf-8')

    def validate_message(self, message):
        try:
            schemas.validate(message, self.KAFKA_MESSAGE_SCHEMA, False)
        except (ValueError, jsonschema.ValidationError) as e:
            # TODO separate message type, version etc? or just coalesce all these exception types
            raise InvalidMessage("Invalid message: {}".format(e))

        # NB: This is also protocol specific stuff, a new version might
        # not always have type and data in positions 1 and 2 of the message.
        type_, data = message[1:3]
        if data is None:
            return None
        elif type_ == 'insert':
            return (self.INSERT, data)
        else:
            return (self.REPLACE, message)

    def process_insert(self, message):
        processed = {'deleted': 0}
        try:
            self.extract_required(processed, message)
        except EventTooOld:
            logger.warn('Event discarded as too old: %s', message, exc_info=True)
            return None

        data = message.get('data', {})
        # HACK: https://sentry.io/sentry/snuba/issues/802102397/
        if not data:
            logger.error('No data for event: %s', message, exc_info=True)
            return None
        self.extract_common(processed, message, data)

        self.extract_metadata(processed, message, data)

        sdk = data.get('sdk', None) or {}
        self.extract_sdk(processed, sdk)

        tags = self._as_dict_safe(data.get('tags', None))
        self.extract_promoted_tags(processed, tags)

        contexts = data.get('contexts', None) or {}
        self.extract_promoted_contexts(processed, contexts, tags)

        user = data.get('user', data.get('sentry.interfaces.User', None)) or {}
        self.extract_user(processed, user)

        geo = user.get('geo', None) or {}
        self.extract_geo(processed, geo)

        http = data.get('request', data.get('sentry.interfaces.Http', None)) or {}
        self.extract_http(processed, http)

        self.extract_extra_contexts(processed, contexts)
        self.extract_extra_tags(processed, tags)

        exception = data.get('exception', data.get('sentry.interfaces.Exception', None)) or {}
        stacks = exception.get('values', None) or []
        self.extract_stacktraces(processed, stacks)

        return row_from_processed_event(self.SCHEMA, processed)

    def process_replacement(self, message):
        # NB we are relying here on the fact that the message has already
        # been validated by the consumer
        version, type_, data = message[:3]
        if version == 2:
            if type_ == 'end_delete_groups':
                return self.process_delete_groups(data)
            elif type_ == 'end_merge':
                return self.process_merge(data)
            elif type_ == 'end_unmerge':
                return self.process_unmerge(data)
            elif type_ == 'end_delete_tag':
                return self.process_delete_tag(data)

        # other types, do nothing
        return None

    ###########################
    # Helpers
    ###########################

    def extract_required(self, output, message):
        output['event_id'] = message['event_id']
        project_id = message['project_id']
        output['project_id'] = project_id
        output['group_id'] = message['group_id']

        # This is not ideal but it should never happen anyways
        timestamp = self._ensure_valid_date(datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT))
        if timestamp is None:
            timestamp = datetime.utcnow()

        retention_days = settings.RETENTION_OVERRIDES.get(project_id)
        if retention_days is None:
            retention_days = int(message.get('retention_days') or settings.DEFAULT_RETENTION_DAYS)

        # TODO: We may need to allow for older events in the future when post
        # processing triggers are based off of Snuba. Or this branch could be put
        # behind a "backfill-only" optional switch.
        if settings.DISCARD_OLD_EVENTS and timestamp < (datetime.utcnow() - timedelta(days=retention_days)):
            raise EventTooOld

        output['timestamp'] = timestamp
        output['retention_days'] = retention_days

    def extract_metadata(self, output, message, data):
        # TODO for col in self.dataset.SCHEMA.METADATA_COLUMNS:
        output['offset'] = message.get('offset', None)
        output['partition'] = message.get('partition', None)

    def extract_common(self, output, message, data):
        # Properties we get from the top level of the message payload
        output['platform'] = self._unicodify(message['platform'])
        output['primary_hash'] = self._hashify(message['primary_hash'])

        # Properties we get from the "data" dict, which is the actual event body.
        received = self._collapse_uint32(int(data['received']))
        output['received'] = datetime.utcfromtimestamp(received) if received is not None else None

        output['culprit'] = self._unicodify(data.get('culprit', None))
        output['type'] = self._unicodify(data.get('type', None))
        output['version'] = self._unicodify(data.get('version', None))
        output['title'] = self._unicodify(data.get('title', None))
        output['location'] = self._unicodify(data.get('location', None))

        # The following concerns the change to message/search_message
        # There are 2 Scenarios:
        #   Pre-rename:
        #        - Payload contains:
        #             "message": "a long search message"
        #        - Does NOT contain a `search_message` property
        #        - "message" value saved in `message` column
        #        - `search_message` column nonexistent or Null
        #   Post-rename:
        #        - Payload contains:
        #             "search_message": "a long search message"
        #        - Optionally the payload's "data" dict (event body) contains:
        #             "message": "short message"
        #        - "search_message" value stored in `search_message` column
        #        - "message" value stored in `message` column
        #
        output['search_message'] = self._unicodify(message.get('search_message', None))
        if output['search_message'] is None:
            # Pre-rename scenario, we expect to find "message" at the top level
            output['message'] = self._unicodify(message['message'])
        else:
            # Post-rename scenario, we check in case we have the optional
            # "message" in the event body.
            output['message'] = self._unicodify(data.get('message', None))

        module_names = []
        module_versions = []
        modules = data.get('modules', {})
        if isinstance(modules, dict):
            for name, version in modules.items():
                module_names.append(self._unicodify(name))
                # Being extra careful about a stray (incorrect by spec) `null`
                # value blowing up the write.
                module_versions.append(self._unicodify(version) or '')

        output['modules.name'] = module_names
        output['modules.version'] = module_versions

    def extract_sdk(self, output, sdk):
        output['sdk_name'] = self._unicodify(sdk.get('name', None))
        output['sdk_version'] = self._unicodify(sdk.get('version', None))

        sdk_integrations = []
        for i in sdk.get('integrations', None) or ():
            i = self._unicodify(i)
            if i:
                sdk_integrations.append(i)
        output['sdk_integrations'] = sdk_integrations

    def extract_promoted_tags(self, output, tags):
        output.update({col.name: self._unicodify(tags.get(col.name, None))
            for col in self.SCHEMA.PROMOTED_TAG_COLUMNS
        })

    def extract_promoted_contexts(self, output, contexts, tags):
        app_ctx = contexts.get('app', None) or {}
        output['app_device'] = self._unicodify(tags.get('app.device', None))
        app_ctx.pop('device_app_hash', None)  # tag=app.device

        os_ctx = contexts.get('os', None) or {}
        output['os'] = self._unicodify(tags.get('os', None))
        output['os_name'] = self._unicodify(tags.get('os.name', None))
        os_ctx.pop('name', None)  # tag=os and/or os.name
        os_ctx.pop('version', None)  # tag=os
        output['os_rooted'] = self._boolify(tags.get('os.rooted', None))
        os_ctx.pop('rooted', None)  # tag=os.rooted
        output['os_build'] = self._unicodify(os_ctx.pop('build', None))
        output['os_kernel_version'] = self._unicodify(os_ctx.pop('kernel_version', None))

        runtime_ctx = contexts.get('runtime', None) or {}
        output['runtime'] = self._unicodify(tags.get('runtime', None))
        output['runtime_name'] = self._unicodify(tags.get('runtime.name', None))
        runtime_ctx.pop('name', None)  # tag=runtime and/or runtime.name
        runtime_ctx.pop('version', None)  # tag=runtime

        browser_ctx = contexts.get('browser', None) or {}
        output['browser'] = self._unicodify(tags.get('browser', None))
        output['browser_name'] = self._unicodify(tags.get('browser.name', None))
        browser_ctx.pop('name', None)  # tag=browser and/or browser.name
        browser_ctx.pop('version', None)  # tag=browser

        device_ctx = contexts.get('device', None) or {}
        output['device'] = self._unicodify(tags.get('device', None))
        device_ctx.pop('model', None)  # tag=device
        output['device_family'] = self._unicodify(tags.get('device.family', None))
        device_ctx.pop('family', None)  # tag=device.family
        output['device_name'] = self._unicodify(device_ctx.pop('name', None))
        output['device_brand'] = self._unicodify(device_ctx.pop('brand', None))
        output['device_locale'] = self._unicodify(device_ctx.pop('locale', None))
        output['device_uuid'] = self._unicodify(device_ctx.pop('uuid', None))
        output['device_model_id'] = self._unicodify(device_ctx.pop('model_id', None))
        output['device_arch'] = self._unicodify(device_ctx.pop('arch', None))
        output['device_battery_level'] = self._floatify(device_ctx.pop('battery_level', None))
        output['device_orientation'] = self._unicodify(device_ctx.pop('orientation', None))
        output['device_simulator'] = self._boolify(device_ctx.pop('simulator', None))
        output['device_online'] = self._boolify(device_ctx.pop('online', None))
        output['device_charging'] = self._boolify(device_ctx.pop('charging', None))

    def extract_extra_contexts(self, output, contexts):
        context_keys = []
        context_values = []
        valid_types = (int, float) + six.string_types
        for ctx_name, ctx_obj in contexts.items():
            if isinstance(ctx_obj, dict):
                ctx_obj.pop('type', None)  # ignore type alias
                for inner_ctx_name, ctx_value in ctx_obj.items():
                    if isinstance(ctx_value, valid_types):
                        value = self._unicodify(ctx_value)
                        if value:
                            context_keys.append("%s.%s" % (ctx_name, inner_ctx_name))
                            context_values.append(self._unicodify(ctx_value))

        output['contexts.key'] = context_keys
        output['contexts.value'] = context_values

    def extract_extra_tags(self, output, tags):
        tag_keys = []
        tag_values = []
        for tag_key, tag_value in sorted(tags.items()):
            value = self._unicodify(tag_value)
            if value:
                tag_keys.append(self._unicodify(tag_key))
                tag_values.append(value)

        output['tags.key'] = tag_keys
        output['tags.value'] = tag_values

    def extract_user(self, output, user):
        output['user_id'] = self._unicodify(user.get('id', None))
        output['username'] = self._unicodify(user.get('username', None))
        output['email'] = self._unicodify(user.get('email', None))
        output['ip_address'] = self._unicodify(user.get('ip_address', None))

    def extract_geo(self, output, geo):
        output['geo_country_code'] = self._unicodify(geo.get('country_code', None))
        output['geo_region'] = self._unicodify(geo.get('region', None))
        output['geo_city'] = self._unicodify(geo.get('city', None))

    def extract_http(self, output, http):
        output['http_method'] = self._unicodify(http.get('method', None))
        http_headers = self._as_dict_safe(http.get('headers', None))
        output['http_referer'] = self._unicodify(http_headers.get('Referer', None))

    def extract_stacktraces(self, output, stacks):
        stack_types = []
        stack_values = []
        stack_mechanism_types = []
        stack_mechanism_handled = []

        frame_abs_paths = []
        frame_filenames = []
        frame_packages = []
        frame_modules = []
        frame_functions = []
        frame_in_app = []
        frame_colnos = []
        frame_linenos = []
        frame_stack_levels = []

        stack_level = 0
        for stack in stacks:
            if stack is None:
                continue

            stack_types.append(self._unicodify(stack.get('type', None)))
            stack_values.append(self._unicodify(stack.get('value', None)))

            mechanism = stack.get('mechanism', None) or {}
            stack_mechanism_types.append(self._unicodify(mechanism.get('type', None)))
            stack_mechanism_handled.append(self._boolify(mechanism.get('handled', None)))

            frames = (stack.get('stacktrace', None) or {}).get('frames', None) or []
            for frame in frames:
                if frame is None:
                    continue

                frame_abs_paths.append(self._unicodify(frame.get('abs_path', None)))
                frame_filenames.append(self._unicodify(frame.get('filename', None)))
                frame_packages.append(self._unicodify(frame.get('package', None)))
                frame_modules.append(self._unicodify(frame.get('module', None)))
                frame_functions.append(self._unicodify(frame.get('function', None)))
                frame_in_app.append(frame.get('in_app', None))
                frame_colnos.append(self._collapse_uint32(frame.get('colno', None)))
                frame_linenos.append(self._collapse_uint32(frame.get('lineno', None)))
                frame_stack_levels.append(stack_level)

            stack_level += 1

        output['exception_stacks.type'] = stack_types
        output['exception_stacks.value'] = stack_values
        output['exception_stacks.mechanism_type'] = stack_mechanism_types
        output['exception_stacks.mechanism_handled'] = stack_mechanism_handled
        output['exception_frames.abs_path'] = frame_abs_paths
        output['exception_frames.filename'] = frame_filenames
        output['exception_frames.package'] = frame_packages
        output['exception_frames.module'] = frame_modules
        output['exception_frames.function'] = frame_functions
        output['exception_frames.in_app'] = frame_in_app
        output['exception_frames.colno'] = frame_colnos
        output['exception_frames.lineno'] = frame_linenos
        output['exception_frames.stack_level'] = frame_stack_levels

    def process_delete_groups(self, message):
        group_ids = message['group_ids']
        if not group_ids:
            return None

        assert all(isinstance(gid, six.integer_types) for gid in group_ids)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        select_columns = map(lambda i: i if i != 'deleted' else '1', self.REQUIRED_COLUMN_NAMES)

        where = """\
            WHERE project_id = %(project_id)s
            AND group_id IN (%(group_ids)s)
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(required_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        query_args = {
            'required_columns': ', '.join(self.REQUIRED_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'project_id': message['project_id'],
            'group_ids': ", ".join(str(gid) for gid in group_ids),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
            'table_name': self.SCHEMA.QUERY_TABLE,
        }

        query_time_flags = (EXCLUDE_GROUPS, message['project_id'], group_ids)

        return (count_query_template, insert_query_template, query_args, query_time_flags)


    def process_merge(self, message):
        # HACK: We were sending duplicates of the `end_merge` message from Sentry,
        # this is only for performance of the backlog.
        txn = message.get('transaction_id')
        if txn:
            if txn in self.SEEN_MERGE_TXN_CACHE:
                return None
            else:
                self.SEEN_MERGE_TXN_CACHE.append(txn)

        previous_group_ids = message['previous_group_ids']
        if not previous_group_ids:
            return None

        assert all(isinstance(gid, six.integer_types) for gid in previous_group_ids)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        select_columns = map(lambda i: i if i != 'group_id' else str(message['new_group_id']), self.ALL_COLUMN_NAMES)

        where = """\
            WHERE project_id = %(project_id)s
            AND group_id IN (%(previous_group_ids)s)
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(all_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        query_args = {
            'all_columns': ', '.join(self.ALL_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'project_id': message['project_id'],
            'previous_group_ids': ", ".join(str(gid) for gid in previous_group_ids),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
            'table_name': self.SCHEMA.QUERY_TABLE,
        }

        query_time_flags = (EXCLUDE_GROUPS, message['project_id'], previous_group_ids)

        return (count_query_template, insert_query_template, query_args, query_time_flags)


    def process_unmerge(self, message):
        hashes = message['hashes']
        if not hashes:
            return None

        assert all(isinstance(h, six.string_types) for h in hashes)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        select_columns = map(lambda i: i if i != 'group_id' else str(message['new_group_id']), self.ALL_COLUMN_NAMES)

        where = """\
            WHERE project_id = %(project_id)s
            AND group_id = %(previous_group_id)s
            AND primary_hash IN (%(hashes)s)
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(all_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        query_args = {
            'all_columns': ', '.join(self.ALL_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'previous_group_id': message['previous_group_id'],
            'project_id': message['project_id'],
            'hashes': ", ".join("'%s'" % self._hashify(h) for h in hashes),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
            'table_name': self.SCHEMA.QUERY_TABLE,
        }

        query_time_flags = (NEEDS_FINAL, message['project_id'])

        return (count_query_template, insert_query_template, query_args, query_time_flags)


    def process_delete_tag(self, message):
        tag = message['tag']
        if not tag:
            return None

        assert isinstance(tag, six.string_types)
        timestamp = datetime.strptime(message['datetime'], settings.PAYLOAD_DATETIME_FORMAT)
        tag_column_name = self.SCHEMA.TAG_COLUMN_MAP['tags'].get(tag, tag)
        is_promoted = tag in self.SCHEMA.PROMOTED_TAGS['tags']

        where = """\
            WHERE project_id = %(project_id)s
            AND received <= CAST('%(timestamp)s' AS DateTime)
            AND NOT deleted
        """

        if is_promoted:
            where += "AND %(tag_column)s IS NOT NULL"
        else:
            where += "AND has(`tags.key`, %(tag_str)s)"

        insert_query_template = """\
            INSERT INTO %(table_name)s (%(all_columns)s)
            SELECT %(select_columns)s
            FROM %(table_name)s FINAL
        """ + where

        select_columns = []
        for col in self.SCHEMA.ALL_COLUMNS:
            if is_promoted and col.flattened == tag_column_name:
                select_columns.append('NULL')
            elif col.flattened == 'tags.key':
                select_columns.append(
                    "arrayFilter(x -> (indexOf(`tags.key`, x) != indexOf(`tags.key`, %s)), `tags.key`)" % escape_string(tag)
                )
            elif col.flattened == 'tags.value':
                select_columns.append(
                    "arrayMap(x -> arrayElement(`tags.value`, x), arrayFilter(x -> x != indexOf(`tags.key`, %s), arrayEnumerate(`tags.value`)))" % escape_string(tag)
                )
            else:
                select_columns.append(col.escaped)

        query_args = {
            'all_columns': ', '.join(self.ALL_COLUMN_NAMES),
            'select_columns': ', '.join(select_columns),
            'project_id': message['project_id'],
            'tag_str': escape_string(tag),
            'tag_column': escape_col(tag_column_name),
            'timestamp': timestamp.strftime(CLICKHOUSE_DATETIME_FORMAT),
            'table_name': self.SCHEMA.QUERY_TABLE,
        }

        count_query_template = """\
            SELECT count()
            FROM %(table_name)s FINAL
        """ + where

        query_time_flags = (NEEDS_FINAL, message['project_id'])

        return (count_query_template, insert_query_template, query_args, query_time_flags)


class EventsDataSet(DataSet):
    """
    Represents the collection of classic sentry "error" type events
    and the particular quirks of storing and querying them.
    """

    def __init__(self, *args, **kwargs):
        super(EventsDataSet, self).__init__()

        self.SCHEMA = EventsTableSchema()
        self.PROCESSOR = EventsProcessor(self.SCHEMA)
        self.PROD = True

        self.TIME_GROUP_COLUMNS = {
            'time': 'timestamp',
            'rtime': 'received'
        }
        # A column name like "tags[url]"
        self.NESTED_COL_EXPR_RE = re.compile('^(tags|contexts)\[([a-zA-Z0-9_\.:-]+)\]$')

    def default_conditions(self, body):
        return [
            ('deleted', '=', 0),
        ]

    def column_expr(self, column_name, body):
        expr = None
        if column_name in self.TIME_GROUP_COLUMNS:
            expr = util.time_expr(self.TIME_GROUP_COLUMNS[column_name], body['granularity'])
        elif self.NESTED_COL_EXPR_RE.match(column_name):
            expr = self.tag_expr(column_name)
        elif column_name in ['tags_key', 'tags_value']:
            expr = self.tags_expr(column_name, body)
        elif column_name == 'issue':
            expr = 'group_id'
        elif column_name == 'message':
            # Because of the rename from message->search_message without backfill,
            # records will have one or the other of these fields.
            # TODO this can be removed once all data has search_message filled in.
            expr = 'coalesce(search_message, message)'
        return expr

    def tag_expr(self, column_name):
        """
        Return an expression for the value of a single named tag.

        For tags/contexts, we expand the expression depending on whether the tag is
        "promoted" to a top level column, or whether we have to look in the tags map.
        """
        col, tag = self.NESTED_COL_EXPR_RE.match(column_name).group(1, 2)

        # For promoted tags, return the column name.
        if col in self.SCHEMA.PROMOTED_COLS:
            actual_tag = self.SCHEMA.TAG_COLUMN_MAP[col].get(tag, tag)
            if actual_tag in self.SCHEMA.PROMOTED_COLS[col]:
                return self.string_col(actual_tag)

        # For the rest, return an expression that looks it up in the nested tags.
        return u'{col}.value[indexOf({col}.key, {tag})]'.format(**{
            'col': col,
            'tag': util.escape_literal(tag)
        })


    def tags_expr(self, column_name, body):
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
            promoted = self.SCHEMA.PROMOTED_COLS[col]
            col_map = self.SCHEMA.COLUMN_TAG_MAP[col]
            key_list = u'arrayConcat([{}], {}.key)'.format(
                u', '.join(u'\'{}\''.format(col_map.get(p, p)) for p in promoted),
                col
            )
            val_list = u'arrayConcat([{}], {}.value)'.format(
                ', '.join(self.string_col(p) for p in promoted),
                col
            )

        cols_used = util.all_referenced_columns(body) & set(['tags_key', 'tags_value'])
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
            expr = util.alias_expr(expr, 'all_tags', body)
            return u'({})[{}]'.format(expr, 1 if k_or_v == 'key' else 2)
        else:
            # If we are only ever going to use one of tags_key or tags_value, don't
            # bother creating the k/v tuples to arrayJoin on, or the all_tags alias
            # to re-use as we won't need it.
            return 'arrayJoin({})'.format(key_list if k_or_v == 'key' else val_list)

    def condition_expr(self, condition, body):
        lhs, op, lit = condition
        modified = False
        if (
            lhs in ('received', 'timestamp') and
            op in ('>', '<', '>=', '<=', '=', '!=') and
            isinstance(lit, str)
        ):
            lit = util.parse_datetime(lit)
            modified = True

        # facilitate deduping IN conditions by sorting them.
        if op in ('IN', 'NOT IN') and isinstance(lit, tuple):
            lit = tuple(sorted(lit))
            modified = True

        # If the LHS is a simple column name that refers to an array column
        # (and we are not arrayJoining on that column, which would make it
        # scalar again) and the RHS is a scalar value, we assume that the user
        # actually means to check if any (or all) items in the array match the
        # predicate, so we return an `any(x == value for x in array_column)`
        # type expression. We assume that operators looking for a specific value
        # (IN, =, LIKE) are looking for rows where any array value matches, and
        # exclusionary operators (NOT IN, NOT LIKE, !=) are looking for rows
        # where all elements match (eg. all NOT LIKE 'foo').
        if (
            isinstance(lhs, six.string_types) and
            lhs in self.SCHEMA.ALL_COLUMNS and
            type(self.SCHEMA.ALL_COLUMNS[lhs].type) == clickhouse.Array and
            self.SCHEMA.ALL_COLUMNS[lhs].base_name != body.get('arrayjoin') and
            not isinstance(lit, (list, tuple))
            ):
            any_or_all = 'arrayExists' if op in schemas.POSITIVE_OPERATORS else 'arrayAll'
            return u'{}(x -> assumeNotNull(x {} {}), {})'.format(
                any_or_all,
                op,
                util.escape_literal(lit),
                util.column_expr(self, lhs, body)
            )

        if modified:
            return u'{} {} {}'.format(
                util.column_expr(self, lhs, body),
                op,
                util.escape_literal(lit)
            )
        else:
            return None


    def string_col(self, col):
        col_type = self.SCHEMA.ALL_COLUMNS.get(col, None)
        col_type = str(col_type) if col_type else None

        if col_type and 'String' in col_type and 'FixedString' not in col_type:
            return escape_col(col)
        else:
            return 'toString({})'.format(escape_col(col))


# TODO if the only thing that is different about these is the table name
# it might be better to just have the table name as a constructor parameter
# to EventsDataSet
class TestEventsDataSet(EventsDataSet):
    def __init__(self, *args, **kwargs):
        super(TestEventsDataSet, self).__init__(*args, **kwargs)

        self.SCHEMA = TestEventsTableSchema()
        self.PROCESSOR = EventsProcessor(self.SCHEMA)
        self.PROD = False


class DevEventsDataSet(EventsDataSet):
    def __init__(self, *args, **kwargs):
        super(DevEventsDataSet, self).__init__(*args, **kwargs)

        self.SCHEMA = DevEventsTableSchema()
        self.PROCESSOR = EventsProcessor(self.SCHEMA)
        self.PROD = False

