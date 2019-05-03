from datetime import datetime
import jsonschema
import six

from snuba import schemas, settings
from snuba.clickhouse import *
from snuba.datasets import DataSet
from snuba.processor import Processor, InvalidMessage
from snuba.writer import row_from_processed_event

class OutcomesTableSchema(TableSchema):
    def __init__(self, *args, **kwargs):
        super(OutcomesTableSchema, self).__init__(*args, **kwargs)

        self.CLICKHOUSE_CLUSTER = None
        self.DATABASE = 'default'
        self.LOCAL_TABLE = 'outcomes_local'
        self.DIST_TABLE = 'outcomes_dist'
        self.QUERY_TABLE = self.DIST_TABLE # For prod, queries are run against the dist table
        self.CAN_DROP = False

        # The sort key starts with timestamp, as this dataset will
        # be used primarily by TSDB, so all queries should be
        # within a time range, but using hourly granularity for
        # the sort so that within a given time bucket we still
        # have decent size chunks by org and project
        self.ORDER_BY = '(toStartOfHour(timestamp), org_id, project_id)'
        self.PARTITION_BY = 'toMonday(timestamp)'
        self.SHARDING_KEY = 'cityHash64(timestamp)'

        self.ALL_COLUMNS = ColumnSet([
            ('org_id', UInt(64)),
            ('project_id', UInt(64)),
            ('key_id', Nullable(UInt(64))),
            ('timestamp', DateTime()),
            ('outcome', UInt(8)),
            ('reason', Nullable(String())),
            ('event_id', Nullable(String())),
        ])

    def get_local_engine(self):
        return """
            ReplicatedMergeTree('/clickhouse/tables/{shard}/%(name)s', '{replica}')
            PARTITION BY %(partition_by)s
            ORDER BY %(order_by)s;""" % {
                'name': self.LOCAL_TABLE,
                'order_by': self.ORDER_BY,
                'partition_by': self.PARTITION_BY,
            }


class DevOutcomesTableSchema(OutcomesTableSchema):
    def __init__(self, *args, **kwargs):
        super(DevOutcomesTableSchema, self).__init__(*args, **kwargs)

        self.LOCAL_TABLE = 'dev_outcomes'
        self.QUERY_TABLE = self.LOCAL_TABLE
        self.CAN_DROP = True

    def get_local_engine(self):
        return """
            MergeTree()
            PARTITION BY %(partition_by)s
            ORDER BY %(order_by)s;""" % {
                'name': self.LOCAL_TABLE,
                'order_by': self.ORDER_BY,
                'partition_by': self.PARTITION_BY,
            }

class TestOutcomesTableSchema(DevOutcomesTableSchema):
    def __init__(self, *args, **kwargs):
        super(TestOutcomesTableSchema, self).__init__(*args, **kwargs)

        self.LOCAL_TABLE = 'test_outcomes'
        self.QUERY_TABLE = self.LOCAL_TABLE
        self.CAN_DROP = True

class OutcomesProcessor(Processor):
    KAFKA_MESSAGE_SCHEMA = {
        'type': 'object',
        'properties': {
            'org_id': {'type': 'number'},
            'project_id': {'type': 'number'},
            'key_id': {'type': ['number', 'null']},
            'timestamp': {
                'type': 'string',
                'format': 'date-time',
            },
            'outcome': {
                'type': ['string', 'number'],
                'min': 0,
                'max': 255
            },
            'reason': {'type': ['string', 'null']},
            'event_id': {'type': ['string', 'null']},
        },
        'required': [
            'timestamp',
            'org_id',
            'project_id',
            'outcome'
        ],
        'additionalProperties': False,
    }

    def __init__(self, SCHEMA):
        super(OutcomesProcessor, self).__init__(SCHEMA)

        self.MESSAGE_TOPIC = 'outcomes'
        self.MESSAGE_CONSUMER_GROUP = 'outcome-consumers'
        self.REPLACEMENTS_TOPIC = None
        self.REPLACEMENTS_CONSUMER_GROUP = None
        self.COMMIT_LOG_TOPIC = None

    def validate_message(self, message):
        try:
            schemas.validate(message, self.KAFKA_MESSAGE_SCHEMA, False)
        except (ValueError, jsonschema.ValidationError) as e:
            raise InvalidMessage(u"Invalid message: {}".format(e))
        return (self.INSERT, message)

    def process_insert(self, message):
        message['timestamp'] = self._ensure_valid_date(
            datetime.strptime(message['timestamp'], settings.PAYLOAD_DATETIME_FORMAT)
        )

        # temporary hack to allow processing of messages where outcome
        # was sent as a string.
        if isinstance(message['outcome'], six.string_types):
            message['outcome'] = {
                'accepted': 0,
                'filtered': 1,
                'rate_limited': 2,
                'invalid': 3,
            }.get(message['outcome'], 0)
        return row_from_processed_event(self.SCHEMA, message)


class OutcomesDataSet(DataSet):
    def __init__(self, *args, **kwargs):
        super(OutcomesDataSet, self).__init__()

        self.SCHEMA = OutcomesTableSchema()
        self.PROCESSOR = OutcomesProcessor(self.SCHEMA)
        self.PROD = True


class TestOutcomesDataSet(OutcomesDataSet):
    def __init__(self, *args, **kwargs):
        super(TestOutcomesDataSet, self).__init__(*args, **kwargs)

        self.SCHEMA = TestOutcomesTableSchema()
        self.PROCESSOR = OutcomesProcessor(self.SCHEMA)
        self.PROD = False


class DevOutcomesDataSet(OutcomesDataSet):
    def __init__(self, *args, **kwargs):
        super(DevOutcomesDataSet, self).__init__(*args, **kwargs)

        self.SCHEMA = DevOutcomesTableSchema()
        self.PROCESSOR = OutcomesProcessor(self.SCHEMA)
        self.PROD = False
