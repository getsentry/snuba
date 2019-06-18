from datetime import datetime

from snuba.processor import _unicodify
from snuba.datasets.cdc.cdcprocessors import CdcProcessor


class GroupedMessageProcessor(CdcProcessor):

    def __init__(self):
        super(GroupedMessageProcessor, self).__init__(
            pg_table='sentry_groupedmessage',
        )

    DELETED_STATUS = 3

    def __prepare_date(self, dt):
        return datetime.strptime(
            "%s00" % dt, '%Y-%m-%d %H:%M:%S%z')

    def __build_record(self, xid, columnnames, columnvalues):
        raw_data = dict(zip(columnnames, columnvalues))
        string_cols = [
            'logger', 'message', 'view', 'data', 'platform'
        ]
        for c in string_cols:
            raw_data[c] = _unicodify(raw_data[c])

        raw_data['last_seen'] = self.__prepare_date(raw_data['last_seen'])
        raw_data['first_seen'] = self.__prepare_date(raw_data['first_seen'])
        raw_data['resolved_at'] = self.__prepare_date(raw_data['resolved_at']) if raw_data['resolved_at'] else None
        raw_data['active_at'] = self.__prepare_date(raw_data['active_at'])
        raw_data['is_public'] = 1 if raw_data['is_public'] else 0
        raw_data['project_id'] = raw_data['project_id'] or 0

        raw_data['commit_id'] = xid
        raw_data['deleted'] = 1 if raw_data['status'] == self.DELETED_STATUS else 0
        return raw_data

    def _process_insert(self, xid, columnnames, columnvalues):
        return self.__build_record(xid, columnnames, columnvalues)

    def _process_update(self, xid, key, columnnames, columnvalues):
        return self.__build_record(xid, columnnames, columnvalues)
