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
        output = {
            'commit_id': xid,
            'id': raw_data['id'],
            'logger': _unicodify(raw_data['logger']),
            'level': raw_data['level'],
            'message': _unicodify(raw_data['message']),
            'view': _unicodify(raw_data['view']),
            'status': raw_data['status'],
            'last_seen': self.__prepare_date(raw_data['last_seen']),
            'first_seen': self.__prepare_date(raw_data['first_seen']),
            'project_id': raw_data['project_id'] or 0,
            'active_at': self.__prepare_date(raw_data['active_at']),
            'platform': _unicodify(raw_data['platform']),
            'first_release_id': raw_data['first_release_id'],
        }

        return output

    def _process_insert(self, xid, columnnames, columnvalues):
        return self.__build_record(xid, columnnames, columnvalues)

    def _process_update(self, xid, key, columnnames, columnvalues):
        return self.__build_record(xid, columnnames, columnvalues)
