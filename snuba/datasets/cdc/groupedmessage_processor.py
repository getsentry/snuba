from datetime import datetime

from snuba.datasets.cdc.cdcprocessors import CdcProcessor


class GroupedMessageProcessor(CdcProcessor):

    def __init__(self):
        super(GroupedMessageProcessor, self).__init__(
            pg_table='sentry_groupedmessage',
        )

    def __prepare_date(self, dt):
        return datetime.strptime(
            "%s00" % dt, '%Y-%m-%d %H:%M:%S%z')

    def __build_record(self, offset, columnnames, columnvalues):
        raw_data = dict(zip(columnnames, columnvalues))
        output = {
            'offset': offset,
            'id': raw_data['id'],
            'status': raw_data['status'],
            'last_seen': self.__prepare_date(raw_data['last_seen']),
            'first_seen': self.__prepare_date(raw_data['first_seen']),
            'active_at': self.__prepare_date(raw_data['active_at']),
            'first_release_id': raw_data['first_release_id'],
        }

        return output

    def _process_insert(self, offset, columnnames, columnvalues):
        return self.__build_record(offset, columnnames, columnvalues)

    def _process_update(self, offset, key, columnnames, columnvalues):
        new_id = columnvalues[columnnames.index('id')]
        key_names = key['keynames']
        key_values = key['keyvalues']
        old_id = key_values[key_names.index('id')]
        # We cannot support a change in the identity of the record
        # clickhouse will use the identity column to find rows to merge.
        # if we change it, merging won't work.
        assert old_id == new_id, 'Changing Primary Key is not supported.'
        return self.__build_record(offset, columnnames, columnvalues)
