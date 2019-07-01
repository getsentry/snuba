from dateutil.parser import parse as dateutil_parse
from snuba.datasets.cdc.cdcprocessors import CdcProcessor


class GroupedMessageProcessor(CdcProcessor):

    def __init__(self):
        super(GroupedMessageProcessor, self).__init__(
            pg_table='sentry_groupedmessage',
        )

    def __build_record(self, offset, columnnames, columnvalues):
        raw_data = dict(zip(columnnames, columnvalues))
        output = {
            'offset': offset,
            'id': raw_data['id'],
            'record_deleted': 0,
            'status': raw_data['status'],
            'last_seen': dateutil_parse(raw_data['last_seen']),
            'first_seen': dateutil_parse(raw_data['first_seen']),
            'active_at': dateutil_parse(raw_data['active_at']),
            'first_release_id': raw_data['first_release_id'],
        }

        return output

    def _process_insert(self, offset, columnnames, columnvalues):
        return self.__build_record(offset, columnnames, columnvalues)

    def _process_delete(self, offset, key):
        key_names = key['keynames']
        key_values = key['keyvalues']
        id = key_values[key_names.index('id')]
        return {
            'offset': offset,
            'id': id,
            'record_deleted': 1,
            'status': None,
            'last_seen': None,
            'first_seen': None,
            'active_at': None,
            'first_release_id': None,
        }

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
