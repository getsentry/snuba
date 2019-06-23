from snuba import processor
from snuba.consumer import KAFKA_MESSAGE_OFFSET_KEY
from snuba.processor import MessageProcessor


class CdcProcessor(MessageProcessor):

    def __init__(self, pg_table):
        self.pg_table = pg_table

    def _process_begin(self, offset):
        pass

    def _process_commit(self, offset):
        pass

    def _process_insert(self, offset, columnnames, columnvalues):
        pass

    def _process_update(self, offset, key, columnnames, columnvalues):
        pass

    def _process_delete(self, offset, key):
        pass

    def process_message(self, value, metadata):
        assert isinstance(value, dict)
        assert isinstance(metadata, dict)

        offset = metadata[KAFKA_MESSAGE_OFFSET_KEY]

        event = value['event']
        if event == 'begin':
            message = self._process_begin(offset)
        elif event == 'commit':
            message = self._process_commit(offset)
        elif event == 'change':
            table_name = value['table']
            if table_name != self.pg_table:
                return None

            operation = value['kind']
            if operation == 'insert':
                message = self._process_insert(
                    offset, value['columnnames'], value['columnvalues'])
            elif operation == 'update':
                message = self._process_update(
                    offset, value['oldkeys'], value['columnnames'], value['columnvalues'])
            elif operation == 'delete':
                message = self._process_delete(offset, value['oldkeys'])
            else:
                raise ValueError("Invalid value for operation in replication log: %s" % value['kind'])
        else:
            raise ValueError("Invalid value for event in replication log: %s" % value['event'])

        if message is None:
            return None

        return (processor.INSERT, message)
