from snuba import processor
from snuba.processor import MessageProcessor


class CdcProcessor(MessageProcessor):

    def __init__(self, pg_table):
        self.pg_table = pg_table

    def _process_begin(self, xid):
        pass

    def _process_commit(self):
        pass

    def _process_insert(self, xid, columnnames, columnvalues):
        pass

    def _process_update(self, xid, key, columnnames, columnvalues):
        pass

    def _process_delete(self, xid, key):
        pass

    def process_message(self, value):
        assert isinstance(value, dict)
        event = value['event']
        if event == 'begin':
            xid = value['xid']
            message = self._process_begin(xid)
        elif event == 'commit':
            message = self._process_commit()
        elif event == 'change':
            table_name = value['table']
            if table_name != self.pg_table:
                return None

            operation = value['kind']
            xid = value['xid']
            if operation == 'insert':
                message = self._process_insert(
                    xid, value['columnnames'], value['columnvalues'])
            elif operation == 'update':
                message = self._process_update(
                    xid, value['oldkeys'], value['columnnames'], value['columnvalues'])
            elif operation == 'delete':
                message = self._process_delete(xid)
            else:
                raise
        else:
            raise

        if message is None:
            return None

        return (processor.INSERT, message)
