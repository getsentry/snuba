import petname
import random
import uuid
import sys
from datetime import datetime

from base import BaseTest


class TestWriter(BaseTest):
    def test_foo(self):
        # only the required fields
        events = [{
            'event_id': uuid.uuid4().hex,
            'timestamp': datetime.now(),
            'platform': petname.generate(),
            'message': petname.generate(),
            'primary_hash': uuid.uuid4().hex[:16],
            'project_id': random.randint(0, sys.maxint),
            'received': datetime.now(),
        }]

        self._write_events(events)

        res = self.conn.execute("SELECT count() FROM %s" % self.table)

        assert res[0][0] == 1
