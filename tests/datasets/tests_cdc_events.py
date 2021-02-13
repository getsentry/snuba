from datetime import datetime, timedelta
from functools import partial

import pytz
import simplejson as json
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.utils.metrics.backends.dummy import DummyMetricsBackend
from tests.base import BaseApiTest
from tests.fixtures import get_raw_event
from tests.helpers import write_unprocessed_events


class TestCdcEvents(BaseApiTest):
    def setup_method(self, test_method):
        super().setup_method(test_method)
        self.app.post = partial(self.app.post, headers={"referer": "test"})
        self.event = get_raw_event()
        self.project_id = self.event["project_id"]
        self.base_time = datetime.utcnow().replace(
            second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=90)

        self.events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
        write_unprocessed_events(self.events_storage, [self.event])

        groups = [
            {
                "offset": 0,
                "project_id": self.project_id,
                "id": self.event["group_id"],
                "record_deleted": 0,
                "status": 0,
            }
        ]

        groups_storage = get_entity(EntityKey.GROUPEDMESSAGES).get_writable_storage()
        groups_storage.get_table_writer().get_batch_writer(
            metrics=DummyMetricsBackend(strict=True)
        ).write([json.dumps(group).encode("utf-8") for group in groups])

    def test_basic_join(self) -> None:
        query_template = (
            "MATCH (e: events) -[grouped]-> (g: groupedmessage) "
            "SELECT e.event_id WHERE "
            "e.project_id = %(project_id)s AND "
            "g.project_id = %(project_id)s AND "
            "g.id %(operator)s %(group_id)s AND "
            "e.timestamp > toDateTime('%(time)s')"
        )

        response = self.app.post(
            "/events/snql",
            data=json.dumps(
                {
                    "dataset": "events",
                    "query": query_template
                    % {
                        "project_id": self.project_id,
                        "operator": "=",
                        "group_id": self.event["group_id"],
                        "time": self.base_time,
                    },
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 1, data

        response = self.app.post(
            "/events/snql",
            data=json.dumps(
                {
                    "dataset": "events",
                    "query": query_template
                    % {
                        "project_id": self.project_id,
                        "operator": "!=",
                        "group_id": self.event["group_id"],
                        "time": self.base_time,
                    },
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 0, data
