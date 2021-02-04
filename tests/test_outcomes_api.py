import pytest
import uuid
from datetime import datetime, timedelta

import pytz
import simplejson as json

from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages
from sentry_relay import DataCategory


class TestOutcomesApi(BaseApiTest):
    @pytest.fixture(
        autouse=True, params=["/query", "/outcomes/snql"], ids=["legacy", "snql"]
    )
    def _set_endpoint(self, request, convert_legacy_to_snql):
        self.endpoint = request.param
        self.multiplier = 1
        if request.param == "/outcomes/snql":
            self.multiplier = 2
            old_post = self.app.post

            def new_post(endpoint, data=None):
                return old_post(endpoint, data=convert_legacy_to_snql(data, "outcomes"))

            self.app.post = new_post

    def setup_method(self, test_method):
        super().setup_method(test_method)

        self.skew = timedelta(minutes=180)
        self.base_time = datetime.utcnow().replace(
            second=0, microsecond=0, tzinfo=pytz.utc
        ) - timedelta(minutes=90)
        self.storage = get_writable_storage(StorageKey.OUTCOMES_RAW)

    def generate_outcomes(
        self,
        org_id: int,
        project_id: int,
        num_outcomes: int,
        category: int,
        time_since_base: timedelta,
        outcome: int,
    ) -> None:
        outcomes = []
        for _ in range(num_outcomes):
            processed = (
                self.storage.get_table_writer()
                .get_stream_loader()
                .get_processor()
                .process_message(
                    {
                        "project_id": project_id,
                        "event_id": uuid.uuid4().hex,
                        "timestamp": (self.base_time + time_since_base).strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        ),
                        "org_id": org_id,
                        "reason": None,
                        "key_id": 1,
                        "category": category,
                        "outcome": outcome,
                    },
                    None,
                )
            )

            outcomes.append(processed)

        write_processed_messages(self.storage, outcomes)

    def format_time(self, time: datetime) -> str:
        return time.replace(tzinfo=pytz.utc).isoformat()

    # def test_happy_path_querying(self):
    #     # the outcomes we are going to query; multiple project over multiple times
    #     self.generate_outcomes(
    #         org_id=1,
    #         project_id=1,
    #         num_outcomes=5,
    #         outcome=0,
    #         category=DataCategory.ERROR,
    #         time_since_base=timedelta(minutes=1),
    #     )
    #     self.generate_outcomes(
    #         org_id=1,
    #         project_id=1,
    #         num_outcomes=5,
    #         outcome=0,
    #         category=DataCategory.ERROR,
    #         time_since_base=timedelta(minutes=30),
    #     )
    #     self.generate_outcomes(
    #         org_id=1,
    #         project_id=2,
    #         num_outcomes=10,
    #         outcome=0,
    #         category=DataCategory.ERROR,
    #         time_since_base=timedelta(minutes=30),
    #     )
    #     self.generate_outcomes(
    #         org_id=1,
    #         project_id=1,
    #         num_outcomes=10,
    #         outcome=0,
    #         category=DataCategory.ERROR,
    #         time_since_base=timedelta(minutes=61),
    #     )

    #     # outcomes for a different outcome
    #     self.generate_outcomes(
    #         org_id=1,
    #         project_id=1,
    #         num_outcomes=1,
    #         outcome=1,
    #         category=DataCategory.ERROR,
    #         time_since_base=timedelta(minutes=1),
    #     )

    #     # outcomes outside the time range we are going to request
    #     self.generate_outcomes(
    #         org_id=1,
    #         project_id=1,
    #         num_outcomes=1,
    #         outcome=0,
    #         category=DataCategory.ERROR,
    #         time_since_base=timedelta(minutes=(self.skew_minutes + 60)),
    #     )

    #     from_date = self.format_time(self.base_time - self.skew)
    #     to_date = self.format_time(self.base_time + self.skew)

    #     response = self.app.post(
    #         self.endpoint,
    #         data=json.dumps(
    #             {
    #                 "dataset": "outcomes",
    #                 "aggregations": [["sum", "times_seen", "aggregate"]],
    #                 "from_date": from_date,
    #                 "selected_columns": [],
    #                 "to_date": to_date,
    #                 "organization": 1,
    #                 "conditions": [
    #                     ["outcome", "=", 0],
    #                     ["project_id", "IN", [1, 2]],
    #                     ["timestamp", ">", from_date],
    #                     ["timestamp", "<=", to_date],
    #                 ],
    #                 "groupby": ["project_id", "time"],
    #             }
    #         ),
    #     )

    #     data = json.loads(response.data)
    #     assert response.status_code == 200
    #     assert len(data["data"]) == 3
    #     assert all([row["aggregate"] == 10 * self.multiplier for row in data["data"]])
    #     assert sorted([row["project_id"] for row in data["data"]]) == [1, 1, 2]

    def test_category_sum_querying(self):
        self.generate_outcomes(
            org_id=1,
            project_id=1,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.ERROR,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=1,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.SECURITY,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=1,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.TRANSACTION,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=1,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.ATTACHMENT,
            time_since_base=timedelta(minutes=30),
        )

        from_date = self.format_time(self.base_time - self.skew)
        to_date = self.format_time(self.base_time + self.skew)
        response = self.app.post(
            self.endpoint,
            data=json.dumps(
                {
                    "dataset": "outcomes",
                    "aggregations": [["sum", "times_seen", "aggregate"]],
                    "from_date": from_date,
                    "selected_columns": [],
                    "to_date": to_date,
                    "organization": 1,
                    "conditions": [
                        ["timestamp", ">", from_date],
                        ["timestamp", "<=", to_date],
                    ],
                    "groupby": ["time", "category"],
                }
            ),
        )
        data = json.loads(response.data)
        print(json.dumps(data))
        assert response.status_code == 200
        assert len(data["data"]) == 4
        assert all([row["aggregate"] == 1 for row in data["data"]])
