import itertools
import pytest
import pytz
import uuid
from datetime import datetime, timedelta
import simplejson as json
from typing import Any, Callable, Tuple, Union


from snuba.datasets.storages import StorageKey
from snuba.datasets.storages.factory import get_writable_storage
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


class TestOutcomesApi(BaseApiTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "outcomes"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[[Any], Any]) -> None:
        self.post = _build_snql_post_methods

    @pytest.fixture(scope="class")
    def get_project_id(self, request: object) -> Callable[[], int]:
        id_iter = itertools.count()
        next(id_iter)  # skip 0
        return lambda: next(id_iter)

    def setup_method(self, test_method):
        super().setup_method(test_method)

        self.skew_minutes = 180
        self.skew = timedelta(minutes=self.skew_minutes)
        self.base_time = (
            datetime.utcnow().replace(minute=0, second=0, microsecond=0) - self.skew
        )
        self.storage = get_writable_storage(StorageKey.OUTCOMES_RAW)

    def generate_outcomes(
        self,
        org_id: int,
        project_id: int,
        num_outcomes: int,
        outcome: int,
        time_since_base: timedelta,
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
                        "outcome": outcome,
                    },
                    None,
                )
            )

            outcomes.append(processed)

        write_processed_messages(self.storage, outcomes)

    def format_time(self, time: datetime) -> str:
        return time.replace(tzinfo=pytz.utc).isoformat()

    def test_happy_path_querying(self, get_project_id):
        project_id = get_project_id()
        other_project_id = get_project_id()
        # the outcomes we are going to query; multiple project over multiple times
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=5,
            outcome=0,
            time_since_base=timedelta(minutes=1),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=5,
            outcome=0,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=other_project_id,
            num_outcomes=10,
            outcome=0,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=10,
            outcome=0,
            time_since_base=timedelta(minutes=61),
        )

        # outcomes for a different outcome
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=1,
            time_since_base=timedelta(minutes=1),
        )

        # outcomes outside the time range we are going to request
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            time_since_base=timedelta(minutes=(self.skew_minutes + 60)),
        )

        from_date = self.format_time(self.base_time - self.skew)
        to_date = self.format_time(self.base_time + self.skew)

        response = self.post(
            data=json.dumps(
                {
                    "dataset": "outcomes",
                    "aggregations": [["sum", "times_seen", "aggregate"]],
                    "from_date": from_date,
                    "selected_columns": [],
                    "to_date": to_date,
                    "organization": 1,
                    "conditions": [
                        ["outcome", "=", 0],
                        ["project_id", "IN", [project_id, other_project_id]],
                        ["timestamp", ">", from_date],
                        ["timestamp", "<=", to_date],
                    ],
                    "groupby": ["project_id", "time"],
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 3
        assert all([row["aggregate"] == 10 for row in data["data"]])
        assert sorted([row["project_id"] for row in data["data"]]) == [
            project_id,
            project_id,
            other_project_id,
        ]
