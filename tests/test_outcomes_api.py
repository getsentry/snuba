import itertools
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional, Tuple, Union

import pytest
import simplejson as json
from sentry_relay.consts import DataCategory

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.base import BaseApiTest
from tests.helpers import write_processed_messages


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestLegacyOutcomesApi(BaseApiTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "outcomes"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_teardown(
        self, clickhouse_db: None, _build_snql_post_methods: Callable[[str], Any]
    ) -> None:
        self.post = _build_snql_post_methods

        self.skew_minutes = 180
        self.skew = timedelta(minutes=self.skew_minutes)
        self.base_time = (
            datetime.utcnow().replace(minute=0, second=0, microsecond=0) - self.skew
        )
        self.storage = get_writable_storage(StorageKey.OUTCOMES_RAW)

    @pytest.fixture(scope="class")
    def get_project_id(self, request: object) -> Callable[[], int]:
        id_iter = itertools.count()
        next(id_iter)  # skip 0
        return lambda: next(id_iter)

    def generate_outcomes(
        self,
        org_id: int,
        project_id: int,
        num_outcomes: int,
        outcome: int,
        time_since_base: timedelta,
        category: Optional[int],
        quantity: Optional[int] = None,
    ) -> None:
        outcomes = []
        for _ in range(num_outcomes):
            message = {
                "project_id": project_id,
                "event_id": uuid.uuid4().hex,
                "timestamp": (self.base_time + time_since_base).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "org_id": org_id,
                "reason": None,
                "key_id": 1,
                "outcome": outcome,
                "category": category,
                "quantity": quantity,
            }
            if message["category"] is None:
                del message["category"]  # for testing None category case
            if message["quantity"] is None:
                del message["quantity"]  # for testing None quantity case
            processed = (
                self.storage.get_table_writer()
                .get_stream_loader()
                .get_processor()
                .process_message(
                    message,
                    KafkaMessageMetadata(0, 0, self.base_time),
                )
            )
            if processed:
                outcomes.append(processed)

        write_processed_messages(self.storage, outcomes)

    def format_time(self, time: datetime) -> str:
        return time.replace(tzinfo=timezone.utc).isoformat()

    def test_happy_path_querying(self, get_project_id: Callable[[], int]) -> None:
        project_id = get_project_id()
        other_project_id = get_project_id()
        # the outcomes we are going to query; multiple project over multiple times
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=5,
            outcome=0,
            time_since_base=timedelta(minutes=1),
            category=DataCategory.ERROR,
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=5,
            outcome=0,
            time_since_base=timedelta(minutes=30),
            category=DataCategory.TRANSACTION,
        )
        self.generate_outcomes(
            org_id=1,
            project_id=other_project_id,
            num_outcomes=10,
            outcome=0,
            time_since_base=timedelta(minutes=30),
            category=DataCategory.SECURITY,
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=10,
            outcome=0,
            time_since_base=timedelta(minutes=61),
            category=None,
        )

        # outcomes for a different outcome
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=1,
            time_since_base=timedelta(minutes=1),
            category=DataCategory.ERROR,
        )

        # outcomes outside the time range we are going to request
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            time_since_base=timedelta(minutes=(self.skew_minutes + 60)),
            category=DataCategory.ERROR,
        )

        from_date = self.format_time(self.base_time - self.skew)
        to_date = self.format_time(self.base_time + self.skew)

        response = self.post(
            json.dumps(
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
                    "tenant_ids": {"referrer": "test", "organization_id": 1},
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

    def test_category_quantity_sum_querying(
        self, get_project_id: Callable[[], int]
    ) -> None:
        project_id = get_project_id()
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.ERROR,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            category=None,  # should be counted as an Error
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.SECURITY,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.TRANSACTION,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            quantity=6,
            category=DataCategory.SESSION,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            quantity=4,
            category=DataCategory.SESSION,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.ATTACHMENT,
            quantity=65536,
            time_since_base=timedelta(minutes=30),
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=1,
            outcome=0,
            category=DataCategory.ATTACHMENT,
            quantity=16384,
            time_since_base=timedelta(minutes=30),
        )

        from_date = self.format_time(self.base_time - self.skew)
        to_date = self.format_time(self.base_time + self.skew)

        response = self.post(
            json.dumps(
                {
                    "dataset": "outcomes",
                    "aggregations": [
                        ["sum", "times_seen", "times_seen"],
                        ["sum", "quantity", "quantity_sum"],
                    ],
                    "from_date": from_date,
                    "selected_columns": [],
                    "to_date": to_date,
                    "organization": 1,
                    "conditions": [
                        ["timestamp", ">", from_date],
                        ["timestamp", "<=", to_date],
                        ["project_id", "=", project_id],
                    ],
                    "groupby": ["category"],
                    "tenant_ids": {"referrer": "test", "organization_id": 1},
                }
            ),
        )

        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 5
        correct_data = [
            {"category": DataCategory.ERROR, "times_seen": 2, "quantity_sum": 2},
            {"category": DataCategory.TRANSACTION, "times_seen": 1, "quantity_sum": 1},
            {"category": DataCategory.SECURITY, "times_seen": 1, "quantity_sum": 1},
            {
                "category": DataCategory.ATTACHMENT,
                "times_seen": 2,
                "quantity_sum": (65536 + 16384),
            },
            {
                "category": DataCategory.SESSION,
                "times_seen": 2,
                "quantity_sum": (6 + 4),
            },
        ]
        assert data["data"] == correct_data


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
class TestOutcomesAPI(BaseApiTest):
    def post(self, url: str, data: str) -> Any:
        return self.app.post(url, data=data, headers={"referer": "test"})

    @pytest.fixture(scope="class")
    def get_project_id(self, request: object) -> Callable[[], int]:
        id_iter = itertools.count()
        next(id_iter)  # skip 0
        return lambda: next(id_iter)

    def generate_outcomes(
        self,
        org_id: int,
        project_id: int,
        num_outcomes: int,
        outcome: int,
        time_since_base: timedelta,
        category: Optional[int],
        quantity: Optional[int] = None,
    ) -> None:
        outcomes = []
        for _ in range(num_outcomes):
            message = {
                "project_id": project_id,
                "event_id": uuid.uuid4().hex,
                "timestamp": (self.base_time + time_since_base).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "org_id": org_id,
                "reason": None,
                "key_id": 1,
                "outcome": outcome,
                "category": category,
                "quantity": quantity,
            }
            if message["category"] is None:
                del message["category"]  # for testing None category case
            if message["quantity"] is None:
                del message["quantity"]  # for testing None quantity case
            processed = (
                self.storage.get_table_writer()
                .get_stream_loader()
                .get_processor()
                .process_message(
                    message,
                    KafkaMessageMetadata(0, 0, self.base_time),
                )
            )
            if processed:
                outcomes.append(processed)

        write_processed_messages(self.storage, outcomes)

    def format_time(self, time: datetime) -> str:
        return time.replace(tzinfo=timezone.utc).isoformat()

    @pytest.fixture(autouse=True)
    def setup_teardown(self, clickhouse_db: None) -> None:
        self.skew = timedelta(minutes=180)
        self.base_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) - timedelta(minutes=180)
        self.next_time = datetime.utcnow().replace(
            minute=0, second=0, microsecond=0
        ) + timedelta(minutes=180)
        self.storage = get_writable_storage(StorageKey.OUTCOMES_RAW)

    def test_virtual_time_column(self, get_project_id: Callable[[], int]) -> None:
        project_id = get_project_id()
        self.generate_outcomes(
            org_id=2,
            project_id=project_id,
            num_outcomes=5,
            outcome=0,
            time_since_base=timedelta(minutes=1),
            category=DataCategory.ERROR,
        )
        self.generate_outcomes(
            org_id=2,
            project_id=project_id,
            num_outcomes=5,
            outcome=0,
            time_since_base=timedelta(minutes=30),
            category=DataCategory.TRANSACTION,
        )
        self.generate_outcomes(
            org_id=1,
            project_id=project_id,
            num_outcomes=3,
            outcome=0,
            time_since_base=timedelta(minutes=30),
            category=DataCategory.TRANSACTION_INDEXED,
        )
        self.generate_outcomes(
            org_id=2,
            project_id=project_id,
            num_outcomes=10,
            outcome=0,
            time_since_base=timedelta(minutes=61),
            category=None,
        )
        self.generate_outcomes(
            org_id=2,
            project_id=project_id,
            num_outcomes=1,
            outcome=1,
            time_since_base=timedelta(minutes=1),
            category=DataCategory.ERROR,
        )

        response = self.post(
            "/outcomes/snql",
            data=json.dumps(
                {
                    "query": f"""MATCH (outcomes )
                    SELECT sum(times_seen) AS aggregate BY time
                    WHERE org_id = 2
                    AND project_id IN tuple({project_id})
                    AND outcome = 0
                    AND timestamp >= toDateTime('{self.base_time.isoformat()}')
                    AND timestamp < toDateTime('{self.next_time.isoformat()}')
                    GRANULARITY 60
                    """,
                    "dataset": "outcomes",
                    "tenant_ids": {"referrer": "test", "organization_id": 1},
                }
            ),
        )
        data = json.loads(response.data)
        assert response.status_code == 200
        assert len(data["data"]) == 2
        assert all([row["aggregate"] == 10 for row in data["data"]])
