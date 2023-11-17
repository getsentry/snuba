from datetime import datetime
from typing import Any, Callable, Tuple, Union

import pytest
import simplejson as json
from sentry_kafka_schemas.schema_types.group_attributes_v1 import (
    GroupAttributesSnapshot,
)

from snuba.consumers.types import KafkaMessageMetadata
from snuba.core.initialize import initialize_snuba
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from tests.base import BaseApiTest
from tests.datasets.configuration.utils import ConfigurationTest
from tests.helpers import write_processed_messages
from tests.test_api import SimpleAPITest


def attribute_message() -> GroupAttributesSnapshot:
    return {
        "group_deleted": False,
        "project_id": 1,
        "group_id": 1,
        "status": 0,
        "substatus": 7,
        "first_seen": "2023-02-27T15:40:12.223000Z",
        "num_comments": 0,
        "assignee_user_id": None,
        "assignee_team_id": None,
        "owner_suspect_commit_user_id": None,
        "owner_ownership_rule_user_id": None,
        "owner_ownership_rule_team_id": None,
        "owner_codeowners_user_id": None,
        "owner_codeowners_team_id": None,
        "timestamp": "2023-02-27T15:40:12.223000Z",
    }


def kafka_metadata() -> KafkaMessageMetadata:
    return KafkaMessageMetadata(offset=0, partition=0, timestamp=datetime(1970, 1, 1))


class TestGroupAttributesSnQLApi(SimpleAPITest, BaseApiTest, ConfigurationTest):
    @pytest.fixture
    def test_entity(self) -> Union[str, Tuple[str, str]]:
        return "group_attributes"

    @pytest.fixture
    def test_app(self) -> Any:
        return self.app

    @pytest.fixture(autouse=True)
    def setup_post(self, _build_snql_post_methods: Callable[..., Any]) -> None:
        self.post = _build_snql_post_methods

    def setup_method(self, test_method: Callable[..., Any]) -> None:
        super().setup_method(test_method)
        initialize_snuba()
        maybe_writable_storage = get_entity(
            EntityKey.GROUP_ATTRIBUTES
        ).get_writable_storage()
        assert maybe_writable_storage is not None

        self.writable_storage = maybe_writable_storage

    def insert_row(self, message: GroupAttributesSnapshot) -> None:
        processed = (
            self.writable_storage.get_table_writer()
            .get_stream_loader()
            .get_processor()
            .process_message(message, kafka_metadata())
        )
        assert processed is not None
        write_processed_messages(self.writable_storage, [processed])

    def post_query(
        self,
        query: str,
        turbo: bool = False,
        consistent: bool = True,
        debug: bool = True,
    ) -> Any:
        return self.app.post(
            "/group_attributes/snql",
            data=json.dumps(
                {
                    "query": query,
                    "turbo": False,
                    "consistent": True,
                    "debug": True,
                    "tenant_ids": {"referrer": "test", "organization_id": 1},
                }
            ),
            headers={"referer": "test"},
        )

    def test_endpoint(self) -> None:
        self.insert_row(attribute_message())

        response = self.post_query(
            """MATCH (group_attributes)
                SELECT project_id, group_id
                WHERE project_id = 1
                LIMIT 1000
            """
        )

        data = json.loads(response.data)

        assert response.status_code == 200, data
        assert data["stats"]["consistent"]
        assert data["data"] == [
            {
                "project_id": 1,
                "group_id": 1,
            }
        ]
