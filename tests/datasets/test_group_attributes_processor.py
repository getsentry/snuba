from datetime import datetime
from typing import Optional

import pytest
from sentry_kafka_schemas.schema_types.group_attributes_v1 import (
    GroupAttributesSnapshot,
)

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.group_attributes_processor import (
    GroupAttributesMessageProcessor,
)
from snuba.processor import ProcessedMessage
from snuba.writer import WriterTableRow


@pytest.fixture
def group_created() -> GroupAttributesSnapshot:
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


class TestGroupAttributesMessageProcessor:
    KAFKA_META = KafkaMessageMetadata(
        offset=0, partition=0, timestamp=datetime(1970, 1, 1)
    )

    processor = GroupAttributesMessageProcessor()

    def process_message(
        self, message, kafka_meta: KafkaMessageMetadata = KAFKA_META
    ) -> Optional[ProcessedMessage]:
        return self.processor.process_message(message, kafka_meta)

    def processed_single_row(self, message) -> WriterTableRow:
        return self.process_message(message).rows[0]

    def test_group_created(self, group_created):
        assert (
            self.processed_single_row(group_created).items()
            >= {
                "project_id": 1,
                "group_id": 1,
                "group_status": 0,
                "group_substatus": 7,
                "group_first_seen": datetime.strptime(
                    group_created["first_seen"], settings.PAYLOAD_DATETIME_FORMAT
                ),
                "group_num_comments": 0,
                "assignee_user_id": None,
                "assignee_team_id": None,
                "owner_suspect_commit_user_id": None,
                "owner_ownership_rule_user_id": None,
                "owner_ownership_rule_team_id": None,
                "owner_codeowners_user_id": None,
                "owner_codeowners_team_id": None,
                "deleted": 0,
            }.items()
        )
