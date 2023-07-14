from typing import Optional

from sentry_kafka_schemas.schema_types.group_attributes_v1 import (
    GroupAttributesSnapshot,
)

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "group_attributes.processor")


class GroupAttributesMessageProcessor(DatasetMessageProcessor):
    def process_message(
        self, message: GroupAttributesSnapshot, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        return InsertBatch(
            [
                {
                    "project_id": message["project_id"],
                    "group_id": message["group_id"],
                    "group_status": message["status"],
                    "group_substatus": message["substatus"],
                    "group_first_seen": message["first_seen"],
                    "group_num_comments": message["num_comments"],
                    "assignee_user_id": message["assignee_user_id"],
                    "assignee_team_id": message["assignee_team_id"],
                    "owner_suspect_commit_user_id": message[
                        "owner_suspect_commit_user_id"
                    ],
                    "owner_ownership_rule_user_id": message[
                        "owner_ownership_rule_user_id"
                    ],
                    "owner_ownership_rule_team_id": message[
                        "owner_ownership_rule_team_id"
                    ],
                    "owner_codeowners_user_id": message["owner_codeowners_user_id"],
                    "owner_codeowners_team_id": message["owner_codeowners_team_id"],
                    "deleted": message["group_deleted"],
                    "message_timestamp": metadata.timestamp,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                }
            ],
            None,
        )
