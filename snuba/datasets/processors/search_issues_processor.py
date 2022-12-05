from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, TypedDict

from snuba import environment
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import EventTooOld, enforce_retention
from snuba.datasets.processors import DatasetMessageProcessor
from snuba.processor import InsertBatch, ProcessedMessage
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "search_issues.processor")


class SearchIssueMessage(TypedDict, total=False):
    # meta
    payload_version: int
    retention_days: int

    # issue-related
    organization_id: int
    project_id: int
    group_id: int
    search_title: str
    fingerprint: Sequence[str]
    occurrence_id: str
    occurrence_type_id: int
    detection_timestamp: float

    # occurrence-related
    event_id: Optional[str]
    trace_id: Optional[str]
    platform: str
    environment: Optional[str]
    release: Optional[str]
    dist: Optional[str]
    receive_timestamp: float
    client_timestamp: float
    tags: Mapping[str, Any]
    user: Mapping[str, Any]  # user, user_hash, user_id, user_name, user_email
    ip_address: str
    sdk: Mapping[str, Any]  # sdk_name, sdk_version
    contexts: Mapping[str, Any]
    request: Mapping[str, Any]  # http_method, http_referer
    retention_days: int


class SearchIssuesMessageProcessor(DatasetMessageProcessor):
    def process_message(
        self, message: SearchIssueMessage, metadata: KafkaMessageMetadata
    ) -> Optional[ProcessedMessage]:
        try:
            detection_timestamp = datetime.utcfromtimestamp(
                message["detection_timestamp"]
            )

            try:
                retention_days = enforce_retention(
                    message.get("retention_days", 90), detection_timestamp
                )
            except EventTooOld:
                metrics.increment("event_too_old")
                return None

            processed = {
                "project_id": message["project_id"],
                "organization_id": message["organization_id"],
                "group_id": message["group_id"],
                "detection_timestamp": detection_timestamp,
                "retention_days": retention_days,
            }
        except IndexError:
            metrics.increment("invalid_message")
            return None
        except ValueError:
            metrics.increment("invalid_uuid")
            return None
        except KeyError:
            metrics.increment("missing_field")
            return None
        except Exception:
            metrics.increment("process_message_error")
            raise
        return InsertBatch([processed], None)
