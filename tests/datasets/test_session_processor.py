from tests.base import BaseTest

from datetime import datetime, timedelta

from snuba.datasets.sessions import SessionsProcessor
from snuba.consumer import KafkaMessageMetadata
from snuba.processor import ProcessorAction


class TestSessionProcessor(BaseTest):
    def test_ingest_session_event_max_sample_rate(self):
        timestamp = datetime.utcnow()
        started = timestamp - timedelta(hours=1)
        payload = {
            "device_family": "iPhone12,3",
            "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
            "duration": 1947.49,
            "environment": "production",
            "org_id": 1,
            "os": "iOS",
            "os_version": "13.3.1",
            "project_id": 42,
            "release": "sentry-test@1.0.0",
            "retention_days": 90,
            "seq": 42,
            "errors": 0,
            "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "started": started.timestamp(),
            "status": "exited",
            "timestamp": timestamp.timestamp(),
        }

        meta = KafkaMessageMetadata(offset=1, partition=2)
        processor = SessionsProcessor()
        ret = processor.process_message(payload, meta)
        assert ret is not None

        assert ret.action == ProcessorAction.INSERT
        assert ret.data == [
            {
                "deleted": 0,
                "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
                "duration": 1947490,
                "environment": "production",
                "org_id": 1,
                "project_id": 42,
                "release": "sentry-test@1.0.0",
                "retention_days": 90,
                "seq": 42,
                "errors": 0,
                "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
                "started": started,
                "status": 1,
                "timestamp": timestamp,
            }
        ]
