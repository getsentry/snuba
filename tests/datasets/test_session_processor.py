from datetime import datetime, timedelta, timezone

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.sessions_processor import SessionsProcessor
from snuba.processor import InsertBatch, json_encode_insert_batch


class TestSessionProcessor:
    def test_ingest_session_event_max_sample_rate(self) -> None:
        timestamp = datetime.now(timezone.utc)
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
            "received": timestamp.timestamp(),
        }

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        assert SessionsProcessor().process_message(
            payload, meta
        ) == json_encode_insert_batch(
            InsertBatch(
                [
                    {
                        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
                        "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
                        "quantity": 1,
                        "seq": 42,
                        "org_id": 1,
                        "project_id": 42,
                        "retention_days": 90,
                        "duration": 1947490,
                        "status": 1,
                        "errors": 0,
                        "received": timestamp.replace(tzinfo=None),
                        "started": started.replace(tzinfo=None),
                        "release": "sentry-test@1.0.0",
                        "environment": "production",
                    }
                ],
                None,
            )
        )

    def test_ingest_session_event_abnormal(self) -> None:
        timestamp = datetime.now(timezone.utc)
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
            "status": "abnormal",
            "received": timestamp.timestamp(),
        }

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        assert SessionsProcessor().process_message(
            payload, meta
        ) == json_encode_insert_batch(
            InsertBatch(
                [
                    {
                        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
                        "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
                        "quantity": 1,
                        "seq": 42,
                        "org_id": 1,
                        "project_id": 42,
                        "retention_days": 90,
                        "duration": 1947490,
                        "status": 3,
                        # abnormal counts as at least one error
                        "errors": 1,
                        "received": timestamp.replace(tzinfo=None),
                        "started": started.replace(tzinfo=None),
                        "release": "sentry-test@1.0.0",
                        "environment": "production",
                    }
                ],
                None,
            )
        )

    def test_ingest_session_event_crashed(self) -> None:
        timestamp = datetime.now(timezone.utc)
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
            "status": "crashed",
            "received": timestamp.timestamp(),
        }

        meta = KafkaMessageMetadata(
            offset=1, partition=2, timestamp=datetime(1970, 1, 1)
        )
        assert SessionsProcessor().process_message(
            payload, meta
        ) == json_encode_insert_batch(
            InsertBatch(
                [
                    {
                        "session_id": "8333339f-5675-4f89-a9a0-1c935255ab58",
                        "distinct_id": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
                        "quantity": 1,
                        "seq": 42,
                        "org_id": 1,
                        "project_id": 42,
                        "retention_days": 90,
                        "duration": 1947490,
                        "status": 2,
                        # abnormal counts as at least one error
                        "errors": 1,
                        "received": timestamp.replace(tzinfo=None),
                        "started": started.replace(tzinfo=None),
                        "release": "sentry-test@1.0.0",
                        "environment": "production",
                    }
                ],
                None,
            )
        )
