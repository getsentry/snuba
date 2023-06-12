from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence
from uuid import UUID

import pytest
import pytz

from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.processors.errors_processor import ErrorsProcessor
from snuba.processor import InsertBatch
from snuba.settings import PAYLOAD_DATETIME_FORMAT


@dataclass
class ErrorEvent:
    organization_id: int
    event_id: str
    group_id: int
    project_id: int
    platform: str
    message: str
    timestamp: datetime
    release: str | None
    dist: str | None
    username: str
    ip_address: str
    user_id: str
    email: str
    geo: Mapping[str, str]
    threads: Mapping[str, Any] | None
    trace_id: str
    trace_sampled: bool | None
    environment: str
    replay_id: uuid.UUID | None
    received_timestamp: datetime
    errors: Sequence[Mapping[str, Any]] | None

    def serialize(self) -> tuple[int, str, Mapping[str, Any]]:
        serialized_event: dict[str, Any] = {
            "organization_id": self.organization_id,
            "retention_days": 58,
            "event_id": self.event_id,
            "group_id": self.group_id,
            "project_id": self.project_id,
            "platform": self.platform,
            "message": "",
            "datetime": self.timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
            "primary_hash": str(uuid.UUID("04233d08ac90cf6fc015b1be5932e7e2")),
            "data": {
                "event_id": self.event_id,
                "project_id": self.project_id,
                "release": self.release,
                "platform": self.platform,
                "message": "",
                "datetime": self.timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                "tags": [
                    ["handled", "no"],
                    ["level", "error"],
                    ["mechanism", "excepthook"],
                    ["runtime", "CPython 3.7.6"],
                    ["runtime.name", "CPython"],
                    ["server_name", "snuba"],
                    ["environment", self.environment],
                    ["sentry:user", "this_is_me"],
                    ["sentry:dist", self.dist],
                    ["sentry:release", self.release],
                ],
                "user": {
                    "username": "me",
                    "ip_address": "127.0.0.1",
                    "id": "still_me",
                    "email": "me@myself.org",
                    "geo": {
                        "country_code": "XY",
                        "region": "fake_region",
                        "city": "fake_city",
                        "subdivision": "fake_subdivision",
                    },
                },
                "request": {
                    "url": "http://127.0.0.1:/query",
                    "headers": [
                        ["Accept-Encoding", "identity"],
                        ["Content-Length", "398"],
                        ["Host", "127.0.0.1:"],
                        ["Referer", "tagstore.something"],
                        ["Trace", "8fa73032d-1"],
                    ],
                    "data": "",
                    "method": "POST",
                    "env": {"SERVER_PORT": "1010", "SERVER_NAME": "snuba"},
                },
                "_relay_processed": True,
                "breadcrumbs": {
                    "values": [
                        {
                            "category": "snuba.utils.streams.batching",
                            "level": "info",
                            "timestamp": self.timestamp.timestamp(),
                            "data": {
                                "asctime": self.timestamp.strftime(
                                    PAYLOAD_DATETIME_FORMAT
                                )
                            },
                            "message": "New partitions assigned: {}",
                            "type": "default",
                        },
                        {
                            "category": "snuba.utils.streams.batching",
                            "level": "info",
                            "timestamp": self.timestamp.timestamp(),
                            "data": {
                                "asctime": self.timestamp.strftime(
                                    PAYLOAD_DATETIME_FORMAT
                                )
                            },
                            "message": "Flushing ",
                            "type": "default",
                        },
                        {
                            "category": "httplib",
                            "timestamp": self.timestamp.timestamp(),
                            "type": "http",
                            "data": {
                                "url": "http://127.0.0.1:8123/",
                                "status_code": 500,
                                "reason": "Internal Server Error",
                                "method": "POST",
                            },
                            "level": "info",
                        },
                    ]
                },
                "contexts": {
                    "runtime": {
                        "version": "3.7.6",
                        "type": "runtime",
                        "name": "CPython",
                        "build": "3.7.6",
                    },
                    "trace": {
                        "trace_id": self.trace_id,
                        "span_id": "deadbeef",
                    },
                },
                "culprit": "snuba.clickhouse.http in write",
                "exception": {
                    "values": [
                        {
                            "stacktrace": {
                                "frames": [
                                    {
                                        "function": "<module>",
                                        "abs_path": "/usr/local/bin/snuba",
                                        "pre_context": [
                                            "from pkg_resources import load_entry_point",
                                            "",
                                            "if __name__ == '__main__':",
                                            "    sys.argv[0] = re.sub(r'(-script\\.pyw?|\\.exe)?$', '', sys.argv[0])",
                                            "    sys.exit(",
                                        ],
                                        "post_context": ["    )"],
                                        "vars": {
                                            "__spec__": "None",
                                            "__builtins__": "<module 'builtins' (built-in)>",
                                            "__annotations__": {},
                                            "__file__": "'/usr/local/bin/snuba'",
                                            "__loader__": "<_frozen_importlib_external.SourceFileLoader object at 0x7fbbc3a36ed0>",
                                            "__requires__": "'snuba'",
                                            "__cached__": "None",
                                            "__name__": "'__main__'",
                                            "__package__": "None",
                                            "__doc__": "None",
                                        },
                                        "module": "__main__",
                                        "filename": "snuba",
                                        "lineno": 11,
                                        "in_app": False,
                                        "data": {"orig_in_app": 1},
                                        "context_line": "        load_entry_point('snuba', 'console_scripts', 'snuba')()",
                                    },
                                ]
                            },
                            "type": "ClickHouseError",
                            "thread_id": 3,
                            "module": "snuba.clickhouse.http",
                            "value": "[171] DB::Exception: Block structure mismatch",
                            "mechanism": {
                                "type": "excepthook",
                                "handled": False,
                            },
                        }
                    ]
                },
                "extra": {
                    "sys.argv": [
                        "/usr/local/bin/snuba",
                        "consumer",
                        "--dataset",
                        "transactions",
                    ]
                },
                "fingerprint": ["{{ default }}"],
                "hashes": ["c8b21c571231e989060b9110a2ade7d3"],
                "hierarchical_hashes": [
                    "04233d08ac90cf6fc015b1be5932e7e3",
                    "04233d08ac90cf6fc015b1be5932e7e4",
                ],
                "key_id": "537125",
                "level": "error",
                "location": "snuba/clickhouse/http.py",
                "logger": "",
                "metadata": {
                    "function": "write",
                    "type": "ClickHouseError",
                    "value": "[171] DB::Exception: Block structure mismatch",
                    "filename": "snuba/something.py",
                },
                "modules": {
                    "cffi": "1.13.2",
                    "ipython-genutils": "0.2.0",
                    "isodate": "0.6.0",
                },
                "received": self.received_timestamp.timestamp(),
                "sdk": {
                    "version": "0.0.0.0.1",
                    "name": "sentry.python",
                    "packages": [{"version": "0.0.0.0.1", "name": "pypi:sentry-sdk"}],
                    "integrations": [
                        "argv",
                        "atexit",
                        "dedupe",
                        "excepthook",
                        "logging",
                        "modules",
                        "stdlib",
                        "threading",
                    ],
                },
                "timestamp": self.timestamp.timestamp(),
                "title": "ClickHouseError: [171] DB::Exception: Block structure mismatch",
                "type": "error",
                "version": "7",
            },
        }

        if self.replay_id:
            serialized_event["data"]["contexts"]["replay"] = {
                "replay_id": str(self.replay_id)
            }
        if self.threads:
            serialized_event["data"]["threads"] = self.threads
        if self.trace_sampled:
            serialized_event["data"]["contexts"]["trace"][
                "sampled"
            ] = self.trace_sampled
        if self.errors:
            serialized_event["data"]["errors"] = self.errors

        return (
            2,
            "insert",
            serialized_event,
        )

    def build_result(self, meta: KafkaMessageMetadata) -> Mapping[str, Any]:
        expected_result = {
            "project_id": self.project_id,
            "timestamp": self.timestamp,
            "event_id": self.event_id,
            "platform": self.platform,
            "dist": self.dist,
            "environment": self.environment,
            "release": self.release,
            "ip_address_v4": self.ip_address,
            "user": "this_is_me",
            "user_name": "me",
            "user_id": "still_me",
            "user_email": "me@myself.org",
            "sdk_name": "sentry.python",
            "sdk_version": "0.0.0.0.1",
            "http_method": "POST",
            "http_referer": "tagstore.something",
            "trace_id": self.trace_id,
            "span_id": 3735928559,
            "tags.key": [
                "environment",
                "handled",
                "level",
                "mechanism",
                "runtime",
                "runtime.name",
                "sentry:dist",
                "sentry:release",
                "sentry:user",
                "server_name",
            ],
            "tags.value": [
                self.environment,
                "no",
                "error",
                "excepthook",
                "CPython 3.7.6",
                "CPython",
                self.dist,
                self.release,
                "this_is_me",
                "snuba",
            ],
            "contexts.key": [
                "runtime.version",
                "runtime.name",
                "runtime.build",
                "trace.trace_id",
                "trace.span_id",
                "geo.country_code",
                "geo.region",
                "geo.city",
                "geo.subdivision",
            ],
            "contexts.value": [
                "3.7.6",
                "CPython",
                "3.7.6",
                self.trace_id,
                "deadbeef",
                "XY",
                "fake_region",
                "fake_city",
                "fake_subdivision",
            ],
            "partition": meta.partition,
            "offset": meta.offset,
            "message_timestamp": self.timestamp,
            "retention_days": 90,
            "deleted": 0,
            "group_id": self.group_id,
            "primary_hash": "d36001ef-28af-2542-fde8-cf2935766141",
            "hierarchical_hashes": [
                str(UUID("04233d08ac90cf6fc015b1be5932e7e3")),
                str(UUID("04233d08ac90cf6fc015b1be5932e7e4")),
            ],
            "received": self.received_timestamp.astimezone(pytz.utc).replace(
                tzinfo=None, microsecond=0
            ),
            "message": "",
            "title": "ClickHouseError: [171] DB::Exception: Block structure mismatch",
            "culprit": "snuba.clickhouse.http in write",
            "level": "error",
            "location": "snuba/clickhouse/http.py",
            "version": "7",
            "type": "error",
            "exception_stacks.type": ["ClickHouseError"],
            "exception_stacks.value": ["[171] DB::Exception: Block structure mismatch"],
            "exception_stacks.mechanism_type": ["excepthook"],
            "exception_stacks.mechanism_handled": [False],
            "exception_frames.abs_path": ["/usr/local/bin/snuba"],
            "exception_frames.colno": [None],
            "exception_frames.filename": ["snuba"],
            "exception_frames.lineno": [11],
            "exception_frames.in_app": [False],
            "exception_frames.package": [None],
            "exception_frames.module": ["__main__"],
            "exception_frames.function": ["<module>"],
            "exception_frames.stack_level": [0],
            "sdk_integrations": [
                "argv",
                "atexit",
                "dedupe",
                "excepthook",
                "logging",
                "modules",
                "stdlib",
                "threading",
            ],
            "modules.name": ["cffi", "ipython-genutils", "isodate"],
            "modules.version": ["1.13.2", "0.2.0", "0.6.0"],
            "transaction_name": "",
        }

        if self.replay_id:
            expected_result["replay_id"] = str(self.replay_id)
            expected_result["tags.key"].insert(4, "replayId")
            expected_result["tags.value"].insert(4, self.replay_id.hex)

        if self.trace_sampled:
            expected_result["contexts.key"].insert(5, "trace.sampled")
            expected_result["contexts.value"].insert(5, str(self.trace_sampled))

        return expected_result


@pytest.mark.redis_db
class TestErrorsProcessor:

    processor = ErrorsProcessor(
        {
            "environment": "environment",
            "sentry:release": "release",
            "sentry:dist": "dist",
            "sentry:user": "user",
            "transaction": "transaction_name",
            "level": "level",
        }
    )

    def __get_timestamps(self) -> tuple[datetime, datetime]:
        timestamp = datetime.now() - timedelta(seconds=5)
        recieved = datetime.now() - timedelta(seconds=10)
        return timestamp, recieved

    def __get_error_event(self, timestamp: datetime, recieved: datetime) -> ErrorEvent:
        return ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            received_timestamp=recieved,
            threads=None,
            release="1.0.0",
            dist="dist",
            environment="prod",
            replay_id=None,
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            errors=None,
        )

    def test_errors_basic(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = self.__get_error_event(timestamp, recieved)
        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)
        processor = ErrorsProcessor(
            {
                "environment": "environment",
                "sentry:release": "release",
                "sentry:dist": "dist",
                "sentry:user": "user",
                "transaction": "transaction_name",
                "level": "level",
            }
        )
        assert processor.process_message(payload, meta) == InsertBatch(
            [message.build_result(meta)], None
        )

    def test_errors_replayid_context(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            received_timestamp=recieved,
            threads=None,
            release="1.0.0",
            dist="dist",
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=uuid.uuid4(),
            errors=None,
        )

        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        assert self.processor.process_message(payload, meta) == InsertBatch(
            [message.build_result(meta)], None
        )

    def test_errors_replayid_tag(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            threads=None,
            received_timestamp=recieved,
            release="1.0.0",
            dist="dist",
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=None,
            errors=None,
        )
        replay_id = uuid.uuid4()
        payload = message.serialize()
        payload[2]["data"]["tags"].append(["replayId", replay_id.hex])

        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)
        result["replay_id"] = str(replay_id)
        result["tags.key"].insert(4, "replayId")
        result["tags.value"].insert(4, replay_id.hex)
        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )

    def test_errors_replayid_tag_and_context(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        replay_id = uuid.uuid4()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            received_timestamp=recieved,
            release="1.0.0",
            dist="dist",
            threads=None,
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=replay_id,
            errors=None,
        )

        payload = message.serialize()
        payload[2]["data"]["tags"].append(["replayId", replay_id.hex])

        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)
        result["replay_id"] = str(replay_id)
        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )

    def test_errors_replayid_invalid_tag(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            received_timestamp=recieved,
            threads=None,
            release="1.0.0",
            dist="dist",
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=None,
            errors=None,
        )
        invalid_replay_id = "imnotavaliduuid"
        payload = message.serialize()
        payload[2]["data"]["tags"].append(["replayId", invalid_replay_id])

        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)
        result = message.build_result(meta)
        result["tags.key"].insert(4, "replayId")
        result["tags.value"].insert(4, invalid_replay_id)
        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )

    def test_exception_main_thread_true(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            received_timestamp=recieved,
            release="1.0.0",
            dist="dist",
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=None,
            threads={
                "values": [
                    {
                        "id": 1,
                    },
                    {
                        "id": 2,
                        "main": False,
                    },
                    {
                        "id": 3,
                        "main": True,
                    },
                ]
            },
            errors=None,
        )
        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)
        result["exception_main_thread"] = True

        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )

    def test_exception_main_thread_false(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            received_timestamp=recieved,
            release="1.0.0",
            dist="dist",
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=None,
            threads={
                "values": [
                    {
                        "id": 1,
                    },
                    {
                        "id": 2,
                        "main": True,
                    },
                    {
                        "id": 3,
                        "main": False,
                    },
                ]
            },
            errors=None,
        )
        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)
        result["exception_main_thread"] = False

        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )

    def test_trace_sampled(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=True,
            timestamp=timestamp,
            received_timestamp=recieved,
            release="1.0.0",
            dist="dist",
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=None,
            threads=None,
            errors=None,
        )
        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)
        result["trace_sampled"] = True

        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )

        # verify processing trace.sampled=None works as it did before
        message.trace_sampled = None
        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result2 = message.build_result(meta)

        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result2], None
        )

    def test_errors_processed(self) -> None:
        timestamp, recieved = self.__get_timestamps()
        message = ErrorEvent(
            event_id=str(uuid.UUID("dcb9d002cac548c795d1c9adbfc68040")),
            organization_id=1,
            project_id=2,
            group_id=100,
            platform="python",
            message="",
            trace_id=str(uuid.uuid4()),
            trace_sampled=False,
            timestamp=timestamp,
            received_timestamp=recieved,
            release="1.0.0",
            dist="dist",
            environment="prod",
            email="foo@bar.com",
            ip_address="127.0.0.1",
            user_id="myself",
            username="me",
            geo={
                "country_code": "XY",
                "region": "fake_region",
                "city": "fake_city",
                "subdivision": "fake_subdivision",
            },
            replay_id=None,
            threads=None,
            errors=[{"type": "one"}, {"type": "two"}, {"type": "three"}],
            # errors=None,
        )
        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)
        result["num_processing_errors"] = 3

        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )

        # ensure old behavior where data.errors=None won't set 'num_processing_errors'
        message.errors = None
        payload = message.serialize()
        meta = KafkaMessageMetadata(offset=2, partition=2, timestamp=timestamp)

        result = message.build_result(meta)

        assert self.processor.process_message(payload, meta) == InsertBatch(
            [result], None
        )
