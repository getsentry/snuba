from datetime import datetime, timedelta
from uuid import UUID

import pytz

from snuba import settings
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.errors_processor import ErrorsProcessor
from snuba.datasets.events_processor_base import InsertEvent
from snuba.processor import InsertBatch, json_encode_insert_batch
from snuba.settings import PAYLOAD_DATETIME_FORMAT


def test_error_processor() -> None:
    received_timestamp = datetime.now() - timedelta(minutes=1)
    error_timestamp = received_timestamp - timedelta(minutes=1)

    error = (
        2,
        "insert",
        InsertEvent(
            {
                "organization_id": 1,
                "retention_days": settings.DEFAULT_RETENTION_DAYS,
                "event_id": "dcb9d002cac548c795d1c9adbfc68040",
                "group_id": 100,
                "project_id": 300688,
                "platform": "python",
                "message": "",
                "datetime": error_timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                "primary_hash": "04233d08ac90cf6fc015b1be5932e7e2",
                "data": {
                    "event_id": "dcb9d002cac548c795d1c9adbfc68040",
                    "project_id": 300688,
                    "release": None,
                    "dist": None,
                    "platform": "python",
                    "message": "",
                    "datetime": error_timestamp.strftime(PAYLOAD_DATETIME_FORMAT),
                    "tags": [
                        ["handled", "no"],
                        ["level", "error"],
                        ["mechanism", "excepthook"],
                        ["runtime", "CPython 3.7.6"],
                        ["runtime.name", "CPython"],
                        ["server_name", "snuba"],
                        ["environment", "dev"],
                        ["sentry:user", "this_is_me"],
                        ["sentry:release", "4d23338017cdee67daf25f2c"],
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
                                "timestamp": error_timestamp.timestamp(),
                                "data": {
                                    "asctime": error_timestamp.strftime(
                                        PAYLOAD_DATETIME_FORMAT
                                    )
                                },
                                "message": "New partitions assigned: {}",
                                "type": "default",
                            },
                            {
                                "category": "snuba.utils.streams.batching",
                                "level": "info",
                                "timestamp": error_timestamp.timestamp(),
                                "data": {
                                    "asctime": error_timestamp.strftime(
                                        PAYLOAD_DATETIME_FORMAT
                                    )
                                },
                                "message": "Flushing ",
                                "type": "default",
                            },
                            {
                                "category": "httplib",
                                "timestamp": error_timestamp.timestamp(),
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
                        }
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
                                "module": "snuba.clickhouse.http",
                                "value": "[171] DB::Exception: Block structure mismatch",
                                "mechanism": {"type": "excepthook", "handled": False},
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
                    "received": received_timestamp.timestamp(),
                    "sdk": {
                        "version": "0.0.0.0.1",
                        "name": "sentry.python",
                        "packages": [
                            {"version": "0.0.0.0.1", "name": "pypi:sentry-sdk"}
                        ],
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
                    "timestamp": error_timestamp.timestamp(),
                    "title": "ClickHouseError: [171] DB::Exception: Block structure mismatch",
                    "type": "error",
                    "version": "7",
                },
            }
        ),
        None,
    )

    expected_result = {
        "deleted": 0,
        "project_id": 300688,
        "event_id": str(UUID("dcb9d002cac548c795d1c9adbfc68040")),
        "retention_days": 90,
        "group_id": 100,
        "timestamp": error_timestamp,
        "platform": "python",
        "received": received_timestamp.astimezone(pytz.utc).replace(
            tzinfo=None, microsecond=0
        ),
        "version": "7",
        "location": "snuba/clickhouse/http.py",
        "modules.name": ["cffi", "ipython-genutils", "isodate"],
        "modules.version": ["1.13.2", "0.2.0", "0.6.0"],
        "user_name": "me",
        "user_id": "still_me",
        "user_email": "me@myself.org",
        "ip_address_v4": "127.0.0.1",
        "http_method": "POST",
        "http_referer": "tagstore.something",
        "message": "",
        "primary_hash": str(UUID("04233d08ac90cf6fc015b1be5932e7e2")),
        "hierarchical_hashes": [
            str(UUID("04233d08ac90cf6fc015b1be5932e7e3")),
            str(UUID("04233d08ac90cf6fc015b1be5932e7e4")),
        ],
        "culprit": "snuba.clickhouse.http in write",
        "type": "error",
        "title": "ClickHouseError: [171] DB::Exception: Block structure mismatch",
        "sdk_name": "sentry.python",
        "sdk_version": "0.0.0.0.1",
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
        "environment": "dev",
        "release": "4d23338017cdee67daf25f2c",
        "dist": None,
        "user": "this_is_me",
        "transaction_name": "",
        "level": "error",
        "contexts.key": [
            "runtime.version",
            "runtime.name",
            "runtime.build",
            "geo.country_code",
            "geo.region",
            "geo.city",
        ],
        "contexts.value": [
            "3.7.6",
            "CPython",
            "3.7.6",
            "XY",
            "fake_region",
            "fake_city",
        ],
        "tags.key": [
            "environment",
            "handled",
            "level",
            "mechanism",
            "runtime",
            "runtime.name",
            "sentry:release",
            "sentry:user",
            "server_name",
        ],
        "tags.value": [
            "dev",
            "no",
            "error",
            "excepthook",
            "CPython 3.7.6",
            "CPython",
            "4d23338017cdee67daf25f2c",
            "this_is_me",
            "snuba",
        ],
        "exception_stacks.type": ["ClickHouseError"],
        "exception_stacks.value": ["[171] DB::Exception: Block structure mismatch"],
        "exception_stacks.mechanism_type": ["excepthook"],
        "exception_stacks.mechanism_handled": [False],
        "exception_frames.abs_path": ["/usr/local/bin/snuba"],
        "exception_frames.filename": ["snuba"],
        "exception_frames.package": [None],
        "exception_frames.module": ["__main__"],
        "exception_frames.function": ["<module>"],
        "exception_frames.in_app": [False],
        "exception_frames.colno": [None],
        "exception_frames.lineno": [11],
        "exception_frames.stack_level": [0],
        "offset": 2,
        "partition": 1,
        "message_timestamp": datetime(1970, 1, 1),
    }

    meta = KafkaMessageMetadata(offset=2, partition=1, timestamp=datetime(1970, 1, 1))
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

    assert processor.process_message(error, meta) == json_encode_insert_batch(
        InsertBatch([expected_result], None)
    )
