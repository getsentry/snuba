import json
from typing import Any

from snuba.consumers.schemas import get_json_codec, get_schema
from snuba.utils.streams.topics import Topic


def check_example(payload: Any, topic: Topic) -> None:
    assert get_schema(topic) is not None
    codec = get_json_codec(topic)
    assert codec.decode(json.dumps(payload).encode("utf8"), validate=True) == payload


def test_metrics() -> None:
    payload = {
        "use_case_id": "release-health",
        "org_id": 1,
        "project_id": 2,
        "metric_id": 1232341,
        "type": "s",
        "timestamp": 1676388965,
        "tags": {"10": 11, "20": 22, "30": 33},
        "value": [324234, 345345, 456456, 567567],
        "retention_days": 22,
        "mapping_meta": {
            "c": {
                "10": "tag-1",
                "20": "tag-2",
                "11": "value-1",
                "22": "value-2",
                "30": "tag-3",
            },
            "d": {"33": "value-3"},
        },
    }
    check_example(payload, Topic.METRICS)


def test_generic_metrics() -> None:
    payload = {
        "version": 2,
        "mapping_meta": {
            "c": {
                "1": "c:sessions/session@none",
                "3": "environment",
                "5": "session.status",
            },
        },
        "metric_id": 1,
        "org_id": 1,
        "project_id": 3,
        "retention_days": 90,
        "tags": {"3": "production", "5": "init"},
        "timestamp": 1677512412,
        "type": "c",
        "use_case_id": "performance",
        "value": 1.0,
    }

    check_example(payload, Topic.GENERIC_METRICS)


def test_errors_eventstream() -> None:
    payload = [
        2,
        "insert",
        {
            "data": {
                "environment": "production",
                "event_id": "9cdc4c32dff14fbbb012b0aa9e908126",
                "level": "error",
                "logger": "",
                "platform": "javascript",
                "received": 1677512412.437706,
                "release": "123abc",
                "timestamp": 1677512412.223,
                "type": "error",
                "version": "7",
            },
            "datetime": "2023-02-27T15:40:12.223000Z",
            "event_id": "9cdc4c32dff14fbbb012b0aa9e908126",
            "group_id": 124,
            "group_ids": [124],
            "message": "hello world",
            "organization_id": 123,
            "platform": "javascript",
            "primary_hash": "061cf02b26374d108694d6643a7a2f4e",
            "project_id": 6036610,
        },
        {
            "group_states": [
                {
                    "id": "124",
                    "is_new": False,
                    "is_new_group_environment": False,
                    "is_regression": False,
                }
            ],
            "is_new": False,
            "is_new_group_environment": False,
            "is_regression": False,
            "queue": "post_process_errors",
            "skip_consume": False,
        },
    ]

    check_example(payload, Topic.EVENTS)
