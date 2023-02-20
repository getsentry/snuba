import json

from snuba.consumers.schemas import get_json_codec
from snuba.utils.streams.topics import Topic


def test_metrics() -> None:
    codec = get_json_codec(Topic.GENERIC_METRICS)
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
    assert codec.decode(json.dumps(payload).encode("utf8"), validate=True) == payload
