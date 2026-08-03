from unittest import mock

from snuba.state import _kafka_producer, record_query


def test_get_producer() -> None:
    assert _kafka_producer() is not None


def test_record_query_respects_record_queries_setting() -> None:
    payload = {
        "request": {"id": "00000000-0000-0000-0000-000000000000", "body": {}, "referrer": "t"},
        "dataset": "storage_routing",
        "entity": "eap",
        "start_timestamp": 0,
        "end_timestamp": 0,
        "status": "TIER_1",
        "request_status": "NA",
        "slo": "N/A",
        "projects": [],
        "timing": {"timestamp": 0, "duration_ms": 0, "marks_ms": {}, "tags": {}},
        "snql_anonymized": "",
        "query_list": [],
    }

    with (
        mock.patch("snuba.state.settings.RECORD_QUERIES", False),
        mock.patch("snuba.state._kafka_producer") as producer_factory,
    ):
        record_query(payload)  # type: ignore[arg-type]
        producer_factory.assert_not_called()

    producer = mock.Mock()
    with (
        mock.patch("snuba.state.settings.RECORD_QUERIES", True),
        mock.patch("snuba.state._kafka_producer", return_value=producer),
    ):
        record_query(payload)  # type: ignore[arg-type]
        producer.produce.assert_called_once()
