from unittest.mock import Mock, patch

from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.storage_key import StorageKey
from snuba.lw_deletions.types import AttributeConditions
from snuba.utils.delete_log import eap_group_ids, log_delete_event, log_eap_delete_event
from tests.helpers import get_delete_log_events


@patch("snuba.utils.delete_log.sentry_logger")
def test_log_delete_event_attributes(sentry_logger: Mock) -> None:
    log_delete_event(
        "received",
        "errors",
        project_id=1,
        group_ids=list(range(1, 13)),
        transaction_id="t1",
        written_rows=0,
        partition_week=None,
    )

    assert get_delete_log_events(sentry_logger) == [
        (
            "info",
            "snuba.delete.received",
            {
                "target": "errors",
                "project_id": 1,
                "group_ids_count": 12,
                "group_ids": "1,2,3,4,5,6,7,8,9,10",
                "transaction_id": "t1",
                # 0 is kept, only None is dropped.
                "written_rows": 0,
            },
        )
    ]


@patch("snuba.utils.delete_log.sentry_logger")
def test_failed_event_is_error_level_with_exception(sentry_logger: Mock) -> None:
    log_delete_event(
        "redis_failed",
        "redis",
        project_id=1,
        reason="redis_write_failed",
        error=ValueError("x" * 500),
    )

    assert get_delete_log_events(sentry_logger) == [
        (
            "error",
            "snuba.delete.redis_failed",
            {
                "target": "redis",
                "project_id": 1,
                "group_ids_count": 0,
                "reason": "redis_write_failed",
                "error": "ValueError",
                "error_message": "x" * 200,
            },
        )
    ]


@patch("snuba.utils.delete_log.sentry_logger")
def test_log_delete_event_never_raises(sentry_logger: Mock) -> None:
    sentry_logger.info.side_effect = RuntimeError("logger is broken")
    log_delete_event("received", "errors", project_id=1)


@patch("snuba.utils.delete_log.sentry_logger")
def test_log_eap_delete_event_one_per_project(sentry_logger: Mock) -> None:
    attribute_conditions = AttributeConditions(
        item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE,
        attributes={
            "group_id": (AttributeKey(type=AttributeKey.Type.TYPE_INT, name="group_id"), [7, 8])
        },
    )
    log_eap_delete_event(
        StorageKey("eap_items_2"),
        "received",
        {"organization_id": [5], "project_id": [1, 2]},
        eap_group_ids(attribute_conditions),
    )

    expected = {"target": "eap", "organization_id": 5, "group_ids_count": 2, "group_ids": "7,8"}
    assert get_delete_log_events(sentry_logger) == [
        ("info", "snuba.delete.received", {**expected, "project_id": 1}),
        ("info", "snuba.delete.received", {**expected, "project_id": 2}),
    ]


@patch("snuba.utils.delete_log.sentry_logger")
def test_log_eap_delete_event_ignores_other_storages(sentry_logger: Mock) -> None:
    log_eap_delete_event(StorageKey("search_issues"), "received", {"project_id": [1]}, [1])
    assert sentry_logger.method_calls == []


def test_eap_group_ids_without_group_id_condition() -> None:
    assert eap_group_ids(None) == []
    assert (
        eap_group_ids(
            AttributeConditions(item_type=TraceItemType.TRACE_ITEM_TYPE_OCCURRENCE, attributes={})
        )
        == []
    )


class _UnprintableError(Exception):
    def __str__(self) -> str:
        raise ValueError("no str")


@patch("snuba.utils.delete_log.sentry_logger")
def test_failed_event_sent_when_error_has_no_str(sentry_logger: Mock) -> None:
    log_delete_event("eap_failed", "eap", project_id=1, error=_UnprintableError())

    assert get_delete_log_events(sentry_logger) == [
        (
            "error",
            "snuba.delete.eap_failed",
            {"target": "eap", "project_id": 1, "group_ids_count": 0, "error": "_UnprintableError"},
        )
    ]
