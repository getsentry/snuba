from datetime import datetime, timedelta

import pytest
from google.protobuf import json_format, struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import RequestMeta

from snuba import settings
from snuba.web.rpc.common.common import next_monday, prev_monday, use_sampling_factor
from snuba.web.rpc.common.exceptions import (
    RPCAllocationPolicyException,
    convert_rpc_exception_to_proto,
)
from tests.conftest import SnubaSetConfig


class TestCommon:
    def test_timestamp_rounding(self) -> None:
        start = datetime(2025, 3, 10)
        end = datetime(2025, 3, 17)

        tmp = start.replace()
        for _ in range(7):
            assert prev_monday(tmp) == start
            assert next_monday(tmp) == end
            tmp += timedelta(days=1)

    @pytest.mark.redis_db
    def test_use_sampling_factor(self, snuba_set_config: SnubaSetConfig) -> None:
        assert use_sampling_factor(
            RequestMeta(
                start_timestamp=Timestamp(seconds=settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS)
            )
        )
        assert not use_sampling_factor(
            RequestMeta(
                start_timestamp=Timestamp(
                    seconds=settings.USE_SAMPLING_FACTOR_TIMESTAMP_SECONDS - 1
                )
            )
        )
        snuba_set_config("use_sampling_factor_timestamp_seconds", 10)
        assert use_sampling_factor(RequestMeta(start_timestamp=Timestamp(seconds=10)))
        assert not use_sampling_factor(RequestMeta(start_timestamp=Timestamp(seconds=9)))


@pytest.mark.redis_db
@pytest.mark.clickhouse_db
def test_convert_rpc_exception_to_proto_packs_details() -> None:
    routing_decision_log_dict = {
        "can_run": False,
        "is_throttled": True,
        "strategy": "OutcomesBasedRoutingStrategy",
        "source_request_id": "21df09b9-41a8-4529-9fc7-d75c851adbfe",
        "extra_info": {
            "sampling_in_storage_estimation_time_overhead": {
                "type": "timing",
                "value": 10,
                "tags": None,
            }
        },
        "clickhouse_settings": {"max_threads": 0},
        "result_info": {},
        "routed_tier": "TIER_1",
        "allocation_policies_recommendations": {
            "RejectionPolicy": {
                "can_run": False,
                "max_threads": 0,
                "explanation": {
                    "reason": "policy rejects all queries",
                    "storage_key": "doesntmatter",
                },
                "is_throttled": True,
                "throttle_threshold": 1000000000000,
                "rejection_threshold": 1000000000000,
                "quota_used": 0,
                "quota_unit": "no_units",
                "suggestion": "no_suggestion",
                "max_bytes_to_read": 0,
            }
        },
    }

    exc = RPCAllocationPolicyException(
        "Query cannot be run due to routing strategy deciding it cannot run, most likely due to allocation policies",
        routing_decision_log_dict,
    )

    proto = convert_rpc_exception_to_proto(exc)
    assert isinstance(proto, ErrorProto)
    assert proto.code == 429
    assert (
        proto.message
        == "Query cannot be run due to routing strategy deciding it cannot run, most likely due to allocation policies"
    )

    s = struct_pb2.Struct()
    assert proto.details[0].Unpack(s)
    unpacked = json_format.MessageToDict(s)
    assert unpacked == routing_decision_log_dict
