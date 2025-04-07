import uuid
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.downsampled_storage_tiers import Tier
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.sampling_in_storage_util import (
    DOWNSAMPLING_TIER_MULTIPLIERS,
    _get_target_tier,
    _run_query_on_most_downsampled_tier,
)

DOESNT_MATTER_STR = "doesntmatter"
DOESNT_MATTER_INT = 2
SAMPLING_IN_STORAGE_UTIL_PREFIX = (
    "snuba.web.rpc.v1.resolvers.R_eap_spans.common.sampling_in_storage_util."
)


@pytest.mark.parametrize(
    "most_downsampled_query_bytes_scanned, bytes_scanned_limit, expected_tier",
    [
        (100, 200, Tier.TIER_512),
        (100, 900, Tier.TIER_64),
        (100, 6500, Tier.TIER_8),
        (100, 51300, Tier.TIER_1),
    ],
)
def test_get_target_tier(
    most_downsampled_query_bytes_scanned: int,
    bytes_scanned_limit: int,
    expected_tier: Tier,
) -> None:
    res = MagicMock(QueryResult)
    metrics_mock = MagicMock(spec=MetricsBackend)
    timer = MagicMock(spec=Timer)

    with patch(
        SAMPLING_IN_STORAGE_UTIL_PREFIX + "_get_query_bytes_scanned",
        return_value=most_downsampled_query_bytes_scanned,
    ), patch(
        SAMPLING_IN_STORAGE_UTIL_PREFIX + "_get_bytes_scanned_limit",
        return_value=bytes_scanned_limit,
    ):
        target_tier, estimated_target_tier_query_bytes_scanned = _get_target_tier(
            res, metrics_mock, DOESNT_MATTER_STR, timer
        )
        assert target_tier == expected_tier
        assert (
            estimated_target_tier_query_bytes_scanned
            == most_downsampled_query_bytes_scanned
            * cast(int, DOWNSAMPLING_TIER_MULTIPLIERS.get(target_tier))
        )


def test_sampling_in_storage_query_bytes_scanned_from_most_downsampled_tier_metric_is_sent() -> None:
    timer = Timer(DOESNT_MATTER_STR)
    metrics_mock = MagicMock(spec=MetricsBackend)

    doesnt_matter_request = Request(
        id=uuid.uuid4(),
        original_body={},
        query=LogicalQuery(from_clause=None),
        query_settings=HTTPQuerySettings(),
        attribution_info=AttributionInfo(
            app_id=AppID(key=DOESNT_MATTER_STR),
            tenant_ids={},
            referrer=DOESNT_MATTER_STR,
            team=None,
            feature=None,
            parent_api=None,
        ),
    )
    with patch(SAMPLING_IN_STORAGE_UTIL_PREFIX + "run_query",), patch(
        SAMPLING_IN_STORAGE_UTIL_PREFIX + "_get_query_bytes_scanned",
        return_value=DOESNT_MATTER_INT,
    ):
        _run_query_on_most_downsampled_tier(
            doesnt_matter_request, timer, metrics_mock, DOESNT_MATTER_STR
        )
        metrics_mock.timing.assert_called_once_with(
            "sampling_in_storage_query_bytes_scanned_from_most_downsampled_tier",
            DOESNT_MATTER_INT,
            tags={"referrer": DOESNT_MATTER_STR},
        )
