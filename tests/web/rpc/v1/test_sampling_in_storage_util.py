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
    "most_downsampled_query_duration_ms, time_budget, expected_tier",
    [
        (100, 200, Tier.TIER_512),
        (100, 900, Tier.TIER_64),
        (100, 6500, Tier.TIER_8),
        (100, 51300, Tier.TIER_1),
    ],
)
def test_get_target_tier(
    most_downsampled_query_duration_ms: int, time_budget: int, expected_tier: Tier
) -> None:
    timer = Timer(DOESNT_MATTER_STR)
    metrics_mock = MagicMock(spec=MetricsBackend)

    with patch(
        SAMPLING_IN_STORAGE_UTIL_PREFIX + "_get_query_duration",
        return_value=most_downsampled_query_duration_ms,
    ), patch(
        SAMPLING_IN_STORAGE_UTIL_PREFIX + "_get_time_budget", return_value=time_budget
    ):
        assert _get_target_tier(timer, metrics_mock, DOESNT_MATTER_STR) == expected_tier

        for tier in sorted(Tier, reverse=True)[:-1]:
            expected_estimated_duration = most_downsampled_query_duration_ms * cast(
                int, DOWNSAMPLING_TIER_MULTIPLIERS.get(tier)
            )
            metrics_mock.timing.assert_any_call(
                "sampling_in_storage_estimated_query_duration",
                expected_estimated_duration,
                tags={"referrer": DOESNT_MATTER_STR, "tier": str(tier)},
            )

        metrics_mock.timing.assert_any_call(
            "sampling_in_storage_routed_tier",
            expected_tier,
            tags={"referrer": DOESNT_MATTER_STR, "tier": str(expected_tier)},
        )


def test_sampling_in_storage_query_duration_from_most_downsampled_tier_metric_is_sent() -> None:
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
        SAMPLING_IN_STORAGE_UTIL_PREFIX + "_get_query_duration",
        return_value=DOESNT_MATTER_INT,
    ):
        _run_query_on_most_downsampled_tier(
            doesnt_matter_request, timer, metrics_mock, DOESNT_MATTER_STR
        )
        metrics_mock.timing.assert_called_once_with(
            "sampling_in_storage_query_duration_from_most_downsampled_tier",
            DOESNT_MATTER_INT,
            tags={"referrer": DOESNT_MATTER_STR},
        )
