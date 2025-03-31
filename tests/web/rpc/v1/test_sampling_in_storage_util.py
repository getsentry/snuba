import uuid
from unittest.mock import MagicMock, patch

from snuba.attribution import AppID
from snuba.attribution.attribution_info import AttributionInfo
from snuba.query.logical import Query as LogicalQuery
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.sampling_in_storage_util import (
    _run_query_on_most_downsampled_tier,
)

DOESNT_MATTER_STR = "doesntmatter"
DOESNT_MATTER_INT = 2


def test_sampling_in_storage_most_downsampled_tier_query_duration_metric_is_sent() -> None:
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
    with patch(
        "snuba.web.rpc.v1.resolvers.R_eap_spans.common.sampling_in_storage_util.run_query",
    ), patch(
        "snuba.web.rpc.v1.resolvers.R_eap_spans.common.sampling_in_storage_util._get_query_duration",
        return_value=DOESNT_MATTER_INT,
    ):
        _run_query_on_most_downsampled_tier(
            doesnt_matter_request, timer, metrics_mock, DOESNT_MATTER_STR
        )
        metrics_mock.timing.assert_called_once_with(
            "sampling_in_storage_most_downsampled_tier_query_duration",
            DOESNT_MATTER_INT,
            tags={"referrer": DOESNT_MATTER_STR},
        )
