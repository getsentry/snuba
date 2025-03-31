from unittest.mock import MagicMock, patch

from snuba.reader import Result
from snuba.request import Request
from snuba.utils.metrics import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryExtraData, QueryResult
from snuba.web.rpc.v1.resolvers.R_eap_spans.common.sampling_in_storage_util import (
    _run_query_on_most_downsampled_tier,
)


def test_sampling_in_storage_estimation_duration_metric_is_sent() -> None:
    metrics_mock = MagicMock(spec=MetricsBackend)
    timer = Timer("doesntmatter")
    doesntmatterresult = QueryResult(
        result=MagicMock(spec=Result),
        extra=MagicMock(spec=QueryExtraData),
    )

    with patch(
        "snuba.web.rpc.v1.resolvers.R_eap_spans.common.sampling_in_storage_util.run_query",
        return_value=doesntmatterresult,
    ):

        _run_query_on_most_downsampled_tier(
            request_to_most_downsampled_tier=MagicMock(spec=Request),
            timer=timer,
            metrics_backend=metrics_mock,
            referrer="doesntmatter",
        )

        duration = timer.get_duration_between_marks(
            "right_before_execute",
            "execute",
        )
        metrics_mock.timing.assert_called_once_with(
            "sampling_in_storage_estimation_duration", duration
        )
