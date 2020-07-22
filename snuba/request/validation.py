import sentry_sdk

from snuba.datasets.dataset import Dataset
from snuba.request import Request
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer


def build_request(
    body, schema: RequestSchema, timer: Timer, dataset: Dataset, referrer: str
) -> Request:
    with sentry_sdk.start_span(description="build_request", op="validate") as span:
        request = schema.validate(body, dataset, referrer)
        span.set_data("snuba_query", request.body)

        timer.mark("validate_schema")

    return request
