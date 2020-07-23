import sentry_sdk

from snuba.datasets.dataset import Dataset
from snuba.query.parser.exceptions import InvalidQueryException
from snuba.querylog import record_error_building_request, record_invalid_request
from snuba.request import Request
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer


def build_request(
    body, schema: RequestSchema, timer: Timer, dataset: Dataset, referrer: str
) -> Request:
    with sentry_sdk.start_span(description="build_request", op="validate") as span:
        try:
            request = schema.validate(body, dataset, referrer)
            span.set_data("snuba_query", request.body)

            timer.mark("validate_schema")
            return request

        except Exception as exception:
            if isinstance(
                exception, (InvalidJsonRequestException, InvalidQueryException)
            ):
                record_invalid_request(timer, referrer)
            else:
                record_error_building_request(timer, referrer)
            raise exception
