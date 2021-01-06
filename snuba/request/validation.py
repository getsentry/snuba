import sentry_sdk
from typing import Any, MutableMapping

from snuba.datasets.dataset import Dataset
from snuba.query.exceptions import InvalidQueryException
from snuba.querylog import record_error_building_request, record_invalid_request
from snuba.request import Request
from snuba.request.exceptions import InvalidJsonRequestException
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer


def build_request(
    body: MutableMapping[str, Any],
    schema: RequestSchema,
    timer: Timer,
    dataset: Dataset,
    referrer: str,
) -> Request:
    with sentry_sdk.start_span(description="build_request", op="validate") as span:
        try:
            request = schema.validate(body, dataset, referrer)
        except (InvalidJsonRequestException, InvalidQueryException) as exception:
            record_invalid_request(timer, referrer)
            raise exception
        except Exception as exception:
            record_error_building_request(timer, referrer)
            raise exception

        span.set_data("snuba_query", request.body)

        timer.mark("validate_schema")
        return request
