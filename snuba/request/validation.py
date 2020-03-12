from typing import Optional

import jsonschema
import sentry_sdk
from werkzeug.exceptions import BadRequest

from snuba.datasets.dataset import Dataset
from snuba.request import Request
from snuba.request.schema import RequestSchema
from snuba.utils.metrics.timer import Timer


def validate_request_content(
    body, schema: RequestSchema, timer: Optional[Timer], dataset: Dataset, referrer: str
) -> Request:
    with sentry_sdk.start_span(
        description="validate_request_content", op="validate"
    ) as span:
        try:
            request = schema.validate(body, dataset, referrer)
            span.set_data("snuba_query", request.body)
        except jsonschema.ValidationError as error:
            raise BadRequest(str(error)) from error

        if timer is not None:
            timer.mark("validate_schema")

    return request
