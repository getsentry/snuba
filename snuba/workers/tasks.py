import logging
from typing import Any, Dict, Mapping, MutableMapping

from snuba import settings
from snuba.clickhouse.errors import ClickhouseError
from snuba.datasets.dataset import Dataset
from snuba.datasets.storage import StorageNotAvailable
from snuba.query.allocation_policies import AllocationPolicyViolations
from snuba.query.exceptions import QueryPlanException
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request.schema import RequestSchema
from snuba.request.validation import build_request, parse_snql_query
from snuba.state.rate_limit import RateLimitExceeded
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException, QueryTooLongException
from snuba.web.constants import get_http_status_for_clickhouse_error
from snuba.web.query import parse_and_run_query
from snuba.workers import celery_app

logger = logging.getLogger("snuba.tasks")


@celery_app.task(serializer="pickle")
def send_query(
    dataset: Dataset, body: Dict[str, Any], timer: Timer, referrer: str
) -> Mapping[str, Any]:
    schema = RequestSchema.build(HTTPQuerySettings)
    request = build_request(
        body, parse_snql_query, HTTPQuerySettings, schema, dataset, timer, referrer
    )

    try:
        result = parse_and_run_query(dataset, request, timer)
    except QueryException as exception:
        status = 500
        details: Mapping[str, Any]

        cause = exception.__cause__
        if isinstance(cause, (RateLimitExceeded, AllocationPolicyViolations)):
            status = 429
            details = {
                "type": "rate-limited",
                "message": str(cause),
            }
            logger.warning(
                str(cause),
                exc_info=True,
            )
        elif isinstance(cause, ClickhouseError):
            status = get_http_status_for_clickhouse_error(cause)
            details = {
                "type": "clickhouse",
                "message": str(cause),
                "code": cause.code,
            }
        elif isinstance(cause, QueryTooLongException):
            status = 400
            details = {"type": "query-too-long", "message": str(cause)}
        elif isinstance(cause, Exception):
            details = {
                "type": "unknown",
                "message": str(cause),
            }
        else:
            raise  # exception should have been chained

        return {
            "status": status,
            "error": details,
            "timing": timer.for_json(),
            **exception.extra,
        }
    except QueryPlanException as exception:
        if isinstance(exception, StorageNotAvailable):
            status = 400
            details = {
                "type": "storage-not-available",
                "message": str(exception.message),
            }
        else:
            raise  # exception should have been chained
        return {"status": status, "error": details, "timing": timer.for_json()}
    payload: MutableMapping[str, Any] = {**result.result, "timing": timer.for_json()}

    if settings.STATS_IN_RESPONSE or request.query_settings.get_debug():
        payload.update(result.extra)

    payload["status"] = 200
    return payload
