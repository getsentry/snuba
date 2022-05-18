from http.client import BAD_REQUEST, INTERNAL_SERVER_ERROR

from clickhouse_driver.errors import ErrorCodes

from snuba.clickhouse.errors import ClickhouseError

# Since the query validator doesn't have a typing system, queries containing type errors are run on
# Clickhouse and generate ClickhouseErrors. Return a 400 status code for such requests because the problem
# lies with the query, not Snuba.
CLICKHOUSE_TYPING_ERROR_CODES = {
    ErrorCodes.ILLEGAL_TYPE_OF_ARGUMENT,
    ErrorCodes.TYPE_MISMATCH,
}

ACCEPTABLE_CLICKHOUSE_ERROR_CODES = {
    *CLICKHOUSE_TYPING_ERROR_CODES,
    # queries that can never return the amount of data requested by the user are not an internal error
    ErrorCodes.MEMORY_LIMIT_EXCEEDED,
}


def get_http_status_for_clickhouse_error(cause: ClickhouseError) -> int:
    """
    ClickHouse Errors are generally internal errors, but sometimes they
    are caused by bad Snuba requests.
    """
    if cause.code in ACCEPTABLE_CLICKHOUSE_ERROR_CODES:
        return BAD_REQUEST
    return INTERNAL_SERVER_ERROR
