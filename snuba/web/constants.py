from snuba.clickhouse.errors import ClickhouseError

### HTTP Status Codes ###
INTERNAL_SERVER_ERROR = 500
BAD_REQUEST = 400

### ClickHouse Error Codes ###
# https://github.com/ClickHouse/ClickHouse/blob/1c0b731ea6b86ce3bf7f88bd3ec27df7b218454d/src/Common/ErrorCodes.cpp
ILLEGAL_TYPE_OF_ARGUMENT = 43
TYPE_MISMATCH = 53

# queries that can never return the amount of data requested by the user are not an internal error
MEMORY_LIMIT_EXCEEDED = 241

# Since the query validator doesn't have a typing system, queries containing type errors are run on
# Clickhouse and generate ClickhouseErrors. Return a 400 status code for such requests because the problem
# lies with the query, not Snuba.
CLICKHOUSE_TYPING_ERROR_CODES = {
    ILLEGAL_TYPE_OF_ARGUMENT,
    TYPE_MISMATCH,
}

ACCEPTABLE_CLICKHOUSE_ERROR_CODES = {
    *CLICKHOUSE_TYPING_ERROR_CODES,
    MEMORY_LIMIT_EXCEEDED,
}


def get_http_status_for_clickhouse_error(cause: ClickhouseError) -> int:
    """
    ClickHouse Errors are generally internal errors, but sometimes they
    are caused by bad Snuba requests.
    """
    if cause.code in ACCEPTABLE_CLICKHOUSE_ERROR_CODES:
        return BAD_REQUEST
    return INTERNAL_SERVER_ERROR
