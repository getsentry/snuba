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

NON_RETRYABLE_CLICKHOUSE_ERROR_CODES = {
    ErrorCodes.MEMORY_LIMIT_EXCEEDED,
    ErrorCodes.TOO_SLOW,
}


def get_http_status_for_clickhouse_error(cause: ClickhouseError) -> int:
    """
    ClickHouse Errors are generally internal errors, but sometimes they
    are caused by bad Snuba requests.
    """
    if cause.code in ACCEPTABLE_CLICKHOUSE_ERROR_CODES:
        return BAD_REQUEST
    return INTERNAL_SERVER_ERROR


# error codes that are unlikely to be caused by a specific query, and rather
# indicate a systematic/global problem with the dataset affecting all queries.
CLICKHOUSE_SYSTEMATIC_FAILURES = [
    ErrorCodes.RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
    ErrorCodes.CANNOT_READ_FROM_SOCKET,
    ErrorCodes.CANNOT_WRITE_TO_SOCKET,
    ErrorCodes.UNKNOWN_PACKET_FROM_CLIENT,
    ErrorCodes.UNKNOWN_PACKET_FROM_SERVER,
    ErrorCodes.UNEXPECTED_PACKET_FROM_CLIENT,
    ErrorCodes.UNEXPECTED_PACKET_FROM_SERVER,
    ErrorCodes.TIMEOUT_EXCEEDED,
    ErrorCodes.TOO_SLOW,
    ErrorCodes.CANNOT_ALLOCATE_MEMORY,
    ErrorCodes.UNKNOWN_USER,
    ErrorCodes.WRONG_PASSWORD,
    ErrorCodes.REQUIRED_PASSWORD,
    ErrorCodes.IP_ADDRESS_NOT_ALLOWED,
    ErrorCodes.DNS_ERROR,
    ErrorCodes.TOO_MANY_SIMULTANEOUS_QUERIES,
    ErrorCodes.NO_FREE_CONNECTION,
    ErrorCodes.CANNOT_FSYNC,
    ErrorCodes.SOCKET_TIMEOUT,
    ErrorCodes.NETWORK_ERROR,
    ErrorCodes.CLIENT_HAS_CONNECTED_TO_WRONG_PORT,
    ErrorCodes.NO_ZOOKEEPER,
    ErrorCodes.ABORTED,
    ErrorCodes.MEMORY_LIMIT_EXCEEDED,
    ErrorCodes.TABLE_IS_READ_ONLY,
    ErrorCodes.NOT_ENOUGH_SPACE,
    ErrorCodes.UNEXPECTED_ZOOKEEPER_ERROR,
    ErrorCodes.CORRUPTED_DATA,
    ErrorCodes.INCORRECT_MARK,
    ErrorCodes.INVALID_PARTITION_VALUE,
    ErrorCodes.NOT_ENOUGH_BLOCK_NUMBERS,
    ErrorCodes.NO_SUCH_REPLICA,
    ErrorCodes.TOO_MANY_PARTS,
    ErrorCodes.REPLICA_IS_ALREADY_EXIST,
    ErrorCodes.NO_ACTIVE_REPLICAS,
    ErrorCodes.TOO_MANY_RETRIES_TO_FETCH_PARTS,
    ErrorCodes.PARTITION_ALREADY_EXISTS,
    ErrorCodes.PARTITION_DOESNT_EXIST,
    ErrorCodes.UNKNOWN_BLOCK_INFO_FIELD,
    ErrorCodes.NO_AVAILABLE_REPLICA,
    ErrorCodes.MISMATCH_REPLICAS_DATA_SOURCES,
    ErrorCodes.STORAGE_DOESNT_SUPPORT_PARALLEL_REPLICAS,
    ErrorCodes.CPUID_ERROR,
    ErrorCodes.CANNOT_COMPRESS,
    ErrorCodes.CANNOT_DECOMPRESS,
    ErrorCodes.AIO_SUBMIT_ERROR,
    ErrorCodes.AIO_COMPLETION_ERROR,
    ErrorCodes.AIO_READ_ERROR,
    ErrorCodes.AIO_WRITE_ERROR,
    ErrorCodes.LEADERSHIP_LOST,
    ErrorCodes.ALL_CONNECTION_TRIES_FAILED,
    ErrorCodes.TOO_LESS_LIVE_REPLICAS,
    ErrorCodes.UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE,
    ErrorCodes.REPLICA_IS_NOT_IN_QUORUM,
    ErrorCodes.LIMIT_EXCEEDED,
    ErrorCodes.DATABASE_ACCESS_DENIED,
    ErrorCodes.LEADERSHIP_CHANGED,
    ErrorCodes.NO_REMOTE_SHARD_FOUND,
    ErrorCodes.SHARD_HAS_NO_CONNECTIONS,
    ErrorCodes.CANNOT_PIPE,
    ErrorCodes.CANNOT_FORK,
    ErrorCodes.CANNOT_DLSYM,
    ErrorCodes.CANNOT_CREATE_CHILD_PROCESS,
    ErrorCodes.CHILD_WAS_NOT_EXITED_NORMALLY,
    ErrorCodes.CANNOT_SELECT,
    ErrorCodes.CANNOT_WAITPID,
    ErrorCodes.UNEXPECTED_NODE_IN_ZOOKEEPER,
    ErrorCodes.UNFINISHED,
    ErrorCodes.SEEK_POSITION_OUT_OF_BOUND,
    ErrorCodes.CURRENT_WRITE_BUFFER_IS_EXHAUSTED,
    ErrorCodes.CANNOT_CREATE_IO_BUFFER,
    ErrorCodes.RECEIVED_ERROR_TOO_MANY_REQUESTS,
    ErrorCodes.ALL_REPLICAS_ARE_STALE,
    ErrorCodes.INCONSISTENT_CLUSTER_DEFINITION,
    ErrorCodes.SESSION_NOT_FOUND,
    ErrorCodes.SESSION_IS_LOCKED,
    ErrorCodes.INVALID_SESSION_TIMEOUT,
    ErrorCodes.CANNOT_KILL,
    ErrorCodes.QUERY_WAS_CANCELLED,
    ErrorCodes.KEEPER_EXCEPTION,
    ErrorCodes.POCO_EXCEPTION,
    ErrorCodes.STD_EXCEPTION,
]
