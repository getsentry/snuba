from typing import Union

from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto

from snuba.web import QueryException


class RPCRequestException(Exception):
    status_code: int

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


class BadSnubaRPCRequestException(RPCRequestException):
    def __init__(self, message: str):
        super().__init__(400, message)


class HighAccuracyQueryTimeoutException(RPCRequestException):
    def __init__(self, message: str):
        super().__init__(408, message)


class QueryTimeoutException(RPCRequestException):
    def __init__(self, message: str):
        super().__init__(408, message)


def convert_rpc_exception_to_proto(
    exc: Union[RPCRequestException, QueryException]
) -> ErrorProto:
    if isinstance(exc, RPCRequestException):
        return ErrorProto(code=exc.status_code, message=str(exc))

    inferred_status = 500
    if exc.exception_type == "RateLimitExceeded":
        inferred_status = 429

    return ErrorProto(code=inferred_status, message=str(exc))
