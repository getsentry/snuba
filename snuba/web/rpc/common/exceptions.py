from typing import Any, Union

from google.protobuf import any_pb2, struct_pb2
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto

from snuba.web import QueryException


class RPCRequestException(Exception):
    status_code: int
    details: dict[str, Any]

    def __init__(self, status_code: int, message: str, details: dict[str, Any] | None = None):
        self.status_code = status_code
        self.details = details or {}
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


class RPCAllocationPolicyException(RPCRequestException):
    def __init__(
        self,
        message: str,
        routing_decision_dict: dict[str, Any],
    ) -> None:
        self.routing_decision_dict = routing_decision_dict
        super().__init__(429, message, details=routing_decision_dict)

    @classmethod
    def from_args(
        cls, message: str, routing_decision_dict: dict[str, Any]
    ) -> "RPCAllocationPolicyException":
        return cls(
            message=message,
            routing_decision_dict=routing_decision_dict,
        )


def convert_rpc_exception_to_proto(exc: Union[RPCRequestException, QueryException]) -> ErrorProto:
    if isinstance(exc, RPCRequestException):
        s = struct_pb2.Struct()
        s.update(exc.details)
        a = any_pb2.Any()
        a.Pack(s)
        return ErrorProto(code=exc.status_code, message=str(exc), details=[a])

    inferred_status = 500
    if exc.exception_type == "RateLimitExceeded":
        inferred_status = 429

    return ErrorProto(code=inferred_status, message=str(exc))
