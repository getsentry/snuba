import os
from typing import Generic, List, Tuple, Type, TypeVar, cast

from google.protobuf.message import DecodeError
from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto

from snuba import environment
from snuba.utils.metrics.backends.abstract import MetricsBackend
from snuba.utils.metrics.timer import Timer
from snuba.utils.metrics.wrapper import MetricsWrapper
from snuba.utils.registered_class import (
    InvalidConfigKeyError,
    RegisteredClass,
    import_submodules_in_directory,
)
from snuba.web import QueryException
from snuba.web.rpc.common.exceptions import (
    RPCRequestException,
    convert_rpc_exception_to_proto,
)

Tin = TypeVar("Tin", bound=ProtobufMessage)
Tout = TypeVar("Tout", bound=ProtobufMessage)


class RPCEndpoint(Generic[Tin, Tout], metaclass=RegisteredClass):
    def __init__(self, metrics_backend: MetricsBackend | None = None) -> None:
        self._timer = Timer("endpoint_timing")
        self._metrics_backend = metrics_backend or environment.metrics

    @classmethod
    def request_class(cls) -> Type[Tin]:
        raise NotImplementedError

    @classmethod
    def response_class(cls) -> Type[Tout]:
        raise NotImplementedError

    @classmethod
    def version(cls) -> str:
        raise NotImplementedError

    @classmethod
    def config_key(cls) -> str:
        return f"{cls.__name__}__{cls.version()}"

    @property
    def metrics(self) -> MetricsWrapper:
        return MetricsWrapper(
            self._metrics_backend,
            "rpc",
            tags={"endpoint_name": self.__class__.__name__, "version": self.version()},
        )

    @classmethod
    def get_from_name(cls, name: str, version: str) -> Type["RPCEndpoint[Tin, Tout]"]:
        return cast(
            Type["RPCEndpoint[Tin, Tout]"],
            getattr(cls, "_registry").get_class_from_name(f"{name}__{version}"),
        )

    def parse_from_string(self, bytestring: bytes) -> Tin:
        res = self.request_class()()
        res.ParseFromString(bytestring)
        return res

    def execute(self, in_msg: Tin) -> Tout:
        self.__before_execute(in_msg)
        error = None
        try:
            out = self._execute(in_msg)
        except Exception as e:
            out = self.response_class()()
            error = e
        return self.__after_execute(in_msg, out, error)

    def __before_execute(self, in_msg: Tin) -> None:
        self._timer.mark("rpc_start")
        self._before_execute(in_msg)

    def _before_execute(self, in_msg: Tin) -> None:
        """Override this for any pre-processing/logging before the _execute method"""
        pass

    def _execute(self, in_msg: Tin) -> Tout:
        raise NotImplementedError

    def __after_execute(
        self, in_msg: Tin, out_msg: Tout, error: Exception | None
    ) -> Tout:
        res = self._after_execute(in_msg, out_msg, error)
        self._timer.mark("rpc_end")
        self._timer.send_metrics_to(self.metrics)
        if error is not None:
            self.metrics.increment("request_error")
            raise error
        else:
            self.metrics.increment("request_success")
        return res

    def _after_execute(
        self, in_msg: Tin, out_msg: Tout, error: Exception | None
    ) -> Tout:
        """Override this for any post-processing/logging after the _execute method"""
        return out_msg


def list_all_endpoint_names() -> List[Tuple[str, str]]:
    return [
        (name.split("__")[0], name.split("__")[1])
        for name in RPCEndpoint.all_names()
        if name.count("__") == 1
    ]


_VERSIONS = ["v1alpha", "v1"]
_TO_IMPORT = {
    p: os.path.join(os.path.dirname(os.path.realpath(__file__)), p) for p in _VERSIONS
}


for v, module_path in _TO_IMPORT.items():
    import_submodules_in_directory(module_path, f"snuba.web.rpc.{v}")


def run_rpc_handler(
    name: str, version: str, data: bytes
) -> ProtobufMessage | ErrorProto:
    try:
        endpoint = RPCEndpoint.get_from_name(name, version)()  # type: ignore
    except (AttributeError, InvalidConfigKeyError) as e:
        return convert_rpc_exception_to_proto(
            RPCRequestException(
                status_code=404,
                message=f"endpoint {name} with version {version} does not exist (did you use the correct version and capitalization?) {e}",
            )
        )

    try:
        deserialized_protobuf = endpoint.parse_from_string(data)
    except DecodeError as e:
        return convert_rpc_exception_to_proto(
            RPCRequestException(
                status_code=400,
                message=f"protobuf gave a decode error {e} (are all fields set and the correct types?)",
            )
        )

    try:
        return cast(ProtobufMessage, endpoint.execute(deserialized_protobuf))
    except (RPCRequestException, QueryException) as e:
        return convert_rpc_exception_to_proto(e)
    except Exception as e:
        return convert_rpc_exception_to_proto(
            RPCRequestException(
                status_code=500,
                message=f"internal error occurred while executing this RPC call: {e}",
            )
        )
