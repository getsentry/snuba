from typing import Any, Callable, Mapping, Tuple, Generic, TypeVar, cast, Type
import os

from google.protobuf.message import Message as ProtobufMessage
from sentry_protos.snuba.v1alpha.endpoint_aggregate_bucket_pb2 import (
    AggregateBucketRequest,
)
from sentry_protos.snuba.v1alpha.endpoint_span_samples_pb2 import SpanSamplesRequest
from sentry_protos.snuba.v1alpha.endpoint_tags_list_pb2 import (
    AttributeValuesRequest,
    TraceItemAttributesRequest,
)
from snuba.utils.registered_class import RegisteredClass, import_submodules_in_directory

from snuba.utils.metrics.timer import Timer
from snuba.web.rpc.v1alpha.trace_item_attribute_list import (
    trace_item_attribute_list_query,
)
from snuba.web.rpc.v1alpha.trace_item_attribute_values import (
    trace_item_attribute_values_query,
)

Version = str
EndpointName = str

ALL_RPCS: Mapping[
    Version,
    Mapping[
        EndpointName,
        Tuple[Callable[[Any, Timer], ProtobufMessage], type[ProtobufMessage]],
    ],
] = {
    "v1alpha": {
        # "AggregateBucketRequest": (timeseries_query, AggregateBucketRequest),
        # "SpanSamplesRequest": (span_samples_query, SpanSamplesRequest),
        "TraceItemAttributesRequest": (
            trace_item_attribute_list_query,
            TraceItemAttributesRequest,
        ),
        "AttributeValuesRequest": (
            trace_item_attribute_values_query,
            AttributeValuesRequest,
        ),
    }
}

Tin = TypeVar("Tin", bound=ProtobufMessage)
Tout = TypeVar("Tout", bound=ProtobufMessage)

class RPCEndpoint(Generic[Tin, Tout], metaclass=RegisteredClass):

    def __init__(self) -> None:
        self._timer = Timer(self.config_key())

    @classmethod
    def request_class(cls) ->Type[Tin]:
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

    @classmethod
    def get_from_name(cls, name: str, version: str) -> Type["RPCEndpoint[Tin, Tout]"]:
        return cast(
            Type["RPCEndpoint[Tin, Tout]"],
            getattr(cls, "_registry").get_class_from_name(f"{name}__{version}"),
        )

    def parse_from_string(self, bytestring: bytes) -> Tin:
        return self.request_class().ParseFromString(bytestring)  # type: ignore


    def execute(self, in_msg: Tin) -> Tout:
        self._before_execute(in_msg)
        out = self._execute(in_msg)
        return self._after_execute(in_msg, out)

    def _before_execute(self, in_msg: Tin) -> None:
        pass

    def _execute(self, in_msg: Tin) -> Tout:
        raise NotImplementedError

    def _after_execute(self, in_msg: Tin, out_msg: Tout) -> Tout:
        return out_msg



_VERSIONS = ["v1alpha"]
_TO_IMPORT= {p: os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        p
    ) for p in _VERSIONS}


for version, module_path in _TO_IMPORT.items():
    import_submodules_in_directory(
        module_path,
        f"snuba.web.rpc.{version}"
    )
