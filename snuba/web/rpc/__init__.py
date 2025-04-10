import os
from bisect import bisect_left
from datetime import timedelta
from typing import Generic, List, Tuple, Type, TypeVar, cast, final

import sentry_sdk
from google.protobuf.message import DecodeError
from google.protobuf.message import Message as ProtobufMessage
from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.downsampled_storage_pb2 import DownsampledStorageConfig
from sentry_protos.snuba.v1.error_pb2 import Error as ErrorProto
from sentry_protos.snuba.v1.request_common_pb2 import TraceItemType

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
    BadSnubaRPCRequestException,
    RPCRequestException,
    convert_rpc_exception_to_proto,
)

Tin = TypeVar("Tin", bound=ProtobufMessage)
Tout = TypeVar("Tout", bound=ProtobufMessage)

MAXIMUM_TIME_RANGE_IN_DAYS = 30
_TIME_PERIOD_HOURS_BUCKETS = [
    1,
    24,
    7 * 24,
    14 * 24,
    30 * 24,
    90 * 24,
]
_BUCKETS_COUNT = len(_TIME_PERIOD_HOURS_BUCKETS)


class TraceItemDataResolver(Generic[Tin, Tout], metaclass=RegisteredClass):
    def __init__(
        self, timer: Timer | None = None, metrics_backend: MetricsBackend | None = None
    ) -> None:
        self._timer = timer or Timer("endpoint_timing")
        self._metrics_backend = metrics_backend or environment.metrics

    @classmethod
    def config_key(cls) -> str:
        return f"{cls.endpoint_name()}__{cls.trace_item_type()}"

    @classmethod
    def endpoint_name(cls) -> str:
        if cls.__name__ == "TraceItemDataResolver":
            return cls.__name__
        raise NotImplementedError

    @classmethod
    def trace_item_type(cls) -> TraceItemType.ValueType:
        return TraceItemType.TRACE_ITEM_TYPE_UNSPECIFIED

    @classmethod
    def get_from_trace_item_type(
        cls, trace_item_type: TraceItemType.ValueType
    ) -> "Type[TraceItemDataResolver[Tin, Tout]]":
        return cast(
            Type["TraceItemDataResolver[Tin, Tout]"],
            getattr(cls, "_registry").get_class_from_name(
                f"{cls.endpoint_name()}__{trace_item_type}"
            ),
        )

    def resolve(self, in_msg: Tin) -> Tout:
        raise NotImplementedError


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

    def get_resolver(
        self, trace_item_type: TraceItemType.ValueType
    ) -> TraceItemDataResolver[Tin, Tout]:
        raise NotImplementedError

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

    def _uses_storage_routing(self, in_msg: Tin) -> bool:
        return (
            hasattr(in_msg, "meta")
            and hasattr(in_msg.meta, "downsampled_storage_config")
            and in_msg.meta.downsampled_storage_config.mode
            != DownsampledStorageConfig.MODE_UNSPECIFIED
        )

    @final
    def execute(self, in_msg: Tin) -> Tout:
        scope = sentry_sdk.get_current_scope()
        scope.set_transaction_name(self.config_key())
        span = scope.span
        if span is not None:
            span.description = self.config_key()
        self.__before_execute(in_msg)
        error = None
        if (
            hasattr(in_msg, "meta")
            and hasattr(in_msg.meta, "start_timestamp")
            and hasattr(in_msg.meta, "end_timestamp")
        ):
            start = in_msg.meta.start_timestamp.ToDatetime()
            end = in_msg.meta.end_timestamp.ToDatetime()
            if (end - start).days > MAXIMUM_TIME_RANGE_IN_DAYS:
                timestamp = Timestamp()
                timestamp.FromDatetime(end - timedelta(days=MAXIMUM_TIME_RANGE_IN_DAYS))
                in_msg.meta.start_timestamp.CopyFrom(timestamp)
        try:
            out = self._execute(in_msg)
        except QueryException as e:
            if (
                "error_code" in e.extra["stats"]
                and e.extra["stats"]["error_code"] == 241
            ):
                self.metrics.increment("OOM_query")
                sentry_sdk.capture_exception(e)
            if (
                "error_code" in e.extra["stats"]
                and e.extra["stats"]["error_code"] == 159
            ):
                tags = {"endpoint": str(self.__class__.__name__)}
                if self._uses_storage_routing(in_msg):
                    tags["storage_routing_mode"] = DownsampledStorageConfig.Mode.Name(in_msg.meta.downsampled_storage_config.mode)  # type: ignore
                self.metrics.increment("timeout_query", 1, tags)
                sentry_sdk.capture_exception(e)
            if (
                "error_code" in e.extra["stats"]
                and e.extra["stats"]["error_code"] == 160
            ):
                tags = {"endpoint": str(self.__class__.__name__)}
                if self._uses_storage_routing(in_msg):
                    tags["storage_routing_mode"] = DownsampledStorageConfig.Mode.Name(in_msg.meta.downsampled_storage_config.mode)  # type: ignore
                self.metrics.increment("estimated_execution_timeout", 1, tags)
                sentry_sdk.capture_exception(e)
            out = self.response_class()()
            error = e
        except Exception as e:
            out = self.response_class()()
            error = e  # type: ignore
        return self.__after_execute(in_msg, out, error)

    def __before_execute(self, in_msg: Tin) -> None:
        self._timer.update_tags(self.__extract_request_tags(in_msg))
        self._timer.mark("rpc_start")
        self._before_execute(in_msg)

    def __extract_request_tags(self, in_msg: Tin) -> dict[str, str]:
        if not hasattr(in_msg, "meta"):
            return {}

        meta = in_msg.meta
        tags = {}

        if hasattr(meta, "start_timestamp") and hasattr(meta, "end_timestamp"):
            start = meta.start_timestamp.ToDatetime()
            end = meta.end_timestamp.ToDatetime()
            delta_in_hours = (end - start).total_seconds() / 3600
            bucket = bisect_left(_TIME_PERIOD_HOURS_BUCKETS, delta_in_hours)
            if delta_in_hours <= 1:
                tags["time_period"] = "lte_1_hour"
            elif delta_in_hours <= 24:
                tags["time_period"] = "lte_1_day"
            else:
                tags["time_period"] = (
                    f"lte_{_TIME_PERIOD_HOURS_BUCKETS[bucket] // 24}_days"
                    if bucket < _BUCKETS_COUNT
                    else f"gt_{_TIME_PERIOD_HOURS_BUCKETS[_BUCKETS_COUNT - 1] // 24}_days"
                )

        if hasattr(meta, "referrer"):
            tags["referrer"] = meta.referrer

        if self._uses_storage_routing(in_msg):
            tags["storage_routing_mode"] = DownsampledStorageConfig.Mode.Name(
                in_msg.meta.downsampled_storage_config.mode
            )

        return tags

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
            sentry_sdk.capture_exception(error)
            if isinstance(error, BadSnubaRPCRequestException):
                self.metrics.increment(
                    "request_invalid",
                    tags=self._timer.tags,
                )
            else:
                self.metrics.increment(
                    "request_error",
                    tags=self._timer.tags,
                )
            raise error
        else:
            self.metrics.increment(
                "request_success",
                tags=self._timer.tags,
            )
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
        sentry_sdk.capture_exception(e)
        return convert_rpc_exception_to_proto(
            RPCRequestException(
                status_code=500,
                message=f"internal error occurred while executing this RPC call: {e}",
            )
        )
