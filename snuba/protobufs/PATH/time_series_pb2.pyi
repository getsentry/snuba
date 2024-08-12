"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""

import base_messages_pb2
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import google.protobuf.timestamp_pb2
import typing

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing.final
class TimeSeriesRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    REQUEST_INFO_FIELD_NUMBER: builtins.int
    PENTITY_FILTERS_FIELD_NUMBER: builtins.int
    PENTITY_AGGREGATION_FIELD_NUMBER: builtins.int
    GRANULARITY_SECS_FIELD_NUMBER: builtins.int
    granularity_secs: builtins.int
    @property
    def request_info(self) -> base_messages_pb2.RequestInfo: ...
    @property
    def pentity_filters(self) -> base_messages_pb2.PentityFilters: ...
    @property
    def pentity_aggregation(self) -> base_messages_pb2.PentityAggregation: ...
    def __init__(
        self,
        *,
        request_info: base_messages_pb2.RequestInfo | None = ...,
        pentity_filters: base_messages_pb2.PentityFilters | None = ...,
        pentity_aggregation: base_messages_pb2.PentityAggregation | None = ...,
        granularity_secs: builtins.int = ...,
    ) -> None: ...
    def HasField(self, field_name: typing.Literal["pentity_aggregation", b"pentity_aggregation", "pentity_filters", b"pentity_filters", "request_info", b"request_info"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing.Literal["granularity_secs", b"granularity_secs", "pentity_aggregation", b"pentity_aggregation", "pentity_filters", b"pentity_filters", "request_info", b"request_info"]) -> None: ...

global___TimeSeriesRequest = TimeSeriesRequest

@typing.final
class TimeSeriesResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TIMESTAMP_FIELD_NUMBER: builtins.int
    VALUE_FIELD_NUMBER: builtins.int
    @property
    def timestamp(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def value(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
    def __init__(
        self,
        *,
        timestamp: google.protobuf.timestamp_pb2.Timestamp | None = ...,
        value: collections.abc.Iterable[builtins.float] | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing.Literal["timestamp", b"timestamp"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing.Literal["timestamp", b"timestamp", "value", b"value"]) -> None: ...

global___TimeSeriesResponse = TimeSeriesResponse
