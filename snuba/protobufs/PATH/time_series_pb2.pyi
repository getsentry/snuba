from typing import ClassVar as _ClassVar
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

import base_messages_pb2 as _base_messages_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class TimeSeriesRequest(_message.Message):
    __slots__ = ("request_info", "pentity_filters", "pentity_aggregation")
    REQUEST_INFO_FIELD_NUMBER: _ClassVar[int]
    PENTITY_FILTERS_FIELD_NUMBER: _ClassVar[int]
    PENTITY_AGGREGATION_FIELD_NUMBER: _ClassVar[int]
    request_info: _base_messages_pb2.RequestInfo
    pentity_filters: _base_messages_pb2.PentityFilters
    pentity_aggregation: _base_messages_pb2.PentityAggregation
    def __init__(
        self,
        request_info: _Optional[_Union[_base_messages_pb2.RequestInfo, _Mapping]] = ...,
        pentity_filters: _Optional[
            _Union[_base_messages_pb2.PentityFilters, _Mapping]
        ] = ...,
        pentity_aggregation: _Optional[
            _Union[_base_messages_pb2.PentityAggregation, _Mapping]
        ] = ...,
    ) -> None: ...
