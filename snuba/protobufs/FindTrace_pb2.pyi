from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class FindTraceRequest(_message.Message):
    __slots__ = ("filters",)

    class SpanWithAttrKeyExists(_message.Message):
        __slots__ = ("attr_key",)
        ATTR_KEY_FIELD_NUMBER: _ClassVar[int]
        attr_key: str
        def __init__(self, attr_key: _Optional[str] = ...) -> None: ...

    class SpanWithAttrKeyEqualsValue(_message.Message):
        __slots__ = ("attr_key", "attr_value")
        ATTR_KEY_FIELD_NUMBER: _ClassVar[int]
        ATTR_VALUE_FIELD_NUMBER: _ClassVar[int]
        attr_key: str
        attr_value: str
        def __init__(
            self, attr_key: _Optional[str] = ..., attr_value: _Optional[str] = ...
        ) -> None: ...

    class Filter(_message.Message):
        __slots__ = ("span_with_attr_key_exists", "span_with_attr_key_equals_value")
        SPAN_WITH_ATTR_KEY_EXISTS_FIELD_NUMBER: _ClassVar[int]
        SPAN_WITH_ATTR_KEY_EQUALS_VALUE_FIELD_NUMBER: _ClassVar[int]
        span_with_attr_key_exists: FindTraceRequest.SpanWithAttrKeyExists
        span_with_attr_key_equals_value: FindTraceRequest.SpanWithAttrKeyEqualsValue
        def __init__(
            self,
            span_with_attr_key_exists: _Optional[
                _Union[FindTraceRequest.SpanWithAttrKeyExists, _Mapping]
            ] = ...,
            span_with_attr_key_equals_value: _Optional[
                _Union[FindTraceRequest.SpanWithAttrKeyEqualsValue, _Mapping]
            ] = ...,
        ) -> None: ...
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    filters: _containers.RepeatedCompositeFieldContainer[FindTraceRequest.Filter]
    def __init__(
        self,
        filters: _Optional[_Iterable[_Union[FindTraceRequest.Filter, _Mapping]]] = ...,
    ) -> None: ...

class FindTraceResponse(_message.Message):
    __slots__ = ("trace_uuids",)
    TRACE_UUIDS_FIELD_NUMBER: _ClassVar[int]
    trace_uuids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, trace_uuids: _Optional[_Iterable[str]] = ...) -> None: ...
