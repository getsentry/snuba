from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import well_known_types as _well_known_types

DESCRIPTOR: _descriptor.FileDescriptor

class google(_message.Message):
    __slots__ = ()

    class protobuf(_message.Message):
        __slots__ = ()

        class Timestamp(_message.Message, _well_known_types.Timestamp):
            __slots__ = ("seconds", "nanos")
            SECONDS_FIELD_NUMBER: _ClassVar[int]
            NANOS_FIELD_NUMBER: _ClassVar[int]
            seconds: int
            nanos: int
            def __init__(
                self, seconds: _Optional[int] = ..., nanos: _Optional[int] = ...
            ) -> None: ...

        def __init__(self) -> None: ...

    def __init__(self) -> None: ...

class RequestInfo(_message.Message):
    __slots__ = (
        "project_ids",
        "organization_ids",
        "cogs_category",
        "referrer",
        "start_timestamp",
        "end_timestamp",
    )
    PROJECT_IDS_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_IDS_FIELD_NUMBER: _ClassVar[int]
    COGS_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    REFERRER_FIELD_NUMBER: _ClassVar[int]
    START_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    END_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    project_ids: _containers.RepeatedScalarFieldContainer[int]
    organization_ids: _containers.RepeatedScalarFieldContainer[int]
    cogs_category: str
    referrer: str
    start_timestamp: google.protobuf.Timestamp
    end_timestamp: google.protobuf.Timestamp
    def __init__(
        self,
        project_ids: _Optional[_Iterable[int]] = ...,
        organization_ids: _Optional[_Iterable[int]] = ...,
        cogs_category: _Optional[str] = ...,
        referrer: _Optional[str] = ...,
        start_timestamp: _Optional[_Union[google.protobuf.Timestamp, _Mapping]] = ...,
        end_timestamp: _Optional[_Union[google.protobuf.Timestamp, _Mapping]] = ...,
    ) -> None: ...

class PentityFilter(_message.Message):
    __slots__ = (
        "attribute_name",
        "comparison",
        "string_literal",
        "int_literal",
        "float_literal",
    )
    ATTRIBUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    COMPARISON_FIELD_NUMBER: _ClassVar[int]
    STRING_LITERAL_FIELD_NUMBER: _ClassVar[int]
    INT_LITERAL_FIELD_NUMBER: _ClassVar[int]
    FLOAT_LITERAL_FIELD_NUMBER: _ClassVar[int]
    attribute_name: str
    comparison: str
    string_literal: str
    int_literal: int
    float_literal: float
    def __init__(
        self,
        attribute_name: _Optional[str] = ...,
        comparison: _Optional[str] = ...,
        string_literal: _Optional[str] = ...,
        int_literal: _Optional[int] = ...,
        float_literal: _Optional[float] = ...,
    ) -> None: ...

class PentityFilters(_message.Message):
    __slots__ = ("pentity_name", "filters")
    PENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    pentity_name: str
    filters: _containers.RepeatedCompositeFieldContainer[PentityFilter]
    def __init__(
        self,
        pentity_name: _Optional[str] = ...,
        filters: _Optional[_Iterable[_Union[PentityFilter, _Mapping]]] = ...,
    ) -> None: ...

class PentityAggregation(_message.Message):
    __slots__ = ("aggregation_type", "pentity_name", "attribute_name")
    AGGREGATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    PENTITY_NAME_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTE_NAME_FIELD_NUMBER: _ClassVar[int]
    aggregation_type: str
    pentity_name: str
    attribute_name: str
    def __init__(
        self,
        aggregation_type: _Optional[str] = ...,
        pentity_name: _Optional[str] = ...,
        attribute_name: _Optional[str] = ...,
    ) -> None: ...
