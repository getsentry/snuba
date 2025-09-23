"""
This file contains functionality to encode and decode custom page tokens
"""

from google.protobuf.timestamp_pb2 import Timestamp
from sentry_protos.snuba.v1.request_common_pb2 import PageToken
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey, AttributeValue
from sentry_protos.snuba.v1.trace_item_filter_pb2 import (
    AndFilter,
    ComparisonFilter,
    TraceItemFilter,
)


class FlexibleTimeWindowPage:
    _START_TIMESTAMP_KEY = "sentry.start_timestamp"
    _END_TIMESTAMP_KEY = "sentry.end_timestamp"
    _OFFSET_KEY = "sentry.offset"

    def __init__(
        self,
        start_timestamp: Timestamp | None,
        end_timestamp: Timestamp | None,
        offset: int | None = None,
    ):
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.offset = offset

    def encode(self) -> PageToken:
        if self.start_timestamp is not None and self.end_timestamp is not None:
            return PageToken(
                filter_offset=TraceItemFilter(
                    and_filter=AndFilter(
                        filters=[
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(name=self._START_TIMESTAMP_KEY),
                                    op=ComparisonFilter.OP_GREATER_THAN_OR_EQUALS,
                                    value=AttributeValue(val_int=self.start_timestamp.seconds),
                                )
                            ),
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(name=self._END_TIMESTAMP_KEY),
                                    op=ComparisonFilter.OP_LESS_THAN,
                                    value=AttributeValue(val_int=self.end_timestamp.seconds),
                                )
                            ),
                            TraceItemFilter(
                                comparison_filter=ComparisonFilter(
                                    key=AttributeKey(name=self._OFFSET_KEY),
                                    op=ComparisonFilter.OP_EQUALS,
                                    value=AttributeValue(
                                        val_int=self.offset if self.offset is not None else 0
                                    ),
                                )
                            ),
                        ]
                    )
                )
            )
        else:
            return PageToken(offset=self.offset if self.offset is not None else 0)

    @classmethod
    def decode(cls, page_token: PageToken) -> "FlexibleTimeWindowPage":
        start_timestamp = None
        end_timestamp = None
        offset = None
        if page_token.filter_offset.HasField("and_filter"):
            for filter in page_token.filter_offset.and_filter.filters:
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == cls._START_TIMESTAMP_KEY
                ):
                    start_timestamp = Timestamp(seconds=filter.comparison_filter.value.val_int)
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == cls._END_TIMESTAMP_KEY
                ):
                    end_timestamp = Timestamp(seconds=filter.comparison_filter.value.val_int)
                if (
                    filter.HasField("comparison_filter")
                    and filter.comparison_filter.key.name == cls._OFFSET_KEY
                ):
                    offset = filter.comparison_filter.value.val_int
        elif page_token.HasField("offset"):
            offset = page_token.offset
        return cls(start_timestamp, end_timestamp, offset)
