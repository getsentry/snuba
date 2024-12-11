from typing import Any

import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import (
    AttributeAggregation,
    AttributeKey,
    ExtrapolationMode,
    Function,
    Reliability,
)

from snuba.web.rpc.common.aggregation import (
    CUSTOM_COLUMN_PREFIX,
    Aggregator,
    CustomColumnInformation,
    ExtrapolationMeta,
)
from snuba.web.rpc.common.trace_item_types.spans import SpansSnubaRPCBridge


class TestRPCAggregation:
    @pytest.fixture
    def bridge(self) -> SpansSnubaRPCBridge:
        return SpansSnubaRPCBridge()

    @pytest.fixture
    def aggregator(self) -> Aggregator:
        return Aggregator(snuba_rpc_bridge=SpansSnubaRPCBridge())

    def test_generate_custom_column_alias(self) -> None:
        custom_column_information_with_metadata = CustomColumnInformation(
            custom_column_id="confidence_interval",
            referenced_column="count",
            metadata={"function_type": "count", "additional_metadata": "value"},
        )

        assert custom_column_information_with_metadata.to_alias() == (
            CUSTOM_COLUMN_PREFIX
            + "confidence_interval$count$function_type:count,additional_metadata:value"
        )

    def test_generate_custom_column_alias_without_metadata(self) -> None:
        custom_column_information_without_metadata = CustomColumnInformation(
            custom_column_id="column_id",
            referenced_column="count",
            metadata={},
        )

        assert (
            custom_column_information_without_metadata.to_alias()
            == CUSTOM_COLUMN_PREFIX + "column_id$count"
        )

    def test_generate_custom_column_alias_without_referenced_column(self) -> None:
        custom_column_information_without_referenced_column = CustomColumnInformation(
            custom_column_id="column_id",
            referenced_column=None,
            metadata={},
        )

        assert (
            custom_column_information_without_referenced_column.to_alias()
            == CUSTOM_COLUMN_PREFIX + "column_id"
        )

    def test_get_custom_column_information(self) -> None:
        alias = CUSTOM_COLUMN_PREFIX + "column_type$count$meta1:value1,meta2:value2"
        custom_column_information = CustomColumnInformation.from_alias(alias)
        assert custom_column_information == CustomColumnInformation(
            custom_column_id="column_type",
            referenced_column="count",
            metadata={"meta1": "value1", "meta2": "value2"},
        )

    def test_get_confidence_interval_column_for_non_extrapolatable_column(
        self, aggregator
    ) -> None:
        assert (
            aggregator.get_confidence_interval_column(
                AttributeAggregation(
                    aggregate=Function.FUNCTION_MIN,
                    key=AttributeKey(type=AttributeKey.TYPE_FLOAT, name="test"),
                    label="min(test)",
                    extrapolation_mode=ExtrapolationMode.EXTRAPOLATION_MODE_SAMPLE_WEIGHTED,
                )
            )
            is None
        )

    @pytest.mark.parametrize(
        ("row_data", "column_name", "average_sample_rate", "reliability"),
        [
            (
                {
                    "time": "2024-4-20 16:20:00",
                    "count(sentry.duration)": 100,
                    "p95(sentry.duration)": 123456,
                    CustomColumnInformation(
                        custom_column_id="confidence_interval",
                        referenced_column="count(sentry.duration)",
                        metadata={"function_type": "count"},
                    ).to_alias(): 20,
                    CustomColumnInformation(
                        custom_column_id="average_sample_rate",
                        referenced_column="count(sentry.duration)",
                        metadata={},
                    ).to_alias(): 0.5,
                    CustomColumnInformation(
                        custom_column_id="count",
                        referenced_column="count(sentry.duration)",
                        metadata={},
                    ).to_alias(): 120,
                    "group_by_attr_1": "g1",
                    "group_by_attr_2": "a1",
                },
                "count(sentry.duration)",
                0.5,
                Reliability.RELIABILITY_HIGH,
            ),
            (
                {
                    "time": "2024-4-20 16:20:00",
                    "count(sentry.duration)": 100,
                    "min(sentry.duration)": 123456,
                    CustomColumnInformation(
                        custom_column_id="confidence_interval",
                        referenced_column="count(sentry.duration)",
                        metadata={"function_type": "count"},
                    ).to_alias(): 20,
                    CustomColumnInformation(
                        custom_column_id="average_sample_rate",
                        referenced_column="count(sentry.duration)",
                        metadata={},
                    ).to_alias(): 0.5,
                    CustomColumnInformation(
                        custom_column_id="count",
                        referenced_column="count(sentry.duration)",
                        metadata={},
                    ).to_alias(): 120,
                    "group_by_attr_1": "g1",
                    "group_by_attr_2": "a1",
                },
                "min(sentry.duration)",
                0,
                Reliability.RELIABILITY_UNSPECIFIED,
            ),
        ],
    )
    def test_get_extrapolation_meta(
        self,
        row_data: dict[str, Any],
        column_name: str,
        average_sample_rate: float,
        reliability: Reliability.ValueType,
        aggregator: Aggregator,
    ) -> None:
        extrapolation_meta = ExtrapolationMeta.from_row(
            aggregator, row_data, column_name
        )
        assert extrapolation_meta.avg_sampling_rate == average_sample_rate
        assert extrapolation_meta.reliability == reliability
