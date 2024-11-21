from snuba.web.rpc.common.aggregation import (
    CUSTOM_COLUMN_PREFIX,
    CustomColumnInformation,
    _generate_custom_column_alias,
    get_custom_column_information,
)


def test_generate_custom_column_alias() -> None:
    custom_column_information_with_metadata = CustomColumnInformation(
        column_type="column_type",
        referenced_column="count",
        metadata={"meta1": "value1", "meta2": "value2"},
    )

    assert _generate_custom_column_alias(custom_column_information_with_metadata) == (
        CUSTOM_COLUMN_PREFIX + "column_type$count$meta1:value1,meta2:value2"
    )


def test_generate_custom_column_alias_without_metadata() -> None:
    custom_column_information_without_metadata = CustomColumnInformation(
        column_type="column_type",
        referenced_column="count",
        metadata={},
    )

    assert _generate_custom_column_alias(
        custom_column_information_without_metadata
    ) == (CUSTOM_COLUMN_PREFIX + "column_type$count")


def test_generate_custom_column_alias_without_referenced_column() -> None:
    custom_column_information_without_referenced_column = CustomColumnInformation(
        column_type="column_type",
        referenced_column=None,
        metadata={},
    )

    assert _generate_custom_column_alias(
        custom_column_information_without_referenced_column
    ) == (CUSTOM_COLUMN_PREFIX + "column_type")


def test_get_custom_column_information() -> None:
    alias = CUSTOM_COLUMN_PREFIX + "column_type$count$meta1:value1,meta2:value2"
    custom_column_information = get_custom_column_information(alias)
    assert custom_column_information == CustomColumnInformation(
        column_type="column_type",
        referenced_column="count",
        metadata={"meta1": "value1", "meta2": "value2"},
    )
