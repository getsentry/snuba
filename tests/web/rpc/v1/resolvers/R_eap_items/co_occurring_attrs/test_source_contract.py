"""Contract every CoOccurringAttrsSource must satisfy.

The endpoint builds one query from whichever source it is handed, so these invariants are
what let it do that without knowing which storage it got. Parameterized over all
implementations, so a new source has to satisfy them too.
"""

import pytest
from sentry_protos.snuba.v1.trace_item_attribute_pb2 import AttributeKey

from snuba.datasets.storages.factory import get_storage
from snuba.query.expressions import FunctionCall
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs import V1, V2
from snuba.web.rpc.v1.resolvers.R_eap_items.co_occurring_attrs.base import (
    CoOccurringAttrsSource,
)

ALL_SOURCES = [V1, V2]
SOURCE_IDS = [type(source).__name__ for source in ALL_SOURCES]

# Every AttributeKey type a caller can send, including the ones a given storage may not
# represent natively.
ALL_ATTRIBUTE_TYPES = [value.number for value in AttributeKey.Type.DESCRIPTOR.values]


@pytest.fixture(params=ALL_SOURCES, ids=SOURCE_IDS)
def source(request: pytest.FixtureRequest) -> CoOccurringAttrsSource:
    return request.param  # type: ignore[no-any-return]


def test_implements_the_interface(source: CoOccurringAttrsSource) -> None:
    assert isinstance(source, CoOccurringAttrsSource)


def test_storage_exists(source: CoOccurringAttrsSource) -> None:
    """The declared storage key must resolve, or the FROM clause cannot be built."""
    assert get_storage(source.storage_key) is not None


def test_data_source_matches_the_storage_schema(source: CoOccurringAttrsSource) -> None:
    data_source = source.data_source
    assert data_source.key == source.storage_key
    expected = get_storage(source.storage_key).get_schema().get_columns()
    assert data_source.schema == expected


@pytest.mark.parametrize("attr_type", ALL_ATTRIBUTE_TYPES)
def test_every_attribute_type_reads_something(
    source: CoOccurringAttrsSource, attr_type: AttributeKey.Type.ValueType
) -> None:
    """No requested type may produce an empty read.

    An empty list would make the endpoint emit an arrayConcat() of nothing and silently
    return no attributes, so a storage that cannot answer a type natively must fall back to
    whatever it does have.
    """
    assert len(source.typed_key_arrays(attr_type)) > 0


@pytest.mark.parametrize("attr_type", ALL_ATTRIBUTE_TYPES)
def test_key_arrays_exist_on_the_storage(
    source: CoOccurringAttrsSource, attr_type: AttributeKey.Type.ValueType
) -> None:
    """Every column a source claims to read must exist on its storage.

    This is the invariant that would break if a source were pointed at the wrong table, or
    if a column were renamed in one schema but not the mapping.
    """
    available = {c.name for c in get_storage(source.storage_key).get_schema().get_columns()}
    for column, _ in source.typed_key_arrays(attr_type):
        assert column in available, f"{column} missing from {source.storage_key.value}"


@pytest.mark.parametrize("attr_type", ALL_ATTRIBUTE_TYPES)
def test_reported_types_are_real_attribute_types(
    source: CoOccurringAttrsSource, attr_type: AttributeKey.Type.ValueType
) -> None:
    """The type name each key is tagged with is resolved via getattr(AttributeKey.Type, ...)
    when building the response, so a typo would only surface as a runtime AttributeError."""
    for _, type_name in source.typed_key_arrays(attr_type):
        assert AttributeKey.Type.Value(type_name) is not None


@pytest.mark.parametrize("attr_type", ALL_ATTRIBUTE_TYPES)
def test_no_column_is_read_twice(
    source: CoOccurringAttrsSource, attr_type: AttributeKey.Type.ValueType
) -> None:
    """Reading a column twice would emit each of its keys twice in the response."""
    columns = [col for col, _ in source.typed_key_arrays(attr_type)]
    assert len(set(columns)) == len(columns), f"duplicate columns for {attr_type}: {columns}"


def test_count_is_an_aliased_aggregate(source: CoOccurringAttrsSource) -> None:
    """The endpoint selects this as `count` and orders by that alias, so the expression has
    to carry it regardless of which aggregate the storage uses."""
    expression = source.count_expression()
    assert isinstance(expression, FunctionCall)
    assert expression.alias == "count"


def test_repr_names_the_storage(source: CoOccurringAttrsSource) -> None:
    assert source.storage_key.value in repr(source)


def test_last_seen_capability_is_consistent(source: CoOccurringAttrsSource) -> None:
    """has_last_seen must describe what last_seen_expression actually does.

    The endpoint checks the flag and then calls the expression, so a source claiming the
    capability without implementing it (or vice versa) would either crash or silently drop
    the column.
    """
    if source.has_last_seen:
        expression = source.last_seen_expression()
        assert isinstance(expression, FunctionCall)
        assert expression.alias == "last_seen"
    else:
        with pytest.raises(NotImplementedError):
            source.last_seen_expression()


def test_last_seen_column_is_available_when_claimed(source: CoOccurringAttrsSource) -> None:
    """A source claiming last_seen must actually have the column, or the query fails at
    ClickHouse rather than here."""
    if not source.has_last_seen:
        return
    columns = {c.name for c in get_storage(source.storage_key).get_schema().get_columns()}
    assert "last_seen" in columns
