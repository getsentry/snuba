from copy import deepcopy

import pytest

from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.processors.physical.mapping_optimizer import MappingOptimizer
from snuba.query.query_settings import HTTPQuerySettings
from tests.query.processors.query_builders import build_query


def tag_existence_expression(tag_name: str = "foo") -> FunctionCall:
    return FunctionCall(
        None,
        "notEquals",
        (
            FunctionCall(
                None,
                "ifNull",
                (
                    FunctionCall(
                        "_snuba_tags[duration_group]",
                        "arrayElement",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(
                                        None,
                                        None,
                                        "tags.key",
                                    ),
                                    Literal(
                                        None,
                                        tag_name,
                                    ),
                                ),
                            ),
                        ),
                    ),
                    Literal(None, ""),
                ),
            ),
            Literal(None, ""),
        ),
    )


def tag_equality_expression(
    tag_name: str = "foo", tag_value: str = "bar"
) -> FunctionCall:
    return FunctionCall(
        None,
        "equals",
        (
            FunctionCall(
                None,
                "ifNull",
                (
                    FunctionCall(
                        f"_snuba_tags[{tag_name}]",
                        "arrayElement",
                        (
                            Column(None, None, "tags.value"),
                            FunctionCall(
                                None,
                                "indexOf",
                                (
                                    Column(None, None, "tags.key"),
                                    Literal(None, tag_name),
                                ),
                            ),
                        ),
                    ),
                    Literal(None, ""),
                ),
            ),
            Literal(None, tag_value),
        ),
    )


def optimized_tag_expression(
    tag_name: str = "foo", tag_value: str = "bar"
) -> FunctionCall:
    return FunctionCall(
        None,
        "has",
        (
            Column(None, None, "_tags_hash_map"),
            FunctionCall(
                None,
                "cityHash64",
                (Literal(None, f"{tag_name}={tag_value}"),),
            ),
        ),
    )


def or_exp(op1: Expression, op2: Expression) -> FunctionCall:
    return FunctionCall(None, "or", (op1, op2))


def and_exp(op1: Expression, op2: Expression) -> FunctionCall:
    return FunctionCall(None, "and", (op1, op2))


noop = FunctionCall(None, "eq", (Literal(None, "foo"), Literal(None, "foo")))
noop_and = and_exp(Literal(None, True), Literal(None, True))
noop_or = or_exp(Literal(None, True), Literal(None, True))


TEST_CASES = [
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(tag_existence_expression(), tag_equality_expression()),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=optimized_tag_expression(),
        ),
        id="simplest happy path",
    ),
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=or_exp(tag_existence_expression(), tag_equality_expression()),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=or_exp(tag_existence_expression(), tag_equality_expression()),
        ),
        id="don't reduce OR",
    ),
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(
                noop,
                or_exp(
                    and_exp(tag_existence_expression(), tag_equality_expression()),
                    noop,
                ),
            ),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(
                noop,
                or_exp(optimized_tag_expression(), noop),
            ),
        ),
        id="useless condition nested in OR",
    ),
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(
                noop_or,
                and_exp(
                    noop_or,
                    and_exp(
                        noop_or,
                        and_exp(tag_existence_expression(), tag_equality_expression()),
                    ),
                ),
            ),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(
                and_exp(noop_or, noop_or),
                and_exp(noop_or, optimized_tag_expression()),
            ),
        ),
        id="useless condition nested in AND",
    ),
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(noop_and, and_exp(noop_and, noop)),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(
                Literal(None, True),
                and_exp(
                    and_exp(Literal(None, True), Literal(None, True)),
                    and_exp(Literal(None, True), noop),
                ),
            ),
        ),
        # these expressions are functionally equivalent (A AND B AND C = A AND (B AND C))
        # but the optimizer works by flatttening and recombining the and expressions. Therefore
        # the query tree structure will change but the computation will remain the same
        id="no optimize but query will change",
    ),
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(
                tag_existence_expression(tag_name="blah"),
                tag_equality_expression(tag_name="notblah"),
            ),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=and_exp(
                tag_existence_expression(tag_name="blah"),
                tag_equality_expression(tag_name="notblah"),
            ),
        ),
        id="don't optimize when tags are different",
    ),
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=or_exp(
                and_exp(
                    tag_existence_expression(tag_name="foo"),
                    tag_equality_expression(tag_name="foo"),
                ),
                and_exp(
                    tag_existence_expression(tag_name="blah"),
                    tag_equality_expression(tag_name="blah"),
                ),
            ),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=or_exp(
                optimized_tag_expression(tag_name="foo"),
                optimized_tag_expression(tag_name="blah"),
            ),
        ),
        id="optimize more than one tag",
    ),
]


@pytest.mark.parametrize("input_query, expected_query", deepcopy(TEST_CASES))
def test_recursive_useless_condition(
    input_query: ClickhouseQuery,
    expected_query: ClickhouseQuery,
) -> None:
    # copy the condition to the having condition so that we test both being
    # applied in one test
    input_query.set_ast_having(deepcopy(input_query.get_condition()))
    expected_query.set_ast_having(deepcopy(expected_query.get_condition()))
    MappingOptimizer(
        column_name="tags",
        hash_map_name="_tags_hash_map",
        killswitch="tags_hash_map_enabled",
    ).process_query(input_query, HTTPQuerySettings())
    assert input_query == expected_query


@pytest.mark.parametrize("input_query, expected_query", deepcopy(TEST_CASES))
def test_useless_has_condition(
    input_query: ClickhouseQuery,
    expected_query: ClickhouseQuery,
) -> None:
    from snuba.query.processors.physical.empty_tag_condition_processor import (
        EmptyTagConditionProcessor,
    )

    # change the existence expression to be a has(tags, 'my_tag') expression for boh queries
    # this allows reuse of the previous test cases
    EmptyTagConditionProcessor("tags.key").process_query(
        input_query, HTTPQuerySettings()
    )
    EmptyTagConditionProcessor("tags.key").process_query(
        expected_query, HTTPQuerySettings()
    )

    MappingOptimizer(
        column_name="tags",
        hash_map_name="_tags_hash_map",
        killswitch="tags_hash_map_enabled",
    ).process_query(input_query, HTTPQuerySettings())
    assert input_query == expected_query


# The optimizer returns early if the condition clause is NOT_OPTIMIZABLE, thus not touching
# the having clause of the query. This test is here to document that behaviour but there is
# no reason that it *has* to behave this way. Feel free to change it according to your needs
HAVING_SPECIAL_TEST_CASES = [
    pytest.param(
        build_query(
            selected_columns=[Column("count", None, "count")],
            # this condition will be transformed
            condition=or_exp(
                and_exp(
                    tag_existence_expression(tag_name="foo"),
                    tag_equality_expression(tag_name="foo"),
                ),
                tag_existence_expression(tag_name="blah"),
            ),
            # this clause will not be transformed (because the optimizer returns early)
            having=or_exp(
                and_exp(
                    tag_existence_expression(tag_name="foo"),
                    tag_equality_expression(tag_name="foo"),
                ),
                tag_existence_expression(tag_name="blah"),
            ),
        ),
        build_query(
            selected_columns=[Column("count", None, "count")],
            condition=or_exp(
                tag_equality_expression(tag_name="foo"),
                tag_existence_expression(tag_name="blah"),
            ),
            having=or_exp(
                and_exp(
                    tag_existence_expression(tag_name="foo"),
                    tag_equality_expression(tag_name="foo"),
                ),
                tag_existence_expression(tag_name="blah"),
            ),
        ),
        id="remove useless condition but don't optimize",
    ),
]


@pytest.mark.parametrize("input_query, expected_query", HAVING_SPECIAL_TEST_CASES)
def test_having_special_case(
    input_query: ClickhouseQuery, expected_query: ClickhouseQuery
) -> None:
    MappingOptimizer(
        column_name="tags",
        hash_map_name="_tags_hash_map",
        killswitch="tags_hash_map_enabled",
    ).process_query(input_query, HTTPQuerySettings())
    assert input_query == expected_query
