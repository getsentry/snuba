import datetime

from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.mapping_optimizer import MappingOptimizer
from snuba.request.request_settings import HTTPRequestSettings
from snuba.state import set_config
from tests.query.processors.query_builders import build_query

query = build_query(
    selected_columns=[Column("count", None, "count")],
    condition=FunctionCall(
        None,
        "and",
        (
            FunctionCall(
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
                                            Column(None, None, "tags.key",),
                                            Literal(None, "duration_group",),
                                        ),
                                    ),
                                ),
                            ),
                            Literal(None, ""),
                        ),
                    ),
                    Literal(None, ""),
                ),
            ),
            FunctionCall(
                None,
                "equals",
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
                                            Column(None, None, "tags.key",),
                                            Literal(None, "duration_group",),
                                        ),
                                    ),
                                ),
                            ),
                            Literal(None, ""),
                        ),
                    ),
                    Literal(None, "<10s"),
                ),
            ),
        ),
    ),
)


def test_bs():
    set_config("tags_hash_map_enabled", 1)
    print("BEFORE: \n", query)
    MappingOptimizer(
        column_name="tags",
        hash_map_name="_tags_hash_map",
        killswitch="tags_hash_map_enabled",
    ).process_query(query, HTTPRequestSettings())
    print("<" * 100)
    print("AFTER: \n", query)
    assert "arrayElement" not in repr(query)
