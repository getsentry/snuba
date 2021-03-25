from snuba.clickhouse.columns import ColumnSet, Nested
from snuba.clickhouse.columns import SchemaModifiers as Modifier
from snuba.clickhouse.columns import String, UInt
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.storages.events_bool_contexts import (
    EventsBooleanContextsProcessor,
    EventsPromotedBooleanContextsProcessor,
)
from snuba.query import SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Table
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.request.request_settings import HTTPRequestSettings


def test_events_promoted_boolean_context() -> None:
    columns = ColumnSet(
        [
            ("device_charging", UInt(8, Modifier(nullable=True))),
            ("contexts", Nested([("key", String()), ("value", String())])),
        ]
    )
    query = ClickhouseQuery(
        Table("events", columns),
        selected_columns=[
            SelectedExpression(
                "contexts[device.charging]",
                FunctionCall(
                    "contexts[device.charging]",
                    "arrayElement",
                    (
                        Column(None, None, "contexts.value"),
                        FunctionCall(
                            None,
                            "indexOf",
                            (
                                Column(None, None, "contexts.key"),
                                Literal(None, "device.charging"),
                            ),
                        ),
                    ),
                ),
            )
        ],
    )

    expected = ClickhouseQuery(
        Table("events", columns),
        selected_columns=[
            SelectedExpression(
                "contexts[device.charging]",
                FunctionCall(
                    "contexts[device.charging]",
                    "if",
                    (
                        binary_condition(
                            ConditionFunctions.IN,
                            FunctionCall(
                                None,
                                "toString",
                                (Column(None, None, "device_charging"),),
                            ),
                            literals_tuple(
                                None, [Literal(None, "1"), Literal(None, "True")]
                            ),
                        ),
                        Literal(None, "True"),
                        Literal(None, "False"),
                    ),
                ),
            )
        ],
    )

    settings = HTTPRequestSettings()
    MappingColumnPromoter(
        {"contexts": {"device.charging": "device_charging"}}
    ).process_query(query, settings)
    EventsPromotedBooleanContextsProcessor().process_query(query, settings)

    assert query.get_selected_columns() == expected.get_selected_columns()


def test_events_boolean_context() -> None:
    columns = ColumnSet(
        [("contexts", Nested([("key", String()), ("value", String())]))]
    )
    query = ClickhouseQuery(
        Table("errors", columns),
        selected_columns=[
            SelectedExpression(
                "contexts[device.charging]",
                FunctionCall(
                    "contexts[device.charging]",
                    "arrayElement",
                    (
                        Column(None, None, "contexts.value"),
                        FunctionCall(
                            None,
                            "indexOf",
                            (
                                Column(None, None, "contexts.key"),
                                Literal(None, "device.charging"),
                            ),
                        ),
                    ),
                ),
            )
        ],
    )

    expected = ClickhouseQuery(
        Table("errors", columns),
        selected_columns=[
            SelectedExpression(
                "contexts[device.charging]",
                FunctionCall(
                    "contexts[device.charging]",
                    "if",
                    (
                        binary_condition(
                            ConditionFunctions.IN,
                            FunctionCall(
                                None,
                                "arrayElement",
                                (
                                    Column(None, None, "contexts.value"),
                                    FunctionCall(
                                        None,
                                        "indexOf",
                                        (
                                            Column(None, None, "contexts.key"),
                                            Literal(None, "device.charging"),
                                        ),
                                    ),
                                ),
                            ),
                            literals_tuple(
                                None, [Literal(None, "1"), Literal(None, "True")]
                            ),
                        ),
                        Literal(None, "True"),
                        Literal(None, "False"),
                    ),
                ),
            )
        ],
    )

    settings = HTTPRequestSettings()
    EventsBooleanContextsProcessor().process_query(query, settings)

    assert query.get_selected_columns() == expected.get_selected_columns()
