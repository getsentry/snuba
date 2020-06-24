from snuba.clickhouse.columns import ColumnSet, Nested, Nullable, String, UInt
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storages.events_bool_contexts import EventsBooleanContextsProcessor
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query as LogicalQuery
from snuba.query.processors.mapping_promoter import MappingColumnPromoter
from snuba.request.request_settings import HTTPRequestSettings


def test_events_boolean_context() -> None:
    columns = ColumnSet(
        [
            ("device_charging", Nullable(UInt(8))),
            ("contexts", Nested([("key", String()), ("value", String())])),
        ]
    )
    query = ClickhouseQuery(
        LogicalQuery(
            {},
            TableSource("events", columns),
            selected_columns=[
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
                )
            ],
        )
    )

    expected = ClickhouseQuery(
        LogicalQuery(
            {},
            TableSource("events", columns),
            selected_columns=[
                FunctionCall(
                    "contexts[device.charging]",
                    "multiIf",
                    (
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            FunctionCall(
                                None,
                                "toString",
                                (Column(None, None, "device_charging"),),
                            ),
                            Literal(None, ""),
                        ),
                        Literal(None, ""),
                        binary_condition(
                            None,
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
                )
            ],
        )
    )

    settings = HTTPRequestSettings()
    MappingColumnPromoter(
        {"contexts": {"device.charging": "device_charging"}}
    ).process_query(query, settings)
    EventsBooleanContextsProcessor().process_query(query, settings)

    assert (
        query.get_selected_columns_from_ast()
        == expected.get_selected_columns_from_ast()
    )
