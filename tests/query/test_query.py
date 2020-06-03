from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.factory import get_dataset
from snuba.datasets.schemas.tables import TableSource
from snuba.query.logical import Query


def test_empty_query():
    query = Query({}, TableSource("my_table", ColumnSet([])))

    assert query.get_selected_columns() is None
    assert query.get_aggregations() is None
    assert query.get_groupby() is None
    assert query.get_conditions() is None
    assert query.get_arrayjoin() is None
    assert query.get_having() == []
    assert query.get_orderby() is None
    assert query.get_limitby() is None
    assert query.get_sample() is None
    assert query.get_limit() is None
    assert query.get_offset() == 0
    assert query.has_totals() is False
    assert query.get_prewhere() == []

    assert query.get_data_source().format_from() == "my_table"


def test_full_query():
    query = Query(
        {
            "selected_columns": ["c1", "c2", "c3"],
            "conditions": [["c1", "=", "a"]],
            "arrayjoin": "tags",
            "having": [["c4", "=", "c"]],
            "groupby": ["project_id"],
            "aggregations": [["count()", "", "count"]],
            "orderby": "event_id",
            "limitby": (100, "environment"),
            "sample": 10,
            "limit": 100,
            "offset": 50,
            "totals": True,
            "granularity": 60,
        },
        TableSource("my_table", ColumnSet([])),
    )

    assert query.get_selected_columns() == ["c1", "c2", "c3"]
    assert query.get_aggregations() == [["count()", "", "count"]]
    assert query.get_groupby() == ["project_id"]
    assert query.get_conditions() == [["c1", "=", "a"]]
    assert query.get_arrayjoin() == "tags"
    assert query.get_having() == [["c4", "=", "c"]]
    assert query.get_orderby() == "event_id"
    assert query.get_limitby() == (100, "environment")
    assert query.get_sample() == 10
    assert query.get_limit() == 100
    assert query.get_offset() == 50
    assert query.has_totals() is True
    assert query.get_granularity() == 60

    assert query.get_data_source().format_from() == "my_table"


def test_edit_query():
    query = Query(
        {
            "selected_columns": ["c1", "c2", "c3"],
            "conditions": [["c1", "=", "a"]],
            "arrayjoin": "tags",
            "having": [["c4", "=", "c"]],
            "groupby": ["project_id"],
            "aggregations": [["count()", "", "count"]],
            "orderby": "event_id",
            "limitby": (100, "environment"),
            "sample": 10,
            "limit": 100,
            "offset": 50,
            "totals": True,
        },
        TableSource("my_table", ColumnSet([])),
    )

    query.set_selected_columns(["c4"])
    assert query.get_selected_columns() == ["c4"]

    query.set_aggregations([["different_agg()", "", "something"]])
    assert query.get_aggregations() == [["different_agg()", "", "something"]]

    query.add_groupby(["more", "more2"])
    assert query.get_groupby() == ["project_id", "more", "more2"]

    query.add_conditions([["c5", "=", "9"]])
    assert query.get_conditions() == [
        ["c1", "=", "a"],
        ["c5", "=", "9"],
    ]

    query.set_conditions([["c6", "=", "10"]])
    assert query.get_conditions() == [
        ["c6", "=", "10"],
    ]

    query.set_arrayjoin("not_tags")
    assert query.get_arrayjoin() == "not_tags"

    query.set_granularity(7200)
    assert query.get_granularity() == 7200

    query.set_prewhere([["pc6", "=", "10"]])
    assert query.get_prewhere() == [["pc6", "=", "10"]]


def test_referenced_columns():
    # a = 1 AND b = 1
    dataset = get_dataset("events")
    source = (
        dataset.get_all_storages()[0].get_schemas().get_read_schema().get_data_source()
    )
    body = {"conditions": [["a", "=", "1"], ["b", "=", "1"]]}
    query = Query(body, source)
    assert query.get_all_referenced_columns() == set(["a", "b"])
    assert query.get_columns_referenced_in_conditions() == set(["a", "b"])
    assert query.get_columns_referenced_in_having() == set([])

    # a = 1 AND (b = 1 OR c = 1)
    body = {"conditions": [["a", "=", "1"], [["b", "=", "1"], ["c", "=", "1"]]]}
    query = Query(body, source)
    assert query.get_all_referenced_columns() == set(["a", "b", "c"])
    assert query.get_columns_referenced_in_conditions() == set(["a", "b", "c"])
    assert query.get_columns_referenced_in_having() == set([])

    # a = 1 AND (b = 1 OR foo(c) = 1)
    body = {
        "conditions": [["a", "=", "1"], [["b", "=", "1"], [["foo", ["c"]], "=", "1"]]]
    }
    query = Query(body, source)
    assert query.get_all_referenced_columns() == set(["a", "b", "c"])
    assert query.get_columns_referenced_in_conditions() == set(["a", "b", "c"])
    assert query.get_columns_referenced_in_having() == set([])

    # a = 1 AND (b = 1 OR foo(c, bar(d)) = 1)
    body = {
        "conditions": [
            ["a", "=", "1"],
            [["b", "=", "1"], [["foo", ["c", ["bar", ["d"]]]], "=", "1"]],
        ]
    }
    query = Query(body, source)
    assert query.get_all_referenced_columns() == set(["a", "b", "c", "d"])
    assert query.get_columns_referenced_in_conditions() == set(["a", "b", "c", "d"])
    assert query.get_columns_referenced_in_having() == set([])

    # Other fields, including expressions in selected columns
    body = {
        "arrayjoin": "tags_key",
        "groupby": ["time", "group_id"],
        "orderby": "-time",
        "selected_columns": [
            "group_id",
            "time",
            ["foo", ["c", ["bar", ["d"]]]],  # foo(c, bar(d))
        ],
        "aggregations": [["uniq", "tags_value", "values_seen"]],
    }
    query = Query(body, source)
    assert query.get_all_referenced_columns() == set(
        ["tags_key", "tags_value", "time", "group_id", "c", "d"]
    )
    assert query.get_columns_referenced_in_conditions() == set([])
    assert query.get_columns_referenced_in_having() == set([])

    body = {
        "conditions": [["a", "=", "1"]],
        "having": [
            ["b", "=", "1"],
            [["c", "=", "1"], [["foo", ["d", ["bar", ["e"]]]], "=", "1"]],
        ],
    }
    query = Query(body, source)
    query.set_prewhere([["pc6", "=", "10"]])
    assert query.get_all_referenced_columns() == set(["a", "b", "c", "d", "e", "pc6"])
    assert query.get_columns_referenced_in_having() == set(["b", "c", "d", "e"])
