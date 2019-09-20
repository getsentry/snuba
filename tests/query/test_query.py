from snuba.query.query import Query


def test_empty_query():
    query = Query({})

    assert query.get_selected_columns() is None
    assert query.get_aggregations() is None
    assert query.get_groupby() is None
    assert query.get_conditions() is None
    assert query.get_orderby() is None
    assert query.get_sample() is None
    assert query.get_limit() == 0
    assert query.get_offset() == 0


def test_full_query():
    query = Query({
        "selected_columns": ["c1", "c2", "c3"],
        "conditions": [["c1", "=", "a"]],
        "groupby": ["project_id"],
        "aggregations": [["count()", "", "count"]],
        "orderby": "event_id",
        "sample": 10,
        "limit": 100,
        "offset": 50,
    })

    assert query.get_selected_columns() == ["c1", "c2", "c3"]
    assert query.get_aggregations() == [["count()", "", "count"]]
    assert query.get_groupby() == ["project_id"]
    assert query.get_conditions() == [["c1", "=", "a"]]
    assert query.get_orderby() == "event_id"
    assert query.get_sample() == 10
    assert query.get_limit() == 100
    assert query.get_offset() == 50


def test_edit_query():
    query = Query({
        "selected_columns": ["c1", "c2", "c3"],
        "conditions": [["c1", "=", "a"]],
        "groupby": ["project_id"],
        "aggregations": [["count()", "", "count"]],
        "orderby": "event_id",
        "sample": 10,
        "limit": 100,
        "offset": 50,
    })

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
