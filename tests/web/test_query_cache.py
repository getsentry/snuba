from typing import Any, Callable

import pytest
from clickhouse_driver.errors import ErrorCodes

from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.clickhouse.errors import ClickhouseError
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.web import QueryException
from snuba.web.query import parse_and_run_query


def run_query() -> None:
    events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
    assert events_storage is not None

    query = Query(
        Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("event_id", Column("_snuba_event_id", None, "event_id")),
        ],
    )

    query_settings = HTTPQuerySettings(referrer="asd")

    dataset = get_dataset("events")
    timer = Timer("test")

    result = parse_and_run_query(
        dataset,
        Request(
            id="asd",
            original_body={},
            query=query,
            snql_anonymized="",
            query_settings=query_settings,
            attribution_info=AttributionInfo(
                get_app_id("blah"),
                {"referrer": "r", "organization_id": 1234},
                "blah",
                None,
                None,
                None,
            ),
        ),
        timer,
    )

    assert result.result["data"] == []


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_cache_retries_on_bad_query_id(
    monkeypatch: pytest.MonkeyPatch, snuba_set_config: Callable[[str, Any], None]
) -> None:
    from snuba.web import db_query

    calls = []

    old_excecute_query_with_rate_limits = db_query.execute_query_with_rate_limits

    def execute_query_with_rate_limits(*args: Any) -> Any:
        calls.append(args[-2]["query_id"])

        if len(calls) == 1:
            raise ClickhouseError(
                "duplicate query!",
                True,
                code=ErrorCodes.QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING,
            )

        return old_excecute_query_with_rate_limits(*args)

    monkeypatch.setattr(
        db_query, "execute_query_with_rate_limits", execute_query_with_rate_limits
    )

    with pytest.raises(QueryException):
        run_query()

    assert len(calls) == 1
    calls.clear()

    snuba_set_config("retry_duplicate_query_id", True)

    run_query()

    assert len(calls) == 2
    assert "randomized" not in calls[0]
    assert "randomized" in calls[1]
