from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.conditions import in_condition
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.web.query import run_query as _run_query


def run_query() -> None:
    events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
    assert events_storage is not None

    query = Query(
        Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            SelectedExpression("event_id", Column("_snuba_event_id", None, "event_id")),
        ],
        condition=in_condition(Column(None, None, "project_id"), [Literal(None, 123)]),
    )

    query_settings = HTTPQuerySettings(referrer="asd")

    dataset = get_dataset("events")
    timer = Timer("test")

    result = _run_query(
        dataset,
        Request(
            id="asd",
            original_body={},
            query=query,
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
