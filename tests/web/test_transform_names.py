import time
import uuid
from datetime import datetime

from snuba import settings
from snuba.attribution import get_app_id
from snuba.attribution.attribution_info import AttributionInfo
from snuba.datasets.entities import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.events_processor_base import InsertEvent
from snuba.datasets.factory import get_dataset
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.query_settings import HTTPQuerySettings
from snuba.reader import Column as MetaColumn
from snuba.request import Request
from snuba.utils.metrics.timer import Timer
from snuba.web.query import parse_and_run_query
from tests.helpers import write_unprocessed_events


def test_transform_column_names() -> None:
    """
    Runs a simple query containing selected expressions names that
    do not match the aliases of the expressions themselves.
    It verifies that the names of the columns in the result correspond
    to the SelectedExpression names and not to the expression aliases
    (which are supposed to be internal).
    """
    events_storage = get_entity(EntityKey.EVENTS).get_writable_storage()
    assert events_storage is not None

    event_id = uuid.uuid4().hex

    event_date = datetime.utcnow()
    write_unprocessed_events(
        events_storage,
        [
            InsertEvent(
                {
                    "event_id": event_id,
                    "group_id": 10,
                    "primary_hash": uuid.uuid4().hex,
                    "project_id": 1,
                    "message": "a message",
                    "platform": "python",
                    "datetime": event_date.strftime(settings.PAYLOAD_DATETIME_FORMAT),
                    "data": {"received": time.time()},
                    "organization_id": 1,
                    "retention_days": settings.DEFAULT_RETENTION_DAYS,
                }
            )
        ],
    )

    query = Query(
        Entity(EntityKey.EVENTS, get_entity(EntityKey.EVENTS).get_data_model()),
        selected_columns=[
            # The selected expression names are those provided by the
            # user in the query and those the user expect in the response.
            # Aliases will be internal names to prevent shadowing.
            SelectedExpression("event_id", Column("_snuba_event_id", None, "event_id")),
            SelectedExpression(
                "message",
                FunctionCall(
                    "_snuba_message",
                    "ifNull",
                    (Column(None, None, "message"), Literal(None, "")),
                ),
            ),
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
                get_app_id("blah"), "blah", None, None, None
            ),
        ),
        timer,
    )

    data = result.result["data"]
    assert data == [{"event_id": event_id, "message": "a message"}]
    meta = result.result["meta"]

    assert meta == [
        MetaColumn(name="event_id", type="String"),
        MetaColumn(name="message", type="String"),
    ]
