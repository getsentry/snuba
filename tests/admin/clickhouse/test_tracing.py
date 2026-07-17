from datetime import UTC, datetime, timedelta

import pytest
from sentry_protos.snuba.v1.trace_item_pb2 import TraceItem

from snuba.admin.clickhouse.common import InvalidCustomQuery
from snuba.admin.clickhouse.tracing import run_query_and_get_trace
from snuba.datasets.storages.factory import get_writable_storage
from snuba.datasets.storages.storage_key import StorageKey
from tests.helpers import write_raw_unprocessed_events
from tests.web.rpc.v1.test_utils import gen_item_message

STORAGE = "eap_items"
TABLE = "eap_items_1_local"


@pytest.mark.clickhouse_db
@pytest.mark.redis_db
def test_ai_conversation_id_column_is_not_scrubbed() -> None:
    # Seed a span whose ai_conversation_id is a non-hex string, so that only the
    # scrub exemption (and not the hex/UUID passthrough) can leave it intact.
    # The proto conversation_id field is written to the ai_conversation_id column.
    conversation_id = "conv-not-hex-zzz"
    item = TraceItem()
    item.ParseFromString(
        gen_item_message(start_timestamp=datetime.now(tz=UTC) - timedelta(minutes=5))
    )
    item.conversation_id = conversation_id
    write_raw_unprocessed_events(
        get_writable_storage(StorageKey("eap_items")),
        [item.SerializeToString()],
    )

    trace = run_query_and_get_trace(STORAGE, f"SELECT ai_conversation_id FROM {TABLE}")

    assert trace.cols[0][0] == "ai_conversation_id"
    values = [row[0] for row in trace.result]
    assert conversation_id in values


# Aliasing an arbitrary expression to the exempt `ai_conversation_id` name must
# be rejected in every position it can appear -- SELECT scalar, ARRAY JOIN, and
# CTE (WITH) -- and in every spelling: plain `AS`, keywordless (SELECT only),
# and quoted (double-quote / backtick, which ClickHouse treats as identifiers).
_ALIASING_QUERIES = [
    # SELECT scalar alias
    pytest.param(
        f"SELECT project_id AS ai_conversation_id FROM {TABLE}",
        id="select_scalar-as",
    ),
    pytest.param(
        f"SELECT project_id ai_conversation_id FROM {TABLE}",
        id="select_scalar-no_as",
    ),
    pytest.param(
        f'SELECT project_id AS "ai_conversation_id" FROM {TABLE}',
        id="select_scalar-quoted_double",
    ),
    pytest.param(
        f"SELECT project_id AS `ai_conversation_id` FROM {TABLE}",
        id="select_scalar-quoted_backtick",
    ),
    # ARRAY JOIN alias
    pytest.param(
        f"SELECT trace_id FROM {TABLE} ARRAY JOIN some_arr AS ai_conversation_id",
        id="array_join-as",
    ),
    pytest.param(
        f'SELECT trace_id FROM {TABLE} ARRAY JOIN some_arr AS "ai_conversation_id"',
        id="array_join-quoted_double",
    ),
    pytest.param(
        f"SELECT trace_id FROM {TABLE} ARRAY JOIN some_arr AS `ai_conversation_id`",
        id="array_join-quoted_backtick",
    ),
    # CTE (WITH) alias -- ClickHouse's inverted `WITH <expr> AS <name>`
    pytest.param(
        f"WITH project_id AS ai_conversation_id SELECT ai_conversation_id FROM {TABLE}",
        id="cte-as",
    ),
    pytest.param(
        f'WITH project_id AS "ai_conversation_id" SELECT "ai_conversation_id" FROM {TABLE}',
        id="cte-quoted_double",
    ),
    pytest.param(
        f"WITH project_id AS `ai_conversation_id` SELECT `ai_conversation_id` FROM {TABLE}",
        id="cte-quoted_backtick",
    ),
]


@pytest.mark.parametrize("query", _ALIASING_QUERIES)
def test_aliasing_to_exempt_column_raises(query: str) -> None:
    # Result scrubbing trusts the reported column name (see is_scrub_exempt_column
    # / scrub_row), so aliasing an arbitrary expression to an exempt name would
    # surface unscrubbed data under a trusted name. validate_ro_query must reject
    # it up front, in every position and spelling above.
    with pytest.raises(InvalidCustomQuery, match="Aliasing"):
        run_query_and_get_trace(STORAGE, query)
