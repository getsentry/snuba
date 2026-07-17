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


def test_aliasing_to_exempt_column_raises() -> None:
    # The scrub exemption is keyed on the reported column name, so aliasing an
    # arbitrary expression to an exempt name would let it bypass scrubbing.
    # Such queries are rejected up front by validate_ro_query.
    with pytest.raises(InvalidCustomQuery, match="Aliasing"):
        run_query_and_get_trace(STORAGE, f"SELECT project_id AS ai_conversation_id FROM {TABLE}")


def test_array_join_aliasing_to_exempt_column_raises() -> None:
    # ARRAY JOIN aliases are parsed into tables_aliases rather than
    # columns_aliases_names, so they must be rejected too -- otherwise an
    # arbitrary array could be surfaced under the exempt column name.
    with pytest.raises(InvalidCustomQuery, match="Aliasing"):
        run_query_and_get_trace(
            STORAGE, f"SELECT trace_id FROM {TABLE} ARRAY JOIN some_arr AS ai_conversation_id"
        )


def test_with_clause_aliasing_to_exempt_column_raises() -> None:
    # ClickHouse `WITH <expr> AS <name>` defines an alias, so a WITH clause can
    # surface an arbitrary expression under the exempt result column name and
    # bypass scrubbing. sql_metadata mis-parses this inverted syntax (the exempt
    # name lands in neither columns_aliases_names nor tables_aliases), so this
    # must be rejected explicitly. WITH is otherwise allowed in the trace shell.
    with pytest.raises(InvalidCustomQuery, match="Aliasing"):
        run_query_and_get_trace(
            STORAGE,
            f"WITH project_id AS ai_conversation_id SELECT ai_conversation_id FROM {TABLE}",
        )
