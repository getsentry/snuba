from datetime import UTC, datetime, timedelta

import pytest

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
    conversation_id = "conv-not-hex-zzz"
    write_raw_unprocessed_events(
        get_writable_storage(StorageKey("eap_items")),
        [
            gen_item_message(
                start_timestamp=datetime.now(tz=UTC) - timedelta(minutes=5),
                conversation_id=conversation_id,
            )
        ],
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
