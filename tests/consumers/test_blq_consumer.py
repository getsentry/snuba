import json
import logging
import os
import shutil
import signal
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.admin import AdminClient

from snuba.clusters.cluster import ClickhouseClientSettings
from snuba.datasets.schemas.tables import TableSchema
from snuba.datasets.storages.factory import get_storage
from snuba.datasets.storages.storage_key import StorageKey
from snuba.utils.manage_topics import create_topics
from snuba.utils.streams.configuration_builder import get_default_kafka_configuration
from snuba.utils.streams.topics import Topic as SnubaTopic
from tests.web.rpc.v1.test_utils import gen_item_message

NUM_STALE = 50
NUM_FRESH = 30
STALE_AGE_MINUTES = 60  # well beyond the 30-min BLQ threshold
MAX_WAIT_SECONDS = 40

logger = logging.getLogger(__name__)


@pytest.fixture
def sentry_options_dir(tmp_path: Path) -> str:
    """Create a temporary sentry-options directory with consumer.blq_enabled=true."""
    schemas_dir = tmp_path / "schemas" / "snuba"
    schemas_dir.mkdir(parents=True)
    schema_src = (
        Path(__file__).parent.parent.parent / "sentry-options" / "schemas" / "snuba" / "schema.json"
    )
    shutil.copy(schema_src, schemas_dir / "schema.json")

    values_dir = tmp_path / "values" / "snuba"
    values_dir.mkdir(parents=True)
    (values_dir / "values.json").write_text(json.dumps({"options": {"consumer.blq_enabled": True}}))

    return str(tmp_path)


def get_row_count() -> int:
    storage = get_storage(StorageKey("eap_items"))
    schema = storage.get_schema()
    assert isinstance(schema, TableSchema)
    return int(
        storage.get_cluster()
        .get_query_connection(ClickhouseClientSettings.INSERT)
        .execute(f"SELECT count() FROM {schema.get_local_table_name()}")
        .results[0][0]
    )


def count_dlq_messages(consumer: ConfluentConsumer) -> int:
    count = 0
    empty_polls = 0
    while empty_polls < 3:
        msg = consumer.poll(timeout=2.0)
        if msg is None or msg.error():
            empty_polls += 1
        else:
            count += 1
            empty_polls = 0
    return count


def start_consumer(
    consumer_group: str,
    env: dict[str, str],
    offset_reset: str = "latest",
) -> subprocess.Popen[Any]:
    return subprocess.Popen(
        [
            "snuba",
            "rust-consumer",
            "--storage",
            "eap_items",
            "--consumer-group",
            consumer_group,
            "--auto-offset-reset",
            offset_reset,
            "--no-strict-offset-reset",
            "--log-level",
            "info",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def get_dlq_consumer() -> ConfluentConsumer:
    kafka_config = get_default_kafka_configuration()
    kafka_config["group.id"] = f"dlq-verify-{uuid.uuid4().hex[:8]}"
    kafka_config["auto.offset.reset"] = "earliest"
    dlq_consumer = ConfluentConsumer(kafka_config)
    dlq_consumer.subscribe([SnubaTopic.DEAD_LETTER_ITEMS.value])
    return dlq_consumer


@pytest.mark.eap
@pytest.mark.redis_db
def test_blq_routes_stale_to_dlq_and_fresh_to_clickhouse(
    sentry_options_dir: str,
) -> None:
    """
    Integration test for the BLQ (backlog queue) router.

    Starts the consumer first with auto-offset-reset=latest, then produces
    stale messages (Kafka timestamp > 30 min old) followed by fresh messages.
    Verifies:
    - Stale messages are routed to the DLQ topic
    - Fresh messages are written to ClickHouse

    The consumer starts with latest offset to ignore old messages from prior runs.
    Messages are produced stale-first so the consumer follows the path:
    Idle -> RoutingStale -> Flushing -> Forwarding (no panic).
    """
    # 1. Create topics
    logger.info("creating topics")
    admin_client = AdminClient(get_default_kafka_configuration())
    create_topics(admin_client, [SnubaTopic.ITEMS])
    create_topics(admin_client, [SnubaTopic.DEAD_LETTER_ITEMS])
    create_topics(admin_client, [SnubaTopic.ITEMS_COMMIT_LOG])

    # 2. Record baseline counts
    logger.info("starting dlq consumer")
    dlq_consumer = get_dlq_consumer()
    baseline_ch = get_row_count()
    baseline_dlq = count_dlq_messages(dlq_consumer)

    # 3. Start the consumer FIRST with latest offset so it ignores old messages
    logger.info("starting eap-items rust consumer")
    consumer_group = f"blq-test-{uuid.uuid4().hex[:8]}"
    env = os.environ.copy()
    env["SENTRY_OPTIONS_DIR"] = sentry_options_dir

    proc = start_consumer(consumer_group, env, offset_reset="latest")

    try:
        # Wait for consumer to start up and get partition assignment
        time.sleep(10)

        # Verify consumer is still running
        assert proc.poll() is None, f"Consumer exited during startup with code {proc.returncode}"

        # 4. Produce messages: stale first, then fresh
        producer = ConfluentProducer(get_default_kafka_configuration())

        now = datetime.now(tz=timezone.utc)
        stale_ts_ms = int((now - timedelta(minutes=STALE_AGE_MINUTES)).timestamp() * 1000)
        fresh_ts_ms = int(now.timestamp() * 1000)
        topic_name = SnubaTopic.ITEMS.value
        logger.info("producing messages to snuba-items")
        for _ in range(NUM_STALE):
            msg_bytes = gen_item_message(start_timestamp=now - timedelta(minutes=STALE_AGE_MINUTES))
            producer.produce(topic=topic_name, value=msg_bytes, timestamp=stale_ts_ms)

        for _ in range(NUM_FRESH):
            msg_bytes = gen_item_message(start_timestamp=now)
            producer.produce(topic=topic_name, value=msg_bytes, timestamp=fresh_ts_ms)

        producer.flush(timeout=10)

        # 5. Poll until both destinations have the expected data
        logger.info("polling ch and dlq topic")
        start_time = time.time()
        ch_count = baseline_ch
        dlq_count = baseline_dlq
        while True:
            if time.time() - start_time > MAX_WAIT_SECONDS:
                raise TimeoutError("test timeout exceeded")
            ch_count = get_row_count()

            dlq_count += count_dlq_messages(dlq_consumer)

            fresh_delta = ch_count - baseline_ch
            stale_delta = dlq_count - baseline_dlq

            if fresh_delta >= NUM_FRESH and stale_delta >= NUM_STALE:
                break

            time.sleep(3)

        fresh_delta = ch_count - baseline_ch
        stale_delta = dlq_count - baseline_dlq

        # 6. Verify results
        assert stale_delta == NUM_STALE, (
            f"Expected {NUM_STALE} new messages on DLQ topic, got {stale_delta}"
        )
        assert fresh_delta == NUM_FRESH, (
            f"Expected {NUM_FRESH} new rows in ClickHouse, got {fresh_delta}"
        )
    finally:
        logger.info("cleaning up")
        dlq_consumer.close()
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
