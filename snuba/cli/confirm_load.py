import json
import logging
from typing import Optional, Sequence

import click
from confluent_kafka import Producer

from snuba import settings
from snuba.datasets.factory import DATASET_NAMES, get_dataset
from snuba.datasets.cdc import CdcStorage
from snuba.environment import setup_logging, setup_sentry
from snuba.snapshots.postgres_snapshot import PostgresSnapshot
from snuba.stateful_consumer.control_protocol import SnapshotLoaded, TransactionData


@click.command()
@click.option("--control-topic", help="Topic to produce messages onto.")
@click.option(
    "--bootstrap-server", multiple=True, help="Kafka bootstrap server to use.",
)
@click.option(
    "--dataset",
    "dataset_name",
    type=click.Choice(DATASET_NAMES),
    help="The dataset to bulk load",
)
@click.option(
    "--source",
    help="Source of the dump. Depending on the dataset it may have different meaning.",
)
@click.option("--log-level", help="Logging level to use.")
def confirm_load(
    *,
    control_topic: Optional[str],
    bootstrap_server: Sequence[str],
    dataset_name: str,
    source: Optional[str],
    log_level: Optional[str] = None,
) -> None:
    """
    Confirms the snapshot has been loaded by sending the
    snapshot-loaded message on the control topic.
    """

    setup_logging(log_level)
    setup_sentry()

    logger = logging.getLogger("snuba.loaded-snapshot")
    logger.info(
        "Sending load completion message for dataset %s, from source %s",
        dataset_name,
        source,
    )

    dataset = get_dataset(dataset_name)

    storage = dataset.get_writable_storage()

    assert isinstance(
        storage, CdcStorage
    ), "Only CDC storages have a control topic thus are supported."

    control_topic = control_topic or storage.get_default_control_topic()

    snapshot_source = PostgresSnapshot.load(
        product=settings.SNAPSHOT_LOAD_PRODUCT, path=source,
    )

    descriptor = snapshot_source.get_descriptor()

    if not bootstrap_server:
        bootstrap_server = settings.DEFAULT_DATASET_BROKERS.get(
            dataset, settings.DEFAULT_BROKERS,
        )

    producer = Producer(
        {
            "bootstrap.servers": ",".join(bootstrap_server),
            "partitioner": "consistent",
            "message.max.bytes": 50000000,  # 50MB, default is 1MB
        }
    )

    msg = SnapshotLoaded(
        id=descriptor.id,
        transaction_info=TransactionData(
            xmin=descriptor.xmin, xmax=descriptor.xmax, xip_list=descriptor.xip_list,
        ),
    )
    json_string = json.dumps(msg.to_dict())

    def delivery_callback(error, message) -> None:
        if error is not None:
            raise error
        else:
            logger.info("Message sent %r", message.value())

    producer.produce(
        control_topic, value=json_string, on_delivery=delivery_callback,
    )

    producer.flush()
