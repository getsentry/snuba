import click

from snuba import settings
from snuba.datasets.factory import get_dataset, DATASET_NAMES


@click.command()
@click.option(
    "--bootstrap-server",
    default=settings.DEFAULT_BROKERS,
    multiple=True,
    help="Kafka bootstrap server to use.",
)
@click.option("--kafka/--no-kafka", default=True)
@click.option("--force", is_flag=True)
def bootstrap(bootstrap_server, kafka, force):
    """
    Warning: Not intended to be used in production yet.
    """
    if not force:
        raise click.ClickException("Must use --force to run")

    import time

    if kafka:
        from confluent_kafka.admin import AdminClient, NewTopic

        attempts = 0
        while True:
            try:
                client = AdminClient(
                    {
                        "bootstrap.servers": ",".join(bootstrap_server),
                        "socket.timeout.ms": 1000,
                    }
                )
                client.list_topics(timeout=1)
                break
            except Exception as e:
                print(e)
                attempts += 1
                if attempts == 60:
                    raise
                time.sleep(1)

        topics = []
        for name in DATASET_NAMES:
            dataset = get_dataset(name)
            table_writer = dataset.get_table_writer()
            if table_writer:
                stream_loader = table_writer.get_stream_loader()
                for topic_spec in stream_loader.get_all_topic_specs():
                    topics.append(
                        NewTopic(
                            topic_spec.topic_name,
                            num_partitions=topic_spec.partitions_number,
                            replication_factor=topic_spec.replication_factor,
                        )
                    )

        for topic, future in client.create_topics(topics).items():
            try:
                future.result()
                print("Topic %s created" % topic)
            except Exception as e:
                print("Failed to create topic %s: %s" % (topic, e))

    from snuba.clickhouse.native import ClickhousePool

    attempts = 0
    while True:
        try:
            ClickhousePool().execute("SELECT 1")
            break
        except Exception as e:
            print(e)
            attempts += 1
            if attempts == 60:
                raise
            time.sleep(1)

    # Need to better figure out if we are configured to use replicated
    # tables or distributed tables, etc.

    # Create the tables for every dataset.
    for name in DATASET_NAMES:
        dataset = get_dataset(name)

        for statement in dataset.get_dataset_schemas().get_create_statements():
            ClickhousePool().execute(statement)
