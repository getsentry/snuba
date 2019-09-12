import click

from snuba import settings
from snuba.datasets.factory import get_dataset, DATASET_NAMES


@click.command()
@click.option('--bootstrap-server', default=settings.DEFAULT_BROKERS, multiple=True,
              help='Kafka bootstrap server to use.')
@click.option('--kafka/--no-kafka', default=True)
@click.option('--force', is_flag=True)
def bootstrap(bootstrap_server, kafka, force):
    """
    Warning: Not intended to be used in production yet.
    """
    if not force:
        raise click.ClickException('Must use --force to run')

    import time

    if kafka:
        from confluent_kafka.admin import AdminClient, NewTopic

        attempts = 0
        while True:
            try:
                client = AdminClient({
                    'bootstrap.servers': ','.join(bootstrap_server),
                    'socket.timeout.ms': 1000,
                })
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
            partitions = dataset.get_default_partitions()
            replication = dataset.get_default_replication_factor()
            topics.extend([
                (dataset.get_default_topic(), partitions, replication),
                (dataset.get_default_replacement_topic(), partitions, replication),
                (dataset.get_default_commit_log_topic(), partitions, replication),
            ])

        topics = [NewTopic(t[0], num_partitions=t[1], replication_factor=t[2])
            for t in topics
            if t[0] is not None]

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
            ClickhousePool().execute('SELECT 1')
            break
        except Exception as e:
            print(e)
            attempts += 1
            if attempts == 60:
                raise
            time.sleep(1)

    # Need to better figure out if we are configured to use replicated
    # tables or distributed tables, etc.

    # Migrate from the old table to the new one if needed
    from snuba import migrate
    migrate.rename_dev_table(ClickhousePool())

    # Create the tables for every dataset.
    for name in DATASET_NAMES:
        dataset = get_dataset(name)

        for statement in dataset.get_dataset_schemas().get_create_statements():
            ClickhousePool().execute(statement)
