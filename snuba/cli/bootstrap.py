import click

from snuba import settings


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

        topics = [NewTopic(o.pop('topic'), **o) for o in settings.KAFKA_TOPICS.values()]

        for topic, future in client.create_topics(topics).items():
            try:
                future.result()
                print("Topic %s created" % topic)
            except Exception as e:
                print("Failed to create topic %s: %s" % (topic, e))

    from snuba.clickhouse import ClickhousePool, get_table_definition, get_test_engine

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
    ClickhousePool().execute(
        get_table_definition(
            settings.DEFAULT_LOCAL_TABLE,
            get_test_engine(),
        )
    )
