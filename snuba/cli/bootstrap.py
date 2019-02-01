import click

from snuba import settings


@click.command()
@click.option('--bootstrap-server', default=settings.DEFAULT_BROKERS, multiple=True,
              help='Kafka bootstrap server to use.')
@click.option('--force', is_flag=True)
def bootstrap(bootstrap_server, force):
    """
    Warning: Not intended to be used in production yet.
    """
    if not force:
        raise click.ClickException('Must use --force to run')

    from confluent_kafka.admin import AdminClient, NewTopic

    client = AdminClient({
        'bootstrap.servers': ','.join(bootstrap_server)
    })

    topics = [NewTopic(o.pop('topic'), **o) for o in settings.KAFKA_TOPICS.values()]

    for topic, future in client.create_topics(topics).items():
        try:
            future.result()
            print("Topic %s created" % topic)
        except Exception as e:
            print("Failed to create topic %s: %s" % (topic, e))

    from snuba.clickhouse import ClickhousePool, get_table_definition, get_test_engine

    # Need to better figure out if we are configured to use replicated
    # tables or distributed tables, etc.
    ClickhousePool().execute(
        get_table_definition(
            settings.DEFAULT_LOCAL_TABLE,
            get_test_engine(),
        )
    )
