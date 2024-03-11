import click
from clickhouse_driver import Client


@click.command("")
def show_replication_queue():
    client = Client(host="localhost")
    print(
        client.execute(
            "SELECT * FROM system.replication_queue LIMIT 1 FORMAT Vertical;"
        )
    )


if __name__ == "__main__":
    show_replication_queue()
